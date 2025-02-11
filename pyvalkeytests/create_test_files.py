from __future__ import annotations

import re
import sys
import textwrap
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import cycle, zip_longest
from pathlib import Path
from typing import IO

import typer

from pytcl.commands import TCLBracesWord, TCLCommandBase, TCLCommandForEach, TCLCommandIf, TCLList, TCLScript

app = typer.Typer(pretty_exceptions_enable=False)


@dataclass
class TestFileGenerator:
    output: IO = field(default=sys.stdout)

    @classmethod
    def collect_foreach_name_to_values(cls, foreach_command: TCLCommandForEach) -> dict[str, list]:
        foreach_name_to_values = defaultdict(list)

        for variable_names_list, variable_values_list in foreach_command.variables_names_and_values_lists:
            for value, name in zip(variable_values_list.words, cycle(variable_names_list)):
                foreach_name_to_values[name].append(value)

        return dict(foreach_name_to_values)

    @classmethod
    def generate_parametrize(cls, foreach_command: TCLCommandForEach) -> str:
        foreach_name_to_values = cls.collect_foreach_name_to_values(foreach_command)

        generated = "@pytest.mark.parametrize(["
        generated += ",".join([f'"{values_name}"' for values_name, values_list in foreach_name_to_values.items()])
        generated += "], ["

        values_tuples = []

        for args in zip_longest(
            *[values_list for values_name, values_list in foreach_name_to_values.items()], fillvalue=None
        ):
            values = []
            for value in args:
                if not value:
                    values.append('"""None"""')
                elif "\n" in value:
                    values.append(f'"""{value}"""')
                else:
                    values.append(f'"{value}"')

            values_tuples.append("(" + ",".join(values) + ")")

        generated += ",".join(values_tuples)
        generated += "])\n"

        return generated

    def generate_test(
        self,
        test_command: TCLCommandBase,
        if_expressions: list[str] | None = None,
        parametrizes: list[str] | None = None,
    ) -> None:
        self.output.write("\n\n")

        name = str(test_command.args[0])

        translate_table = str.maketrans(
            {
                "/": "_or_",
                **{c: "_" for c in "{}\",#: -()<>=$'"},
            }
        )
        test_name = re.sub("_+", "_", name.lower().translate(translate_table).strip("_"))

        self.output.write("@pytest.mark.xfail(reason='not implemented')\n")
        if if_expressions:
            for if_expression in if_expressions:
                self.output.write(f"@pytest.mark.skipif('', reason='{if_expression}')\n")
        if parametrizes:
            self.output.writelines(parametrizes)

        self.output.write(f"def {test_name}(s: valkey.Valkey):\n")
        self.output.write('    """\n')
        self.output.write(f"{textwrap.indent(textwrap.dedent(str(test_command)), '    ')}\n")
        self.output.write('    """\n')
        self.output.write("    assert False\n")

    def generate_comment(self, command: TCLCommandBase) -> None:
        self.output.write('\n\n"""\n')
        self.output.write(f"{textwrap.dedent(str(command))}\n")
        self.output.write('"""\n')

    def generate_tests(
        self, script: TCLScript, if_expressions: list[str] | None = None, foreach_commands: list[str] | None = None
    ) -> None:
        for command in script.commands:
            if isinstance(command, TCLCommandBase):
                match command.name:
                    case "test":
                        self.generate_test(command, if_expressions, foreach_commands)
                    case "if":
                        if_command = TCLCommandIf.interpertize(command.args, {})
                        expression, body = if_command.if_part

                        if not any([c for c in body.commands if c.name == "test"]):
                            self.generate_comment(command)
                            continue

                        self.generate_tests(
                            body,
                            (if_expressions or []) + [str(expression.result)],
                            foreach_commands,
                        )
                    case "foreach":
                        foreach_command = TCLCommandForEach.interpertize(command, {})

                        if not any([c for c in foreach_command.body.commands if c.name == "test"]):
                            self.generate_comment(command)
                            continue

                        self.generate_tests(
                            foreach_command.body,
                            if_expressions,
                            (foreach_commands or []) + [self.generate_parametrize(foreach_command)],
                        )
                    case "tags":
                        pass
                        # raise ValueError("Tags")
                    case "proc" | "array" | "for" | "set" | "r":
                        self.generate_comment(command)
                    case _:
                        raise ValueError(command.name)

    def generate_file(self, source_file_path: Path) -> None:
        with open(source_file_path) as source_file:
            self.output.write("import pytest\n")
            self.output.write("import valkey\n")

            script = TCLScript.read_text_io(source_file)

            start_server_command = script.commands[0]
            assert isinstance(start_server_command, TCLCommandBase)
            assert start_server_command.name == "start_server"

            tags: list[str] = []

            options = TCLList.interpertize(start_server_command.args[0], namespace={}).words
            for option, value in zip(options[::2], options[1::2]):
                match option:
                    case "tags":
                        tags.extend([word for word in TCLList.words_iterator(iter(value), {})])

            assert isinstance(start_server_command.args[1], TCLBracesWord)
            start_server_code = TCLScript.read(iter(start_server_command.args[1].value))

            if tags:
                marks = ", ".join(["pytest.mark." + tag for tag in set(tags)])
                self.output.write("\n")
                self.output.write(f"pytestmark = [{marks}]\n")

            assert start_server_code
            self.generate_tests(start_server_code)


@app.command()
def generate(valkey_directory: Path = typer.Argument(..., envvar="VALKEY_DIRECTORY")) -> None:
    unit_tests_path = valkey_directory / "tests" / "unit"
    type_tests_path = unit_tests_path / "type"

    TestFileGenerator().generate_file(Path(type_tests_path / "set.tcl"))
    TestFileGenerator().generate_file(Path(type_tests_path / "string.tcl"))
    TestFileGenerator().generate_file(Path(type_tests_path / "incr.tcl"))


if __name__ == "__main__":
    app()
