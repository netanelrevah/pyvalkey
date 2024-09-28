from __future__ import annotations

import re
import textwrap
from itertools import zip_longest
from pathlib import Path

import typer

from scripts.tcl_parser import TCLCommand, TCLCommandForEach, TCLCommandIf, TCLList, TCLScript

app = typer.Typer(pretty_exceptions_enable=False)


def generate_parametrize(foreach_command: TCLCommandForEach) -> str:
    generated = "@pytest.mark.parametrize(["
    generated += ",".join([f'"{n}"' for n, l in foreach_command.variables_names_and_lists])
    generated += "], ["

    values = []

    for args in zip_longest(*[l.words for n, l in foreach_command.variables_names_and_lists]):
        values.append("(" + ",".join(f'"{v.substitute()}"' for v in args) + ")")

    generated += ",".join(values)
    generated += "])"

    return generated


def generate_test(
    test_command: TCLCommand,
    if_expressions: list[str] | None = None,
    foreach_commands: list[str] | None = None,
) -> None:
    print()
    print()

    name = "".join(test_command.args[0].substitute_iterator())

    translate_table = str.maketrans(
        {
            "/": "_or_",
            **{c: "_" for c in '{}",#: -()'},
        }
    )
    test_name = re.sub("_+", "_", name.lower().translate(translate_table).strip("_"))

    print("@pytest.mark.xfail(reason='not implemented')")
    if if_expressions:
        for if_expression in if_expressions:
            print(f"@pytest.mark.skipif('', reason='{if_expression}')")
    if foreach_commands:
        for foreach_command in foreach_commands:
            print(foreach_command)

    print(f"def {test_name}(s: valkey.Valkey):")
    print('    """')
    print(f"{textwrap.indent(textwrap.dedent(str(test_command.args[1])),"    ")}")
    print('    """')
    print("    assert False")


def generate_tests(
    script: TCLScript, if_expressions: list[str] | None = None, foreach_commands: list[str] | None = None
) -> None:
    for command in script.commands:
        if isinstance(command, TCLCommand):
            match command.name:
                case "test":
                    generate_test(command, if_expressions, foreach_commands)
                case "if":
                    if_command = TCLCommandIf.interpertize(command)
                    expression, body = if_command.if_part
                    generate_tests(
                        body,
                        (if_expressions or []) + ["".join(expression.word.substitute_iterator())],
                        foreach_commands,
                    )
                case "foreach":
                    foreach_command = TCLCommandForEach.interpertize(command)
                    foreach_command.body
                    generate_tests(
                        foreach_command.body,
                        if_expressions,
                        (foreach_commands or []) + [generate_parametrize(foreach_command)],
                    )
                case "proc" | "array" | "for" | "set" | "r" | "tags":
                    continue
                case _:
                    raise ValueError(command.name)


def generate_file(source_file_path: Path) -> None:
    with open(source_file_path) as source_file:
        print("import pytest")
        print("import valkey")

        script = TCLScript.read_text_io(source_file)

        start_server_command = script.commands[0]
        assert isinstance(start_server_command, TCLCommand)
        assert start_server_command.name == "start_server"

        tags: list[str] = []

        options = TCLList.interpertize(start_server_command.args[0]).words
        for option, value in zip(options[::2], options[1::2]):
            match option.substitute_iterator():
                case "tags":
                    tags.extend(
                        [
                            word.substitute_iterator()
                            for word in TCLList.words_iterator(iter(value.substitute_iterator()))
                        ]
                    )

        start_server_code = TCLScript.read(iter(start_server_command.args[1].substitute_iterator()))

        if tags:
            marks = ", ".join(["pytest.mark." + tag for tag in set(tags)])
            print()
            print(f"pytestmark = [{marks}]")

        assert start_server_code
        generate_tests(start_server_code)


@app.command()
def generate(valkey_directory: Path = typer.Argument(..., envvar="VALKEY_DIRECTORY")) -> None:
    unit_tests_path = valkey_directory / "tests" / "unit"
    type_tests_path = unit_tests_path / "type"

    generate_file(Path(type_tests_path / "set.tcl"))
    # generate_file(Path(type_tests_path / "string.tcl"))
    # generate_file(Path(type_tests_path / "incr.tcl"))


if __name__ == "__main__":
    app()
