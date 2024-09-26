from __future__ import annotations

import re
import textwrap
from pathlib import Path

import typer

from scripts.tcl_parser import TCLCommand, TCLCommandIf, TCLList, TCLScript

app = typer.Typer(pretty_exceptions_enable=False)


def generate_test(test_command: TCLCommand, if_expressions: list[str] | None = None) -> None:
    print()
    print()

    name = "".join(test_command.args[0].substitute())

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

    print(f"def {test_name}(s: redis.Redis):")
    print('    """')
    print(f"{textwrap.indent(textwrap.dedent(str(test_command.args[1])),"    ")}")
    print('    """')
    print("    assert False")


def generate_tests(script: TCLScript, if_expressions: list[str] | None = None) -> None:
    for command in script.commands:
        if isinstance(command, TCLCommand):
            match command.name:
                case "test":
                    generate_test(command, if_expressions)
                case "if":
                    if_command = TCLCommandIf.interpertize(command)
                    expression, body = if_command.if_part
                    generate_tests(body, (if_expressions or []) + ["".join(expression.word.substitute())])
                case "foreach":
                    pass


def generate_file(source_file_path: Path) -> None:
    with open(source_file_path) as source_file:
        print("import pytest")
        print("import redis")

        script = TCLScript.read_text_io(source_file)

        start_server_command = script.commands[0]
        assert isinstance(start_server_command, TCLCommand)
        assert start_server_command.name == "start_server"

        tags: list[str] = []

        options = TCLList.interpertize(start_server_command.args[0]).words
        for option, value in zip(options[::2], options[1::2]):
            match option.substitute():
                case "tags":
                    tags.extend([word.substitute() for word in TCLList.words_iterator(iter(value.substitute()))])

        start_server_code = TCLScript.read(iter(start_server_command.args[1].substitute()))

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

    # generate_file(Path(type_tests_path / "set.tcl"))

    # generate_file(Path(type_tests_path / "string.tcl"))

    generate_file(Path(type_tests_path / "incr.tcl"))


if __name__ == "__main__":
    app()
