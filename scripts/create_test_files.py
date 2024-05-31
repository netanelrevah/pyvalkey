import re
from pathlib import Path

import typer

app = typer.Typer()


def generate_file(source_file_path: Path) -> None:
    with open(source_file_path) as source_file:
        print("import pytest")
        print("import redis")

        for line in source_file:
            stripped_line = line.strip()

            if not stripped_line.startswith("test"):
                continue

            print()
            print()

            translate_table = str.maketrans(
                {
                    "/": "_or_",
                    **{c: "_" for c in '{}",#: -()'},
                }
            )
            test_name = re.sub("_+", "_", stripped_line.lower().translate(translate_table).strip("_"))

            print(
                f"@pytest.mark.xfail(reason='not implemented')\n"
                f"def {test_name}(s: redis.Redis):\n"
                f"    assert False"
            )


@app.command()
def generate(valkey_directory: Path = typer.Argument(..., envvar="VALKEY_DIRECTORY")) -> None:
    unit_tests_path = valkey_directory / "tests" / "unit"
    type_tests_path = unit_tests_path / "type"

    generate_file(Path(type_tests_path / "set.tcl"))


if __name__ == "__main__":
    app()
