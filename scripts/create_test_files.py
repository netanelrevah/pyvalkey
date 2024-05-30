import re
from pathlib import Path


def generate_file(source_file_path: Path) -> None:
    with open(source_file_path) as source_file:
        for line in source_file:
            stripped_line = line.strip()

            if not stripped_line.startswith("test"):
                continue

            translate_table = str.maketrans(
                {
                    "/": "_or_",
                    **{c: "_" for c in '{}",#: -()'},
                }
            )
            test_name = re.sub("_+", "_", stripped_line.lower().translate(translate_table).strip("_"))

            print(f"def {test_name}(s: redis.Redis):\n    assert False\n\n")


def main() -> None:
    generate_file(Path("C:\\Users\\netan\\AppData\\Roaming\\JetBrains\\PyCharmCE2024.1\\scratches\\string.tcl"))


if __name__ == "__main__":
    main()
