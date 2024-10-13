from __future__ import annotations

from pathlib import Path

from pytcl.words import TCLScript


def read_tcl_file(source_file_path: Path) -> TCLScript:
    with open(source_file_path) as source_file:
        return TCLScript.read_text_io(source_file)
