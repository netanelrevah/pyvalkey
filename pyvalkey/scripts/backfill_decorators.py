from __future__ import annotations

import ast
import json
from typing import TYPE_CHECKING, Any, cast

import typer

if TYPE_CHECKING:
    from pathlib import Path

ACL_CATEGORIES_FROM_FLAGS = {"fast", "write"}
PARENT_COMMAND_POSITIONAL_INDEX = 2


def _bytes_literal(node: ast.AST) -> bytes | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, bytes):
        return node.value
    return None


def _valkey_json_path(valkey_dir: Path, name: bytes, parent: bytes | None) -> Path:
    stem = (parent.decode() + "-" + name.decode()) if parent else name.decode()
    return valkey_dir / f"{stem}.json"


def _extract_body(valkey_json_path: Path) -> dict[str, Any] | None:
    data = json.loads(valkey_json_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or len(data) != 1:
        return None
    body = next(iter(data.values()))
    if not isinstance(body, dict):
        return None
    return cast("dict[str, Any]", body)


def _line_offsets(source: str) -> list[int]:
    offsets = [0]
    for idx, ch in enumerate(source):
        if ch == "\n":
            offsets.append(idx + 1)
    return offsets


def _pos_to_offset(line_offsets: list[int], line: int, col: int) -> int:
    return line_offsets[line - 1] + col


def _node_end_offset(line_offsets: list[int], node: ast.stmt | ast.expr) -> int:
    if node.end_lineno is None or node.end_col_offset is None:
        raise ValueError(f"AST node {type(node).__name__} is missing end position information")
    return _pos_to_offset(line_offsets, node.end_lineno, node.end_col_offset)


def _segment(source: str, line_offsets: list[int], node: ast.expr) -> str:
    start = _pos_to_offset(line_offsets, node.lineno, node.col_offset)
    end = _node_end_offset(line_offsets, node)
    return source[start:end]


def _format_bytes_set(values: list[str]) -> str:
    return "{" + ", ".join(f'b"{v.lower()}"' for v in sorted(values)) + "}"


def _find_command_call(class_node: ast.ClassDef) -> ast.Call | None:
    for decorator in class_node.decorator_list:
        if not isinstance(decorator, ast.Call):
            continue
        func = decorator.func
        func_name = func.id if isinstance(func, ast.Name) else (func.attr if isinstance(func, ast.Attribute) else None)
        if func_name == "command":
            return decorator
    return None


def _extract_call_parts(
    call: ast.Call,
) -> tuple[ast.expr, ast.expr | None, ast.keyword | None, ast.keyword | None, bool] | None:
    """
    Return (name_arg, parent_arg_or_kw, flags_kw, metadata_kw, uses_parent_kwarg).

    Parent is returned as an expr node (positional) or keyword.
    """
    if not call.args:
        return None
    name_arg = call.args[0]

    parent_node: ast.expr | None = None
    if len(call.args) > PARENT_COMMAND_POSITIONAL_INDEX:
        parent_node = call.args[PARENT_COMMAND_POSITIONAL_INDEX]

    flags_kw: ast.keyword | None = None
    metadata_kw: ast.keyword | None = None
    parent_kw_as_expr: ast.expr | None = None
    for kw in call.keywords:
        if kw.arg == "flags":
            flags_kw = kw
        elif kw.arg == "metadata":
            metadata_kw = kw
        elif kw.arg == "parent_command":
            parent_kw_as_expr = kw.value
    parent = parent_node or parent_kw_as_expr
    uses_parent_kwarg = parent_node is None and parent_kw_as_expr is not None

    return name_arg, parent, flags_kw, metadata_kw, uses_parent_kwarg


def _rebuild_decorator(
    source: str,
    line_offsets: list[int],
    call: ast.Call,
    acl_categories: list[str],
    command_flags: list[str],
) -> str:
    parts = _extract_call_parts(call)
    if parts is None:
        return _segment(source, line_offsets, call)
    name_arg, parent, _, metadata_kw, uses_parent_kwarg = parts

    name_text = _segment(source, line_offsets, name_arg)
    acl_text = _format_bytes_set(acl_categories)

    pieces: list[str] = [name_text, acl_text]

    if parent is not None:
        parent_text = _segment(source, line_offsets, parent)
        pieces.append(f"parent_command={parent_text}" if uses_parent_kwarg else parent_text)

    if command_flags:
        pieces.append(f"flags={_format_bytes_set(command_flags)}")

    if metadata_kw is not None:
        metadata_text = _segment(source, line_offsets, metadata_kw.value)
        pieces.append(f"metadata={metadata_text}")

    return "command(" + ", ".join(pieces) + ")"


def _process_file(source_path: Path, valkey_dir: Path, dry_run: bool) -> int:
    source = source_path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return 0

    line_offsets = _line_offsets(source)
    replacements: list[tuple[int, int, str]] = []
    updated = 0

    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        call = _find_command_call(node)
        if call is None:
            continue
        parts = _extract_call_parts(call)
        if parts is None:
            continue
        name_arg = parts[0]
        name_value = _bytes_literal(name_arg)
        if name_value is None:
            continue
        parent = parts[1]
        parent_value = _bytes_literal(parent) if parent is not None else None

        json_path = _valkey_json_path(valkey_dir, name_value, parent_value)
        if not json_path.exists():
            continue
        body = _extract_body(json_path)
        if body is None:
            continue

        acl_categories = body.get("acl_categories") or []
        command_flags = body.get("command_flags") or []
        if not isinstance(acl_categories, list) or not isinstance(command_flags, list):
            continue
        acl_categories = [
            c for c in acl_categories if isinstance(c, str) and c.lower() not in ACL_CATEGORIES_FROM_FLAGS
        ]
        command_flags = [f for f in command_flags if isinstance(f, str)]
        if not acl_categories:
            # every command must have at least one acl_category -- skip if valkey's entry is empty.
            continue

        new_text = _rebuild_decorator(source, line_offsets, call, acl_categories, command_flags)
        start = _pos_to_offset(line_offsets, call.lineno, call.col_offset)
        end = _node_end_offset(line_offsets, call)
        if source[start:end] == new_text:
            continue
        replacements.append((start, end, new_text))
        updated += 1

    if replacements and not dry_run:
        replacements.sort(key=lambda item: item[0], reverse=True)
        new_source = source
        for start, end, text in replacements:
            new_source = new_source[:start] + text + new_source[end:]
        source_path.write_text(new_source, encoding="utf-8")

    return updated


app = typer.Typer(add_completion=False)


@app.command()
def main(
    commands_dir: Path = typer.Argument(..., help="pyvalkey commands directory (contains *_commands.py)"),
    valkey_dir: Path = typer.Argument(..., help="valkey src/commands directory"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Scan only; do not modify files"),
) -> None:
    """Overwrite @command decorator acl_categories and flags from valkey JSON files."""
    commands_dir = commands_dir.resolve()
    valkey_dir = valkey_dir.resolve()
    if not commands_dir.is_dir() or not valkey_dir.is_dir():
        typer.echo("both directories must exist", err=True)
        raise typer.Exit(code=2)

    total = 0
    for source_path in sorted(commands_dir.glob("*_commands.py")):
        updated = _process_file(source_path, valkey_dir, dry_run)
        if updated:
            typer.echo(f"{source_path.name}: {updated} decorator(s) updated")
        total += updated

    typer.echo("")
    typer.echo(f"{'would update' if dry_run else 'updated'} {total} decorator(s)")


if __name__ == "__main__":
    app()
