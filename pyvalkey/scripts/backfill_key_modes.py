from __future__ import annotations

import ast
import json
from typing import TYPE_CHECKING, Any, cast

import typer

if TYPE_CHECKING:
    from pathlib import Path

PARAMETER_FACTORIES = {"positional_parameter", "keyword_parameter"}
PARENT_COMMAND_POSITIONAL_INDEX = 2


def _bytes_literal(node: ast.AST) -> bytes | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, bytes):
        return node.value
    return None


def _valkey_json_path(valkey_dir: Path, name: bytes, parent: bytes | None) -> Path:
    stem = (parent.decode() + "-" + name.decode()) if parent else name.decode()
    return valkey_dir / f"{stem}.json"


def _mode_from_spec_flags(flags: list[Any]) -> str | None:
    upper = {f.upper() for f in flags if isinstance(f, str)}
    if "RW" in upper:
        return "RW"
    if "OW" in upper:
        return "W"
    if "RO" in upper:
        return "R"
    return None


def _collect_key_modes(body: dict[str, Any]) -> dict[str, str]:
    key_specs = body.get("key_specs") or []
    arguments = body.get("arguments") or []
    if not isinstance(key_specs, list) or not isinstance(arguments, list):
        return {}
    spec_modes: list[str | None] = []
    for spec in key_specs:
        if not isinstance(spec, dict):
            spec_modes.append(None)
            continue
        flags = spec.get("flags") or []
        spec_modes.append(_mode_from_spec_flags(flags if isinstance(flags, list) else []))

    result: dict[str, str] = {}

    def _walk(args: list[Any]) -> None:
        for arg in args:
            if not isinstance(arg, dict):
                continue
            if arg.get("type") == "key":
                idx = arg.get("key_spec_index")
                name = arg.get("name")
                if isinstance(idx, int) and isinstance(name, str) and 0 <= idx < len(spec_modes):
                    mode = spec_modes[idx]
                    if mode:
                        result[name.replace("-", "_")] = mode
            nested = arg.get("arguments")
            if isinstance(nested, list):
                _walk(nested)

    _walk(arguments)
    return result


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


def _find_command_decorator(class_node: ast.ClassDef) -> tuple[bytes, bytes | None] | None:
    for decorator in class_node.decorator_list:
        if not isinstance(decorator, ast.Call):
            continue
        func = decorator.func
        func_name = func.id if isinstance(func, ast.Name) else (func.attr if isinstance(func, ast.Attribute) else None)
        if func_name != "command" or not decorator.args:
            continue
        name = _bytes_literal(decorator.args[0])
        if name is None:
            continue
        parent: bytes | None = None
        if len(decorator.args) > PARENT_COMMAND_POSITIONAL_INDEX:
            parent = _bytes_literal(decorator.args[PARENT_COMMAND_POSITIONAL_INDEX])
        for kw in decorator.keywords:
            if kw.arg == "parent_command":
                parent = _bytes_literal(kw.value)
        return name, parent
    return None


def _call_has_kwarg(call: ast.Call, name: str) -> bool:
    return any(kw.arg == name for kw in call.keywords)


def _process_file(source_path: Path, valkey_dir: Path, dry_run: bool) -> int:
    source = source_path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return 0

    line_offsets = _line_offsets(source)
    edits: list[tuple[int, str]] = []  # (insert_offset, text)
    updated = 0

    for class_node in ast.walk(tree):
        if not isinstance(class_node, ast.ClassDef):
            continue
        decorator_info = _find_command_decorator(class_node)
        if decorator_info is None:
            continue
        json_path = _valkey_json_path(valkey_dir, *decorator_info)
        if not json_path.exists():
            continue
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not isinstance(data, dict) or len(data) != 1:
            continue
        body = next(iter(data.values()))
        if not isinstance(body, dict):
            continue

        key_modes = _collect_key_modes(cast("dict[str, Any]", body))
        if not key_modes:
            continue

        for stmt in class_node.body:
            if not isinstance(stmt, ast.AnnAssign) or not isinstance(stmt.target, ast.Name):
                continue
            field_name = stmt.target.id
            if field_name not in key_modes:
                continue
            call = stmt.value
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            func_name = (
                func.id if isinstance(func, ast.Name) else (func.attr if isinstance(func, ast.Attribute) else None)
            )
            if func_name not in PARAMETER_FACTORIES:
                continue
            if _call_has_kwarg(call, "key_mode"):
                continue

            mode = key_modes[field_name]
            closing_offset = _node_end_offset(line_offsets, call) - 1
            has_existing_args = bool(call.args or call.keywords)
            insertion = f', key_mode=b"{mode}"' if has_existing_args else f'key_mode=b"{mode}"'
            edits.append((closing_offset, insertion))
            updated += 1

    if edits and not dry_run:
        edits.sort(key=lambda item: item[0], reverse=True)
        new_source = source
        for offset, text in edits:
            new_source = new_source[:offset] + text + new_source[offset:]
        source_path.write_text(new_source, encoding="utf-8")

    return updated


app = typer.Typer(add_completion=False)


@app.command()
def main(
    commands_dir: Path = typer.Argument(..., help="pyvalkey commands directory (contains *_commands.py)"),
    valkey_dir: Path = typer.Argument(..., help="valkey src/commands directory"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Scan only; do not modify files"),
) -> None:
    """Add key_mode=b\"...\" to positional/keyword parameter calls based on valkey key_specs."""
    commands_dir = commands_dir.resolve()
    valkey_dir = valkey_dir.resolve()
    if not commands_dir.is_dir() or not valkey_dir.is_dir():
        typer.echo("both directories must exist", err=True)
        raise typer.Exit(code=2)

    total = 0
    for source_path in sorted(commands_dir.glob("*_commands.py")):
        updated = _process_file(source_path, valkey_dir, dry_run)
        if updated:
            typer.echo(f"{source_path.name}: {updated} field(s) updated")
        total += updated

    typer.echo("")
    typer.echo(f"{'would update' if dry_run else 'updated'} {total} field(s)")


if __name__ == "__main__":
    app()
