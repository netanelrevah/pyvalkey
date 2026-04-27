from __future__ import annotations

import json
from typing import TYPE_CHECKING, TypeAlias, cast

import typer

if TYPE_CHECKING:
    from pathlib import Path

UNORDERED_FIELDS = {"acl_categories", "command_flags"}

JsonValue: TypeAlias = "None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]"


def _normalize(value: JsonValue, field_name: str | None = None) -> JsonValue:
    if isinstance(value, dict):
        return {k: _normalize(v, k) for k, v in value.items()}
    if isinstance(value, list):
        normalized = [_normalize(item) for item in value]
        if field_name in UNORDERED_FIELDS and all(isinstance(item, str) for item in normalized):
            return sorted(cast("list[str]", normalized))
        return normalized
    return value


def _load(path: Path) -> JsonValue:
    return _normalize(json.loads(path.read_text(encoding="utf-8")))


def _collect(directory: Path) -> dict[str, Path]:
    return {p.stem: p for p in directory.glob("*.json")}


def _diff(left: JsonValue, right: JsonValue, path: str = "") -> list[str]:
    if type(left) is not type(right):
        return [f"{path or '<root>'}: type {type(left).__name__} vs {type(right).__name__}"]
    if isinstance(left, dict):
        diffs: list[str] = []
        for key in sorted(set(left) | set(right)):
            sub_path = f"{path}.{key}" if path else key
            if key not in left:
                diffs.append(f"{sub_path}: only in right -> {json.dumps(right[key])}")
            elif key not in right:
                diffs.append(f"{sub_path}: only in left -> {json.dumps(left[key])}")
            else:
                diffs.extend(_diff(left[key], right[key], sub_path))
        return diffs
    if isinstance(left, list):
        if left == right:
            return []
        return [f"{path}: list differs | left={json.dumps(left)} | right={json.dumps(right)}"]
    if left != right:
        return [f"{path}: {json.dumps(left)} vs {json.dumps(right)}"]
    return []


def compare(pyvalkey_dir: Path, valkey_dir: Path) -> tuple[list[str], list[str], list[str], dict[str, list[str]]]:
    left = _collect(pyvalkey_dir)
    right = _collect(valkey_dir)
    left_names = set(left)
    right_names = set(right)

    added = sorted(left_names - right_names)
    missing = sorted(right_names - left_names)
    shared = sorted(left_names & right_names)

    differing: dict[str, list[str]] = {}
    for name in shared:
        diffs = _diff(_load(left[name]), _load(right[name]))
        if diffs:
            differing[name] = diffs

    return sorted(differing.keys()), missing, added, differing


def _echo_list(title: str, names: list[str]) -> None:
    typer.echo(f"{title} ({len(names)}):")
    for name in names:
        typer.echo(f"  {name}")


app = typer.Typer(add_completion=False)


@app.command()
def main(
    pyvalkey_dir: Path = typer.Argument(..., help="Directory of pyvalkey-exported JSONs"),
    valkey_dir: Path = typer.Argument(..., help="Directory of valkey src/commands JSONs"),
    diffs: bool = typer.Option(False, "--diffs", help="List commands present in both but with content differences"),
    missing: bool = typer.Option(False, "--missing", help="List commands present in valkey but missing from pyvalkey"),
    added: bool = typer.Option(False, "--added", help="List commands present in pyvalkey but not in valkey"),
    command: str | None = typer.Option(
        None, "--command", help="Show detailed diff for a single command (filename stem)"
    ),
) -> None:
    """Compare pyvalkey exported command JSONs against valkey src/commands."""
    for label, directory in (("pyvalkey_dir", pyvalkey_dir), ("valkey_dir", valkey_dir)):
        resolved = directory.resolve()
        if not resolved.is_dir():
            typer.echo(f"{label}: {resolved} is not a directory", err=True)
            raise typer.Exit(code=2)
        if not any(resolved.glob("*.json")):
            typer.echo(f"{label}: {resolved} contains no *.json files", err=True)
            raise typer.Exit(code=2)

    diff_names, missing_list, added_list, _differing = compare(pyvalkey_dir, valkey_dir)

    if command is not None:
        key = command.lower().replace("|", "-")
        left_path = pyvalkey_dir / f"{key}.json"
        right_path = valkey_dir / f"{key}.json"
        if not left_path.exists() and not right_path.exists():
            typer.echo(f"{key}: not found in either directory", err=True)
            raise typer.Exit(code=1)
        if not left_path.exists():
            typer.echo(f"{key}: missing from pyvalkey")
            return
        if not right_path.exists():
            typer.echo(f"{key}: not in valkey")
            return
        diffs_for_cmd = _diff(_load(left_path), _load(right_path))
        if not diffs_for_cmd:
            typer.echo(f"{key}: identical")
            return
        typer.echo(f"{key}: {len(diffs_for_cmd)} diff(s) (left=pyvalkey, right=valkey)")
        for line in diffs_for_cmd:
            typer.echo(f"  {line}")
        return

    any_flag = diffs or missing or added
    if not any_flag:
        typer.echo(f"pyvalkey dir: {pyvalkey_dir}")
        typer.echo(f"valkey dir:   {valkey_dir}")
        typer.echo(f"commands in pyvalkey:  {len(_collect(pyvalkey_dir))}")
        typer.echo(f"commands in valkey:    {len(_collect(valkey_dir))}")
        typer.echo(f"differing content:     {len(diff_names)}")
        typer.echo(f"missing from pyvalkey: {len(missing_list)}")
        typer.echo(f"added in pyvalkey:     {len(added_list)}")
        return

    if diffs:
        _echo_list("differing content", diff_names)
    if missing:
        _echo_list("missing from pyvalkey", missing_list)
    if added:
        _echo_list("added in pyvalkey", added_list)


if __name__ == "__main__":
    app()
