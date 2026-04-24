from __future__ import annotations

import dataclasses
import inspect
import json
import sys
import types
import typing
from typing import TYPE_CHECKING, Any, TypeAlias

import typer
import yaml

import pyvalkey.commands  # noqa: F401  -- registers every command via its package __init__
from pyvalkey.commands.parameters import ParameterMetadata
from pyvalkey.commands.router import CommandsRouter
from pyvalkey.database_objects.acl import ACL

if TYPE_CHECKING:
    from pathlib import Path

    from pyvalkey.commands.core import Command

DOC_FIELD_KEYS = {"summary", "complexity", "since", "group", "function", "reply_schema"}
_REPLY_SCHEMA_ALIASES = ("reply_schema", "reply_scheme")

JsonValue: TypeAlias = "None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]"


class _NullAsStringLoader(yaml.SafeLoader):
    pass


_NullAsStringLoader.add_constructor(
    "tag:yaml.org,2002:null",
    lambda loader, node: loader.construct_scalar(node) or "null",
)


def _normalize_reply_schema(value: JsonValue, parent_key: str | None = None) -> JsonValue:
    if isinstance(value, dict):
        return {k: _normalize_reply_schema(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_reply_schema(item, parent_key) for item in value]
    if value is None and parent_key == "type":
        return "null"
    return value


def _parse_docstring(command_cls: type[Command]) -> dict[str, Any]:
    docstring = inspect.getdoc(command_cls)
    if not docstring or docstring.startswith(f"{command_cls.__name__}("):
        return {}
    try:
        parsed = yaml.load(docstring, Loader=_NullAsStringLoader)
    except yaml.YAMLError:
        return {"summary": docstring}
    if not isinstance(parsed, dict):
        return {"summary": docstring}

    fields: dict[str, Any] = {}
    for key, value in parsed.items():
        if not isinstance(key, str):
            continue
        if key in _REPLY_SCHEMA_ALIASES:
            fields["reply_schema"] = _normalize_reply_schema(value)
        elif key in DOC_FIELD_KEYS:
            fields[key] = value
    return fields


def _group_from_module(command_cls: type[Command]) -> str | None:
    module_name = command_cls.__module__.rsplit(".", 1)[-1]
    if not module_name.endswith("_commands"):
        return None
    return module_name[: -len("_commands")].replace("_", "-") or None


KEY_MODE_FLAGS: dict[bytes, list[str]] = {
    b"R": ["RO", "ACCESS"],
    b"W": ["OW", "UPDATE"],
    b"RW": ["RW", "UPDATE"],
}


def _iter_all_commands() -> list[tuple[bytes, type[Command]]]:
    results: list[tuple[bytes, type[Command]]] = []
    for name, entry in CommandsRouter.ROUTES.items():
        if isinstance(entry, dict):
            for sub_name, sub_cls in entry.items():
                results.append((name + b"|" + sub_name, sub_cls))
        else:
            results.append((name, entry))
    return results


def _unwrap_optional(annotation: object) -> tuple[object, bool]:
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        is_optional = len(args) != len(typing.get_args(annotation))
        if len(args) == 1:
            return args[0], is_optional
        return annotation, is_optional
    return annotation, False


def _annotation_type(annotation: object) -> tuple[str | None, bool]:
    """Return (json_type, is_multiple). Returns (None, _) when unknown."""
    annotation, _ = _unwrap_optional(annotation)
    origin = typing.get_origin(annotation)
    if origin in (list, tuple, set, frozenset):
        inner_args = typing.get_args(annotation)
        if inner_args:
            inner_type, _ = _annotation_type(inner_args[0])
        else:
            inner_type = None
        return inner_type, True
    if annotation in (bytes, str):
        return "string", False
    if annotation is int:
        return "integer", False
    if annotation is float:
        return "double", False
    if annotation is bool:
        return "boolean", False
    return None, False


def _compute_arity(command_cls: type[Command], is_subcommand: bool) -> int:
    minimum = 1 + (1 if is_subcommand else 0)
    variadic = False
    for field in dataclasses.fields(command_cls):
        metadata = field.metadata
        if not metadata.get(ParameterMetadata.COMMAND_PARAMETER):
            continue

        is_optional = field.default is not dataclasses.MISSING or field.default_factory is not dataclasses.MISSING  # type: ignore[misc]

        annotation = field.type
        if isinstance(annotation, str):
            try:
                annotation = eval(annotation, vars(sys.modules[command_cls.__module__]))
            except Exception:
                annotation = None
        _, is_multiple = _annotation_type(annotation)

        if is_optional:
            variadic = True
            continue

        if is_multiple:
            variadic = True

        token_count = 1 if metadata.get(ParameterMetadata.TOKEN) is not None else 0
        minimum += 1 + token_count

    return -minimum if variadic else minimum


def _build_arguments_and_key_specs(
    command_cls: type[Command],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    arguments: list[dict[str, Any]] = []
    key_specs: list[dict[str, Any]] = []
    position = 1
    for field in dataclasses.fields(command_cls):
        metadata = field.metadata
        if not metadata.get(ParameterMetadata.COMMAND_PARAMETER):
            continue

        argument: dict[str, Any] = {"name": field.name}
        annotation = field.type
        if isinstance(annotation, str):
            try:
                annotation = eval(annotation, vars(sys.modules[command_cls.__module__]))
            except Exception:
                annotation = None

        json_type, is_multiple = _annotation_type(annotation)

        key_mode = metadata.get(ParameterMetadata.KEY_MODE)
        if key_mode is not None:
            argument["type"] = "key"
            argument["key_spec_index"] = len(key_specs)
            key_specs.append(
                {
                    "flags": KEY_MODE_FLAGS.get(key_mode, []),
                    "begin_search": {"index": {"pos": position}},
                    "find_keys": {"range": {"lastkey": 0, "step": 1, "limit": 0}},
                }
            )
        elif json_type is not None:
            argument["type"] = json_type

        token = metadata.get(ParameterMetadata.TOKEN)
        if token is not None:
            if isinstance(token, (bytes, bytearray)):
                argument["token"] = bytes(token).decode()
            elif isinstance(token, dict):
                argument["token"] = sorted(
                    k.decode() if isinstance(k, (bytes, bytearray)) else str(k) for k in token.keys()
                )

        if field.default is not dataclasses.MISSING or field.default_factory is not dataclasses.MISSING:  # type: ignore[misc]
            argument["optional"] = True

        if is_multiple:
            argument["multiple"] = True

        arguments.append(argument)
        position += 1
    return arguments, key_specs


def build_command_json(full_command_name: bytes, command_cls: type[Command]) -> dict[str, Any]:
    parts = full_command_name.split(b"|")
    top_key = parts[-1].decode().upper()
    body: dict[str, Any] = {}

    is_subcommand = len(parts) > 1
    if is_subcommand:
        body["container"] = parts[0].decode().upper()

    body["arity"] = _compute_arity(command_cls, is_subcommand)

    doc_fields = _parse_docstring(command_cls)
    for key in ("summary", "complexity", "since", "function"):
        if key in doc_fields:
            body[key] = doc_fields[key]

    group = doc_fields.get("group") or _group_from_module(command_cls)
    if group:
        body["group"] = group

    reply_schema = getattr(command_cls, "__reply_schema__", None)
    if reply_schema is not None:
        body["reply_schema"] = _normalize_reply_schema(reply_schema)
    elif "reply_schema" in doc_fields:
        body["reply_schema"] = doc_fields["reply_schema"]

    flags = getattr(command_cls, "flags", set()) or set()
    if flags:
        body["command_flags"] = [f.decode().upper() for f in flags]

    acl_categories = ACL.COMMAND_CATEGORIES.get(full_command_name, set()) or set()
    if acl_categories:
        body["acl_categories"] = [c.decode().upper() for c in acl_categories]

    arguments, key_specs = _build_arguments_and_key_specs(command_cls)
    if key_specs:
        body["key_specs"] = key_specs
    if arguments:
        body["arguments"] = arguments

    return {top_key: body}


def export_all(output_dir: Path) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for full_name, command_cls in _iter_all_commands():
        payload = build_command_json(full_name, command_cls)
        filename = full_name.decode().replace("|", "-") + ".json"
        (output_dir / filename).write_text(json.dumps(payload, indent=4) + "\n", encoding="utf-8")
        count += 1
    return count


app = typer.Typer(add_completion=False)


@app.command()
def main(
    output_dir: Path = typer.Argument(..., help="Directory to write command JSON files into"),
) -> None:
    """Export every registered pyvalkey command as a valkey-style JSON file."""
    count = export_all(output_dir)
    typer.echo(f"Wrote {count} command JSON file(s) to {output_dir}")


if __name__ == "__main__":
    app()
