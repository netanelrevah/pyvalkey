# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is pyvalkey

A pure Python implementation of a Valkey/Redis server. It creates a TCP server that speaks the RESP protocol and aims for 1:1 protocol compatibility with the official Valkey implementation.

## Commands

```bash
# Install dev dependencies
uv sync --all-extras

# Run all tests
uv run pytest

# Run a specific test tag
uv run pytest -v -k test_tag --tag=<tag_name> tests/test_valkey_docker/test_valkey.py

# Lint
ruff check .

# Type check
mypy .

# Start server
python -m pyvalkey

# Regenerate _lua_runtime.c from the Cython .pyx source (needed after editing .pyx)
uv run cython --include-dir lua_runtime/lua_runtime lua_runtime/lua_runtime/_lua_runtime.pyx --output-file lua_runtime/lua_runtime/_lua_runtime.c

# Rebuild the lua_runtime C extension after changing .pyx or .c (or luaconf.h)
uv sync --reinstall-package lua-runtime
```

## Architecture

**Entry point**: `pyvalkey/__main__.py` → `pyvalkey/server.py` (`ValkeyServer`)

**Request lifecycle**: TCP connection → `ValkeyClientProtocol` (asyncio) → `resp.py` (decode bytes → command) → 
`commands/router.py` (`CommandsRouter`) → command class → RESP-encoded response

**Key directories**:
- `pyvalkey/commands/` — one file per command group (e.g. `string_commands.py`, `list_commands.py`). Each command is
  a class extending `Command` with `parse()`, `execute()`, and optional `before()`/`after()` hooks.
- `pyvalkey/commands/lua/` — Lua scripting via vendored `lua_runtime` package (Valkey's patched Lua 5.1 + Lupa Cython
  wrapper). `scripts.py` contains Lua template strings; `helpers.py` provides bridge utilities.
- `lua_runtime/` — workspace package: vendored Lua 5.1 with Valkey's readonly table patch and C extension modules
  (cjson, cmsgpack, bit, struct). Built with setuptools.
- `pyvalkey/database_objects/` — in-memory data structures (databases, sorted sets, streams, etc.)
- `tests/test_valkey_docker/` — Docker-based black-box tests using the official Valkey TCL test suite

**Adding a new command**: create a class in the relevant `*_commands.py` file extending `Command`, register it in the
router. Consult existing commands for the pattern.

## Lua↔Python Boundary Rules

The `lua_runtime` workspace package vendors Valkey's patched Lua 5.1 with a Cython wrapper (forked from Lupa). 
Key features:

**Auto arg truncation.** The Cython layer caches each Python callable's parameter count and truncates extra Lua stack
args before calling Python. No `@lua_safe` decorator needed — all Python functions exposed to Lua are automatically 
safe from `debug.sethook` stack leaks.

**`set_readonly()` for table protection.** Valkey's Lua 5.1 patch adds a `readonly` flag to Lua tables, checked at the
C level in `rawset`/`settable`/`setmetatable`. Use `table.set_readonly(True)` instead of metamethod proxies 
(`__newindex = error`). This eliminates the upvalue corruption caused by metamethod resolution during `debug.sethook`.

**Single Lua runtime.** `FunctionsCompiler` has one `lua_runtime`. `CallContext.readonly` is flipped before each 
call — the `_call()` function checks it dynamically.

**C extension modules.** cjson, cmsgpack, bit, and struct are loaded from Valkey's C implementations in 
`lua_runtime.__cinit__`. No Python reimplementations needed.

**Python closures for Lua-exposed callbacks.** Never bind Python objects as Lua closure upvalues — `debug.sethook` 
corrupts them. Instead, use Python closures (e.g., `_make_call_wrapper` in `helpers.py`) that capture Python 
objects in Python scope, then pass the closure to Lua as an opaque callable. Lua tables as upvalues are also
unsafe — the corruption affects table values too, not just direct upvalues.

**`table()` vs `table_from()` key types.** With `encoding=None`, `lua_runtime.table(ok=b'OK')` stores the key as str
`"ok"` (Python kwarg), while `lua_runtime.table_from({b'ok': b'OK'})` stores it as bytes `b'ok'`. Lua attribute access
uses bytes keys. Use `table_from()` when keys must match bytes-based lookups (e.g., `b"ok" in lua_value`).

**Bare `\n` in Lua error messages.** Lua errors propagated through `LuaError` may contain stack traces with newlines.
Always use `.split(b"\n")[0]` to extract just the error message before raising `ServerError`.

## Testing Methodology

Tests run the official Valkey TCL test suite inside Docker, pointed at the pyvalkey server instead of the real 
Valkey binary. A feature is complete only when its official TCL tests pass.

- Test results are in `tests/test_valkey_docker/<tag>.docker.log`
- The Docker container exit code may not reflect actual test results — always read the log
- Log markers: `[ok]` passed, `[fail]` failed, `[err]` exception, `[ignore]` skipped
- `tests/test_valkey_docker/test_valkey.py` is the source of truth for what is currently covered
- When touching Lua/scripting code, always validate with **both** `--tag=functions` and `--tag=scripting`. The
  scripting suite exercises FUNCTION LOAD + FCALL in rapid succession (via `run_script` with `is_eval=0`), which 
  exposes Lupa upvalue corruption that the functions suite alone doesn't catch.

## Build Artifacts

When adding new packages, build targets, or tooling that produces generated files, verify `.gitignore` has entries 
for their artifacts before staging. Common examples: compiled extensions (`.pyd`, `.so`), `*.egg-info/`, `build/`,
generated `.c` from Cython.

## Workflow Corrections

When the user corrects a workflow mistake (e.g., forgetting `.gitignore` entries, wrong test command), save the 
correction to **CLAUDE.md** — not just memory. CLAUDE.md is loaded every session and is the authoritative source 
for project rules.

## Code Style

See `AGENTS.md` for the full style guide. Key rules:
- No comments or docstrings
- No nested `if` — use fast returns
- DRY: extract to variables/functions/constants when it aids readability
- Imports at top of file only
- Use `is`/direct evaluation instead of `== True`/`== False`
