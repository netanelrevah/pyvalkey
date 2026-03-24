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
```

## Architecture

**Entry point**: `pyvalkey/__main__.py` → `pyvalkey/server.py` (`ValkeyServer`)

**Request lifecycle**: TCP connection → `ValkeyClientProtocol` (asyncio) → `resp.py` (decode bytes → command) → `commands/router.py` (`CommandsRouter`) → command class → RESP-encoded response

**Key directories**:
- `pyvalkey/commands/` — one file per command group (e.g. `string_commands.py`, `list_commands.py`). Each command is a class extending `Command` with `parse()`, `execute()`, and optional `before()`/`after()` hooks.
- `pyvalkey/commands/lua/` — Lua scripting via Lupa (Python↔Lua 5.1 bridge). `scripts.py` contains Lua template strings; `helpers.py` provides bridge utilities including the `lua_safe` decorator.
- `pyvalkey/database_objects/` — in-memory data structures (databases, sorted sets, streams, etc.)
- `tests/test_valkey_docker/` — Docker-based black-box tests using the official Valkey TCL test suite

**Adding a new command**: create a class in the relevant `*_commands.py` file extending `Command`, register it in the router. Consult existing commands for the pattern.

## Lua↔Python Boundary Rules

Lupa (Python↔Lua 5.1) has sharp edges that must be respected:

**`@lua_safe` on every Python callable exposed to Lua.** `debug.sethook` fires callbacks mid-instruction; Lupa passes the interrupted Lua frame's stack arguments into the Python callback as extra positional args. `@lua_safe` (in `helpers.py`) truncates `*args` to the function's declared parameter count, absorbing any leaked args. Apply it to every method or function passed to Lua via `debug.sethook` or as a Lua function argument.

**No metatable proxies on tables used during `debug.sethook` execution.** When `debug.sethook` is active (count mode), wrapping a Lua table in a metatable proxy (e.g. `setmetatable({}, {__index = t})`) can cause Lupa closure upvalue corruption — the hook fires during metamethod resolution and swaps captured variables, leading to `'SomeObject' is not callable` errors. This affects `fill_server_globals` (EVAL/FCALL context): assign the `redis`/`server` table directly to globals instead of wrapping it. `fill_load_server_globals` (FUNCTION LOAD context) can safely use `LUA_MAKE_READONLY_SERVER_TABLE` because the load executor's `debug.sethook` does not interact with `redis.call`/`redis.pcall` closures.

**No Lua closures as table values in env tables used during sethook.** Having Lua closures (even pure Lua, no Python upvalues) as direct entries in a table that the sethook-monitored code accesses causes upvalue corruption. Use tables with `__call` metamethods instead: `setmetatable({}, {__call = function(...) error("msg") end})` works safely as a table entry because the `__call` metamethod is only invoked when the entry is explicitly called, not during hash lookups. Similarly, modifying `_G` entries (replacing existing globals) before or after sethook execution accumulates corruption across multiple loads. Pre-create all Lua objects (blockers, env setup functions) once in `__post_init__` and reuse them — never call `lua_runtime.eval()` per-load.

**Two separate Lua runtimes.** `FunctionsCompiler` maintains `_writeable_lua_runtime` and `_readonly_lua_runtime` — completely isolated Lua states. Functions are compiled in both. The correct runtime is selected via `get(writeable)` / `get_runtime(writeable)`.

**Bare `\n` in Lua error messages.** Lua errors propagated through `LuaError` may contain stack traces with newlines. Always use `.split(b"\n")[0]` to extract just the error message before raising `ServerError`.

## Testing Methodology

Tests run the official Valkey TCL test suite inside Docker, pointed at the pyvalkey server instead of the real Valkey binary. A feature is complete only when its official TCL tests pass.

- Test results are in `tests/test_valkey_docker/<tag>.docker.log`
- The Docker container exit code may not reflect actual test results — always read the log
- Log markers: `[ok]` passed, `[fail]` failed, `[err]` exception, `[ignore]` skipped
- `tests/test_valkey_docker/test_valkey.py` is the source of truth for what is currently covered
- When touching Lua/scripting code, always validate with **both** `--tag=functions` and `--tag=scripting`. The scripting suite exercises FUNCTION LOAD + FCALL in rapid succession (via `run_script` with `is_eval=0`), which exposes Lupa upvalue corruption that the functions suite alone doesn't catch.

## Code Style

See `AGENTS.md` for the full style guide. Key rules:
- No comments or docstrings
- No nested `if` — use fast returns
- DRY: extract to variables/functions/constants when it aids readability
- Imports at top of file only
- Use `is`/direct evaluation instead of `== True`/`== False`
