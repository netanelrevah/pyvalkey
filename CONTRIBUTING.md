# Contributing

## Overview

pyvalkey is a pure Python implementation of a Valkey/Redis server. It creates a TCP server that speaks the RESP protocol and aims for 1:1 protocol compatibility with the official Valkey implementation.

## Architecture

**Entry point**: `pyvalkey/__main__.py` → `pyvalkey/server.py` (`ValkeyServer`)

**Request lifecycle**:
```
TCP connection → ValkeyClientProtocol (asyncio)
  → resp.py (decode bytes → command list)
  → commands/router.py (CommandsRouter.route)
  → Command subclass (parse → execute)
  → RESP-encoded response
```

### Key directories

- `pyvalkey/commands/` — one file per command group (e.g. `string_commands.py`, `list_commands.py`). Each command is a `@dataclass` class extending `Command` with `parse()` and `execute()`, registered via `@CommandsRouter.command(...)`.
- `pyvalkey/commands/lua/` — Lua scripting via the vendored `lua_runtime` package (Valkey's patched Lua 5.1 + Lupa Cython wrapper). `scripts.py` contains Lua template strings; `helpers.py` provides bridge utilities.
- `lua_runtime/` — workspace package: vendored Lua 5.1 with Valkey's readonly table patch and C extension modules (cjson, cmsgpack, bit, struct). Built with setuptools.
- `pyvalkey/database_objects/` — in-memory data structures (databases, sorted sets, streams, etc.)
- `tests/test_valkey_docker/` — Docker-based black-box tests using the official Valkey TCL test suite.

### Adding a new command

1. Find or create the relevant `*_commands.py` file in `pyvalkey/commands/`.
2. Define a `@dataclass` class extending `Command`.
3. Decorate with `@CommandsRouter.command(b"name", acl_categories={...})`.
4. Implement `parse(parameters)` and `execute(namespace)`.
5. Consult the official Valkey documentation for expected RESP return types.
6. Enable the corresponding TCL test tag in `tests/test_valkey_docker/test_valkey.py`.

### Key Technical Components

- **ValkeyServer** (`pyvalkey/server.py`) — handles TCP connection management and the main request-response loop.
- **RESP Handler** (`pyvalkey/resp.py`) — decodes incoming byte streams into commands and encodes Python data structures back into valid RESP format.
- **State Management** — in-memory data store; must be safe under concurrent access in async handlers.

## Lua ↔ Python Boundary Rules

The `lua_runtime` workspace package vendors Valkey's patched Lua 5.1 with a Cython wrapper (forked from Lupa).

- **Auto arg truncation.** The Cython layer caches each Python callable's parameter count and truncates extra Lua stack args before calling Python. No `@lua_safe` decorator needed — all Python functions exposed to Lua are automatically safe from `debug.sethook` stack leaks.
- **`set_readonly()` for table protection.** Valkey's Lua 5.1 patch adds a `readonly` flag to Lua tables, checked at the C level in `rawset` / `settable` / `setmetatable`. Use `table.set_readonly(True)` instead of metamethod proxies (`__newindex = error`). This eliminates the upvalue corruption caused by metamethod resolution during `debug.sethook`.
- **Single Lua runtime.** `FunctionsCompiler` has one `lua_runtime`. `CallContext.readonly` is flipped before each call — the `_call()` function checks it dynamically.
- **C extension modules.** cjson, cmsgpack, bit, and struct are loaded from Valkey's C implementations in `lua_runtime.__cinit__`. No Python reimplementations needed.
- **Python closures for Lua-exposed callbacks.** Never bind Python objects as Lua closure upvalues — `debug.sethook` corrupts them. Use Python closures (e.g. `_make_call_wrapper` in `helpers.py`) that capture Python objects in Python scope, then pass the closure to Lua as an opaque callable. Lua tables as upvalues are also unsafe.
- **`table()` vs `table_from()` key types.** With `encoding=None`, `lua_runtime.table(ok=b'OK')` stores the key as str `"ok"`; `lua_runtime.table_from({b'ok': b'OK'})` stores it as bytes. Use `table_from()` when keys must match bytes-based lookups.
- **Bare `\n` in Lua error messages.** Always use `.split(b"\n")[0]` to extract just the error message before raising `ServerError`.

## Testing

The project uses a black-box testing methodology to ensure parity with the official Valkey server:

- **Official test suite**: the project runs the original Valkey TCL tests (the same tests used by the official C-based Valkey repository).
- **Dockerized validation**: the test runner starts a Docker container with the official Valkey environment and TCL test suite, pointed at the pyvalkey Python server instead of the Valkey binary.
- **Success criteria**: a feature is considered complete only when its corresponding official TCL test cases pass without modification.

### Commands

```bash
uv sync --all-extras                                                        # Install dev dependencies
uv run pytest                                                               # Run all tests
uv run pytest -v -k test_tag --tag=<tag_name> tests/test_valkey_docker/test_valkey.py  # Run a specific test tag
uv run ruff check .                                                         # Lint
uv run ty check                                                             # Type check
python -m pyvalkey                                                          # Start the server
```

When touching Lua/scripting code, always validate with **both** `--tag=functions` and `--tag=scripting`. The scripting suite exercises FUNCTION LOAD + FCALL in rapid succession (via `run_script` with `is_eval=0`), which exposes Lupa upvalue corruption that the functions suite alone doesn't catch.

### Test output files

Test results are saved in Docker log files at `tests/test_valkey_docker/<tag>.docker.log`. Log markers:

- `[ok]` — test passed
- `[fail]` — test failed (check error details)
- `[err]` — test encountered an exception
- `[ignore]` — test was skipped due to tags or missing features

**Important**: the Docker container exit status may not reflect the actual test results. Always check the log file to verify individual outcomes — some tests pass even when the container exits with a non-zero status.

### Log analysis rules

- **Never read `*.server.log` directly** — these files can exceed 40k lines. Use `grep` or the `/run_valkey_test` skill.
- `*.docker.log` is small (~500 lines) and safe to read directly.

### Feature source of truth

`tests/test_valkey_docker/test_valkey.py` explicitly lists which TCL tests are enabled and passing against the current Python implementation. Consult it to determine the current scope before implementing new features or refactoring existing ones.

### Troubleshooting test failures

1. **Docker container exit status**: the pytest assertion `assert status["StatusCode"] == 0` may fail even when individual tests pass. Verify by reading the actual test output in `<tag>.docker.log`.
2. **Test result markers**: check for `[ok]`, `[fail]`, `[err]`, `[ignore]` in the log.
3. **WATCH/EXEC tests**: for transaction-related tests, verify expired keys are marked as touched, EXEC returns empty array when watched keys were modified, and check `multi.docker.log` for WATCH test results.

### Rebuilding the Lua runtime

```bash
# Regenerate _lua_runtime.c from the Cython .pyx source
uv run cython --include-dir lua_runtime/lua_runtime lua_runtime/lua_runtime/_lua_runtime.pyx --output-file lua_runtime/lua_runtime/_lua_runtime.c

# Rebuild the C extension after changing .pyx or .c (or luaconf.h)
uv sync --reinstall-package lua-runtime
```

## Build Artifacts

When adding new packages, build targets, or tooling that produces generated files, verify `.gitignore` has entries for their artifacts before staging. Common examples: compiled extensions (`.pyd`, `.so`), `*.egg-info/`, `build/`, generated `.c` from Cython.

## Workflow

**Always run pre-commit before committing and fix all failures before proceeding:**

```bash
uv run pre-commit run --all-files
```

Only commit once all hooks pass.

## Code Style

See [CODE_STYLE.md](CODE_STYLE.md).