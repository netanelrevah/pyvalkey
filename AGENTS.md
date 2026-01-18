# AGENT.md - Project Context for pyvalkey

## Project Overview
- Name: pyvalkey
- Description: A pure Python implementation of a Valkey server using the RESP (Redis Serialization Protocol).
- Core Goal: Achieving 1:1 protocol compatibility with the official Valkey implementation.

## Project Structure
- pyvalkey/: Core source code including server logic, protocol parsing, and command handlers.
- tests/: Integration and compatibility test infrastructure.

## Testing and Compatibility Logic
This project employs a "Black-Box" testing methodology to ensure perfect parity with the original Valkey server:
- Official Test Suite: The project utilizes the original Valkey tests written in TCL, which are the same tests used by the official C-based Valkey repository.
- Dockerized Validation: The test runner initiates a Docker container containing the official Valkey environment and TCL test suite.
- Execution Flow: The TCL tests are directed to run against the pyvalkey Python server instance instead of the standard Valkey binary.
- Success Criteria: A feature is considered complete only when it passes the corresponding official TCL test cases without modifications to the test logic.

## Feature Source of Truth
To understand which commands and features are currently supported and verified, refer to the following file:
- File Path: pyvalkey/tests/test_valkey_docker/test_valkey.py
- Logic: This file explicitly defines which TCL tests are enabled and passing against the current Python implementation.
- Reference: Use this file to determine the current scope of the project before implementing new features or refactoring existing ones.

## Key Technical Components
- ValkeyServer: Located in pyvalkey/server.py, this class handles TCP connection management and the main request-response loop.
- RESP Handler: Responsible for decoding incoming byte streams into commands and encoding Python data structures back into valid RESP format.
- State Management: The server maintains an in-memory data store that must handle concurrent access if running in multi-threaded mode.

## Development and Execution
- Local Server Start: Run "python -m pyvalkey" to start the server on the default port.
- Compatibility Testing: Run "pytest" to trigger the Docker-based TCL test execution. Ensure Docker Desktop or Engine is active.
- Targeted Testing: Run specific test tags INSIDE test_pyvalkey_docker folder using "python -m pytest -s --tag=<tag_name> test_valkey.test_tag" (e.g., "pytest -v --tag=multi" for multi command tests)
- Dependencies: Managed via "uv" (refer to uv.lock and pyproject.toml).
- Development Setup: Install dev dependencies with "uv sync --all-extras"
- Try not to read all files and fill the context with it, use grep more to find relevant parts.

## Implementation Notes for AI Agents
- Always prioritize compatibility with Valkey TCL tests over cuttom implementation preferences.
- Consult pyvalkey/tests/test_valkey_docker/test_valkey.py to see the current coverage before proceeding.
- When adding new commands, refer to the official Valkey documentation for expected RESP return types.
- Ensure that any changes to the server loop do not break the thread-safe nature of the data store.
- Test with specific test tags INSIDE test_pyvalkey_docker folder using "python -m pytest -v --tag=<tag_name> test_valkey.test_tag" to run targeted tests