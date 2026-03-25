"""Sync Lua 5.1 sources from Valkey repository.

Usage:
    python scripts/sync_valkey_lua.py --valkey-repo /path/to/valkey

Copies all .c and .h files from Valkey's deps/lua/src/ into
third_party/lua51/src/, records the Valkey commit hash, and patches
the solarisfixes.h include path in lua_cjson.c.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = SCRIPT_DIR.parent
DEST_DIR = PACKAGE_ROOT / "third_party" / "lua51" / "src"
VERSION_FILE = PACKAGE_ROOT / "third_party" / "VALKEY_VERSION"

EXCLUDE_FILES = {"lua.c", "luac.c", "print.c"}

SOLARISFIXES_STUB = """\
/* Solaris specific fixes - stub for standalone build.
 * Original lives in valkey/src/solarisfixes.h.
 * Only active on Solaris (__sun). */

#if defined(__sun)

#if defined(__GNUC__)
#include <math.h>
#undef isnan
#define isnan(x) \\
    __extension__({ __typeof(x) __x_a = (x); __builtin_expect(__x_a != __x_a, 0); })
#undef isfinite
#define isfinite(x) \\
    __extension__({ __typeof(x) __x_f = (x); __builtin_expect(!isnan(__x_f - __x_f), 1); })
#undef isinf
#define isinf(x) \\
    __extension__({ __typeof(x) __x_i = (x); __builtin_expect(!isnan(__x_i) && !isfinite(__x_i), 0); })
#define u_int uint
#define u_int32_t uint32_t
#endif /* __GNUC__ */

#endif /* __sun */
"""


def get_valkey_commit(valkey_repo: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=valkey_repo,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def sync(valkey_repo: Path) -> None:
    src_dir = valkey_repo / "deps" / "lua" / "src"
    if not src_dir.is_dir():
        raise FileNotFoundError(f"Lua source directory not found: {src_dir}")

    DEST_DIR.mkdir(parents=True, exist_ok=True)

    for existing in DEST_DIR.iterdir():
        if existing.suffix in (".c", ".h"):
            existing.unlink()

    copied = 0
    for src_file in sorted(src_dir.iterdir()):
        if src_file.suffix not in (".c", ".h"):
            continue
        if src_file.name in EXCLUDE_FILES:
            continue
        shutil.copy2(src_file, DEST_DIR / src_file.name)
        copied += 1

    cjson_path = DEST_DIR / "lua_cjson.c"
    if cjson_path.exists():
        content = cjson_path.read_text()
        content = content.replace('#include "../../../src/solarisfixes.h"', '#include "solarisfixes.h"')
        cjson_path.write_text(content)

    (DEST_DIR / "solarisfixes.h").write_text(SOLARISFIXES_STUB)

    commit = get_valkey_commit(valkey_repo)
    VERSION_FILE.write_text(commit + "\n")

    print(f"Synced {copied} files from {src_dir}")
    print(f"Valkey commit: {commit}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Lua 5.1 sources from Valkey")
    parser.add_argument(
        "--valkey-repo",
        type=Path,
        default=Path(os.environ.get("VALKEY_REPO", "")),
        help="Path to local Valkey repository",
    )
    args = parser.parse_args()

    if not args.valkey_repo or not args.valkey_repo.is_dir():
        parser.error("--valkey-repo must point to a valid Valkey repository directory")

    sync(args.valkey_repo)


if __name__ == "__main__":
    main()
