"""Python entrypoint shims for auxiliary cue-shell binaries.

The PyPI package is built from the `cue-cli` Cargo package so maturin installs
`cue` and `cued` directly.  The auxiliary command names are owned by their own
Cargo crates in source builds; these shims make `uv tool install cue-shell`
expose the same command set while delegating to the canonical Rust binaries
when they are available next to `cue`.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _run_companion(program: str) -> int:
    cue_path = Path(sys.executable).with_name("cue")
    companion = cue_path.with_name(program)
    if companion.exists():
        completed = subprocess.run([str(companion), *sys.argv[1:]], check=False)
        return int(completed.returncode)

    message = (
        f"{program} is not bundled as a Rust binary in this wheel. "
        f"Install it from source with `cargo install --path crates/{program}` "
        "or use the bundled short commands `cue`/`cued`."
    )
    if program == "cue-daemon":
        completed = subprocess.run([str(cue_path.with_name("cued")), *sys.argv[1:]], check=False)
        return int(completed.returncode)
    print(message, file=sys.stderr)
    return 127


def cue_daemon() -> None:
    raise SystemExit(_run_companion("cue-daemon"))


def cue_client() -> None:
    raise SystemExit(_run_companion("cue-client"))


def cue_tui() -> None:
    raise SystemExit(_run_companion("cue-tui"))
