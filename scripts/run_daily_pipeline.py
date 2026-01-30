"""run_daily_pipeline.py

Single entry point to run the full daily pipeline end-to-end.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path.cwd()
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
LOG_DIR = PROJECT_ROOT / "logs"

STEPS = ["01_ingest.py", "02_match.py", "03_rules.py", "04_aggregate.py"]


def _run(step: str) -> None:
    cmd = [sys.executable, str(SCRIPTS_DIR / step), "--force"]
    print(f"\n=== Running {step} ===", flush=True)
    res = subprocess.run(cmd)
    if res.returncode != 0:
        raise RuntimeError(f"Step failed: {step}")


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Starting daily pipeline at {datetime.now():%Y-%m-%d %H:%M:%S}")
    for step in STEPS:
        _run(step)
    print("Daily pipeline finished successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
