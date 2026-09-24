"""Start all TaskForge services with separate log files.

Usage:
    uv run python scripts/start_services.py          # start all
    uv run python scripts/start_services.py --no-reaper  # skip reaper

Logs land in logs/api.log, logs/worker.log, logs/reaper.log.
Press Ctrl+C to stop all services.
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
LOGS = ROOT / "logs"
PYTHON = sys.executable


def start(name: str, cmd: list[str], log_path: Path) -> subprocess.Popen:
    log_path.parent.mkdir(exist_ok=True)
    log_file = open(log_path, "a")
    proc = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=log_file,
        cwd=ROOT,
    )
    print(f"  {name:<10} pid={proc.pid}  log={log_path.relative_to(ROOT)}")
    return proc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-reaper", action="store_true", help="skip the reaper")
    parser.add_argument("--workers", type=int, default=1, help="number of workers")
    args = parser.parse_args()

    print("\nStarting TaskForge services...\n")

    procs: dict[str, subprocess.Popen] = {}

    procs["api"] = start(
        "api",
        [PYTHON, "-m", "uvicorn", "taskforge.api:app", "--reload", "--port", "8000"],
        LOGS / "api.log",
    )

    time.sleep(1)  # give the API a moment before workers connect

    for i in range(args.workers):
        label = "worker" if args.workers == 1 else f"worker-{i+1}"
        procs[label] = start(
            label,
            [PYTHON, "-m", "taskforge.worker"],
            LOGS / f"{label}.log",
        )

    if not args.no_reaper:
        procs["reaper"] = start(
            "reaper",
            [PYTHON, "-m", "taskforge.reaper"],
            LOGS / "reaper.log",
        )

    print(f"\nAll services running. Logs in logs/")
    print("Press Ctrl+C to stop.\n")

    def shutdown(sig, frame):
        print("\nStopping all services...")
        for name, proc in procs.items():
            proc.terminate()
        for name, proc in procs.items():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            print(f"  {name} stopped")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Watch for unexpected exits
    while True:
        for name, proc in procs.items():
            if proc.poll() is not None:
                print(f"\n  {name} exited unexpectedly (code {proc.returncode})")
                print(f"  check logs/{name}.log")
        time.sleep(2)


if __name__ == "__main__":
    main()
