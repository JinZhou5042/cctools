"""Stable paths for benchmark results and supporting diagnostic logs."""

from pathlib import Path


def log_path(output, kind):
    output = Path(output).resolve()
    if output.parent.name == "results" and output.parent.parent.name == "raw":
        log_root = output.parent.parent / "logs"
        log_root.mkdir(parents=True, exist_ok=True)
        return log_root / f"{output.stem}.{kind}.log"
    return Path(f"{output}.{kind}.log")
