"""Portable FinFlow ingestion and dbt orchestration."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import subprocess
import sys
from typing import Optional, Sequence

from ingestion.pipeline_utils import PipelineError, configure_logging, normalize_ticker


LOGGER = logging.getLogger(__name__)
REPOSITORY_ROOT = Path(__file__).resolve().parent
INGESTION_TIMEOUT_SECONDS = 600
DBT_TIMEOUT_SECONDS = 900


def dbt_executable() -> str:
    executable_name = "dbt.exe" if sys.platform == "win32" else "dbt"
    adjacent = Path(sys.executable).resolve().parent / executable_name
    return str(adjacent) if adjacent.is_file() else "dbt"


def run_stage(
    stage_name: str,
    command: Sequence[str],
    *,
    cwd: Path,
    timeout: int,
) -> int:
    LOGGER.info("Starting stage: %s", stage_name)
    try:
        completed = subprocess.run(
            list(command),
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=False,
            check=False,
        )
    except subprocess.TimeoutExpired:
        LOGGER.error("Stage timed out: %s", stage_name)
        return 124
    except OSError:
        LOGGER.error("Stage could not be started: %s", stage_name)
        return 1

    if completed.returncode != 0:
        LOGGER.error("Stage failed with exit code %s: %s", completed.returncode, stage_name)
    else:
        LOGGER.info("Completed stage: %s", stage_name)
    return completed.returncode


def run_pipeline(ticker: Optional[str] = None) -> int:
    stock_command = [sys.executable, str(REPOSITORY_ROOT / "ingestion" / "ingest_stocks.py")]
    if ticker:
        stock_command.append(ticker)
    stock_code = run_stage(
        "stock ingestion",
        stock_command,
        cwd=REPOSITORY_ROOT,
        timeout=INGESTION_TIMEOUT_SECONDS,
    )
    if stock_code != 0:
        LOGGER.error("Ingestion failed; dbt build was not started.")
        return stock_code

    if not ticker:
        macro_code = run_stage(
            "macro ingestion",
            [sys.executable, str(REPOSITORY_ROOT / "ingestion" / "ingest_macro.py")],
            cwd=REPOSITORY_ROOT,
            timeout=INGESTION_TIMEOUT_SECONDS,
        )
        if macro_code != 0:
            LOGGER.error("Ingestion failed; dbt build was not started.")
            return macro_code

    return run_stage(
        "dbt build",
        [dbt_executable(), "build"],
        cwd=REPOSITORY_ROOT / "finflow_dbt",
        timeout=DBT_TIMEOUT_SECONDS,
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run the FinFlow production pipeline.")
    parser.add_argument("--ticker", help="Run stock ingestion for one ticker and skip macro ingestion")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    configure_logging()
    args = parse_args(argv)
    ticker = None
    if args.ticker:
        try:
            ticker = normalize_ticker(args.ticker)
        except PipelineError as exc:
            LOGGER.error("%s", exc.public_message)
            return 2
    return run_pipeline(ticker)


if __name__ == "__main__":
    raise SystemExit(main())
