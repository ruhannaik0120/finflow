"""Shared reliability primitives for FinFlow ingestion pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import logging
import math
import os
import re
from types import MappingProxyType
from typing import Callable, Mapping, Optional, Sequence
from uuid import uuid4

import pandas as pd
import snowflake.connector


AUDIT_TABLE = "FINFLOW_DB.RAW.pipeline_logs"
SNOWFLAKE_VARIABLES = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_WAREHOUSE",
)
VALID_PIPELINES = frozenset({"STOCK_INGESTION", "MACRO_INGESTION"})
VALID_STATUSES = frozenset(
    {"SUCCESS", "PARTIAL_SUCCESS", "NO_CHANGES", "NO_VALID_ROWS", "FAILED"}
)
TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")
TEMP_TABLE_PATTERN = re.compile(r"^FF_(?:STOCK|MACRO)_[0-9A-F]{32}$")

AUDIT_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    run_id VARCHAR(36) NOT NULL,
    run_timestamp TIMESTAMP_TZ NOT NULL,
    completed_at TIMESTAMP_TZ NOT NULL,
    pipeline_name VARCHAR(32) NOT NULL,
    entity_name VARCHAR(128) NOT NULL,
    rows_fetched NUMBER(38,0) NOT NULL,
    rows_valid NUMBER(38,0) NOT NULL,
    rows_inserted NUMBER(38,0) NOT NULL,
    rows_updated NUMBER(38,0) NOT NULL,
    rows_unchanged NUMBER(38,0) NOT NULL,
    rows_dropped NUMBER(38,0) NOT NULL,
    status VARCHAR(32) NOT NULL,
    duration_ms NUMBER(38,0) NOT NULL,
    error_code VARCHAR(64),
    error_message VARCHAR(500)
)
"""

AUDIT_INSERT_SQL = f"""
INSERT INTO {AUDIT_TABLE} (
    run_id, run_timestamp, completed_at, pipeline_name, entity_name,
    rows_fetched, rows_valid, rows_inserted, rows_updated, rows_unchanged,
    rows_dropped, status, duration_ms, error_code, error_message
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


@dataclass(frozen=True)
class NormalizationResult:
    dataframe: pd.DataFrame
    rows_fetched: int
    rows_valid: int
    rows_dropped: int
    dropped_by_reason: Mapping[str, int]


@dataclass(frozen=True)
class MergeResult:
    rows_inserted: int
    rows_updated: int
    rows_unchanged: int


@dataclass(frozen=True)
class PipelineRunResult:
    run_id: str
    pipeline_name: str
    entity_name: str
    rows_fetched: int
    rows_valid: int
    rows_inserted: int
    rows_updated: int
    rows_unchanged: int
    rows_dropped: int
    status: str
    error_code: Optional[str]
    started_at: datetime
    completed_at: datetime
    duration_ms: int


class PipelineError(Exception):
    """An expected pipeline failure with a fixed, public-safe error code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.public_message = message


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def elapsed_ms(started_at: datetime, completed_at: datetime) -> int:
    return max(0, int((completed_at - started_at).total_seconds() * 1000))


def validate_environment(
    required: Sequence[str], environ: Optional[Mapping[str, str]] = None
) -> Mapping[str, str]:
    source = os.environ if environ is None else environ
    missing = sorted(name for name in required if not source.get(name))
    if missing:
        raise PipelineError(
            "CONFIGURATION_ERROR",
            "Missing required environment variables: " + ", ".join(missing),
        )
    return {name: source[name] for name in required}


def create_snowflake_connection(
    environ: Optional[Mapping[str, str]] = None,
):
    config = validate_environment(SNOWFLAKE_VARIABLES, environ)
    return snowflake.connector.connect(
        account=config["SNOWFLAKE_ACCOUNT"],
        user=config["SNOWFLAKE_USER"],
        password=config["SNOWFLAKE_PASSWORD"],
        database=config["SNOWFLAKE_DATABASE"],
        warehouse=config["SNOWFLAKE_WAREHOUSE"],
        schema="RAW",
    )


def normalize_ticker(value: object) -> str:
    if value is None:
        raise PipelineError("NORMALIZATION_ERROR", "Ticker is missing or invalid.")
    ticker = str(value).strip().upper()
    if not TICKER_PATTERN.fullmatch(ticker):
        raise PipelineError("NORMALIZATION_ERROR", "Ticker is missing or invalid.")
    return ticker


def new_temp_table_name(kind: str) -> str:
    normalized_kind = kind.upper()
    if normalized_kind not in {"STOCK", "MACRO"}:
        raise ValueError("Unsupported temporary table kind")
    name = f"FF_{normalized_kind}_{uuid4().hex.upper()}"
    if not TEMP_TABLE_PATTERN.fullmatch(name):
        raise ValueError("Unsafe temporary table name")
    return name


def validate_temp_table_name(name: str) -> str:
    if not TEMP_TABLE_PATTERN.fullmatch(name):
        raise ValueError("Unsafe temporary table name")
    return name


def freeze_reason_counts(counts: Mapping[str, int]) -> Mapping[str, int]:
    return MappingProxyType(dict(counts))


def status_for_counts(
    rows_valid: int, rows_inserted: int, rows_updated: int, rows_dropped: int
) -> str:
    if rows_valid == 0:
        return "NO_VALID_ROWS"
    if rows_inserted == 0 and rows_updated == 0:
        return "NO_CHANGES"
    return "PARTIAL_SUCCESS" if rows_dropped > 0 else "SUCCESS"


def _canonical_column_name(value: object) -> str:
    return re.sub(r"[\s_]+", "", str(value)).lower()


def _validated_count(value: object) -> int:
    if isinstance(value, bool):
        raise PipelineError("SNOWFLAKE_LOAD_ERROR", "MERGE metrics were invalid.")
    if isinstance(value, Decimal):
        integral = value.is_finite() and value == value.to_integral_value()
    elif isinstance(value, int):
        integral = True
    elif isinstance(value, float):
        integral = math.isfinite(value) and value.is_integer()
    else:
        integral = False
    if not integral or value < 0:
        raise PipelineError("SNOWFLAKE_LOAD_ERROR", "MERGE metrics were invalid.")
    return int(value)


def extract_merge_result(description, row, rows_valid: int) -> MergeResult:
    if not description or row is None or len(description) != len(row):
        raise PipelineError("SNOWFLAKE_LOAD_ERROR", "MERGE metrics were unavailable.")

    columns = {}
    for index, descriptor in enumerate(description):
        if isinstance(descriptor, (tuple, list)) and descriptor:
            raw_name = descriptor[0]
        elif hasattr(descriptor, "name"):
            raw_name = descriptor.name
        else:
            raise PipelineError("SNOWFLAKE_LOAD_ERROR", "MERGE metrics were unavailable.")
        name = _canonical_column_name(raw_name)
        if name in columns:
            raise PipelineError("SNOWFLAKE_LOAD_ERROR", "MERGE metrics were ambiguous.")
        columns[name] = index

    def find_count(kind: str) -> int:
        matches = [
            index
            for name, index in columns.items()
            if kind in name and ("row" in name or name == kind)
        ]
        if len(matches) != 1:
            raise PipelineError("SNOWFLAKE_LOAD_ERROR", "MERGE metrics were unavailable.")
        return _validated_count(row[matches[0]])

    inserted = find_count("inserted")
    updated = find_count("updated")
    unchanged = rows_valid - inserted - updated
    if unchanged < 0:
        raise PipelineError("SNOWFLAKE_LOAD_ERROR", "MERGE metrics were invalid.")
    return MergeResult(inserted, updated, unchanged)


def safe_error_message(message: object, secrets: Sequence[str] = ()) -> str:
    text = str(message or "Pipeline processing failed.")
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    text = re.sub(
        r"(?i)(api[_-]?key|apikey|password|token|secret)\s*[=:]\s*[^\s&,;]+",
        r"\1=[REDACTED]",
        text,
    )
    text = re.sub(r"(?i)([?&](?:api_key|apikey|token)=)[^&\s]+", r"\1[REDACTED]", text)
    text = text.replace("\r", " ").replace("\n", " ")
    return text[:500]


def build_run_result(
    *,
    run_id: str,
    pipeline_name: str,
    entity_name: str,
    started_at: datetime,
    rows_fetched: int = 0,
    rows_valid: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_unchanged: int = 0,
    rows_dropped: int = 0,
    status: str,
    error_code: Optional[str] = None,
    completed_at: Optional[datetime] = None,
) -> PipelineRunResult:
    completed = completed_at or utc_now()
    values = (
        rows_fetched,
        rows_valid,
        rows_inserted,
        rows_updated,
        rows_unchanged,
        rows_dropped,
    )
    if pipeline_name not in VALID_PIPELINES or status not in VALID_STATUSES:
        raise ValueError("Unsupported pipeline result metadata")
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in values):
        raise ValueError("Pipeline result counts must be non-negative integers")
    if status != "FAILED" and rows_valid + rows_dropped != rows_fetched:
        raise ValueError("Pipeline normalization counts are inconsistent")
    if status in {"SUCCESS", "PARTIAL_SUCCESS", "NO_CHANGES"}:
        if rows_inserted + rows_updated + rows_unchanged != rows_valid:
            raise ValueError("Pipeline MERGE counts are inconsistent")
    if status == "NO_VALID_ROWS" and (
        rows_valid != 0 or rows_inserted != 0 or rows_updated != 0 or rows_unchanged != 0
    ):
        raise ValueError("NO_VALID_ROWS cannot contain valid or merged rows")
    return PipelineRunResult(
        run_id=run_id,
        pipeline_name=pipeline_name,
        entity_name=entity_name[:128],
        rows_fetched=rows_fetched,
        rows_valid=rows_valid,
        rows_inserted=rows_inserted,
        rows_updated=rows_updated,
        rows_unchanged=rows_unchanged,
        rows_dropped=rows_dropped,
        status=status,
        error_code=error_code,
        started_at=started_at,
        completed_at=completed,
        duration_ms=elapsed_ms(started_at, completed),
    )


def ensure_audit_table(connection) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(AUDIT_TABLE_SQL)
        connection.commit()
    finally:
        cursor.close()


def insert_audit_row(cursor, result: PipelineRunResult, error_message: Optional[str] = None) -> None:
    cursor.execute(
        AUDIT_INSERT_SQL,
        (
            result.run_id,
            result.started_at,
            result.completed_at,
            result.pipeline_name,
            result.entity_name,
            result.rows_fetched,
            result.rows_valid,
            result.rows_inserted,
            result.rows_updated,
            result.rows_unchanged,
            result.rows_dropped,
            result.status,
            result.duration_ms,
            result.error_code,
            safe_error_message(error_message) if error_message else None,
        ),
    )


def commit_audit_only(connection_factory: Callable[[], object], result: PipelineRunResult) -> None:
    connection = connection_factory()
    cursor = None
    try:
        ensure_audit_table(connection)
        cursor = connection.cursor()
        cursor.execute("BEGIN")
        insert_audit_row(cursor, result)
        connection.commit()
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass
        raise PipelineError("AUDIT_WRITE_ERROR", "Pipeline audit could not be recorded.")
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()


def write_failure_audit(
    connection_factory: Callable[[], object],
    result: PipelineRunResult,
    error_message: str,
    logger: logging.Logger,
) -> bool:
    connection = None
    cursor = None
    try:
        connection = connection_factory()
        ensure_audit_table(connection)
        cursor = connection.cursor()
        cursor.execute("BEGIN")
        insert_audit_row(cursor, result, safe_error_message(error_message))
        connection.commit()
        return True
    except Exception:
        if connection is not None:
            try:
                connection.rollback()
            except Exception:
                pass
        logger.error("Failed audit record could not be written.")
        return False
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
