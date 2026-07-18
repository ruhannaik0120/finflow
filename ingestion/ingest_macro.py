"""Reliable FRED macroeconomic ingestion for FinFlow."""

from __future__ import annotations

import logging
import math
import os
from pathlib import Path
import sys
from typing import Callable, Mapping, Optional
from uuid import uuid4
import warnings

import pandas as pd
import requests
from dotenv import load_dotenv

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.pipeline_utils import (  # noqa: E402
    NormalizationResult,
    PipelineError,
    PipelineRunResult,
    SNOWFLAKE_VARIABLES,
    build_run_result,
    commit_audit_only,
    configure_logging,
    create_snowflake_connection,
    ensure_audit_table,
    extract_merge_result,
    freeze_reason_counts,
    insert_audit_row,
    new_temp_table_name,
    safe_error_message,
    status_for_counts,
    utc_now,
    validate_environment,
    validate_temp_table_name,
    write_failure_audit,
)


LOGGER = logging.getLogger(__name__)
FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
INDICATORS = {
    "FEDFUNDS": "Federal Funds Rate",
    "CPIAUCSL": "Consumer Price Index",
    "GDP": "Gross Domestic Product",
    "UNRATE": "Unemployment Rate",
    "DGS10": "10 Year Treasury Rate",
}
MACRO_COLUMNS = ("series_id", "indicator_name", "indicator_date", "value")


def get_fred_api_key(environ: Optional[Mapping[str, str]] = None) -> str:
    source = os.environ if environ is None else environ
    if source.get("FRED_API_KEY"):
        return source["FRED_API_KEY"]
    if source.get("FRED_KEY"):
        warnings.warn(
            "FRED_KEY is deprecated; use FRED_API_KEY.", DeprecationWarning, stacklevel=2
        )
        return source["FRED_KEY"]
    raise PipelineError(
        "CONFIGURATION_ERROR", "Missing required environment variables: FRED_API_KEY"
    )


def normalize_macro_data(df: pd.DataFrame) -> NormalizationResult:
    if not isinstance(df, pd.DataFrame):
        raise PipelineError("NORMALIZATION_ERROR", "Macro payload was not tabular.")
    missing = sorted(set(MACRO_COLUMNS) - set(df.columns))
    if missing:
        raise PipelineError(
            "NORMALIZATION_ERROR", "Missing required macro columns: " + ", ".join(missing)
        )

    result = df.loc[:, MACRO_COLUMNS].copy(deep=True)
    rows_fetched = len(result)
    result["_input_order"] = range(rows_fetched)
    reason = pd.Series(pd.NA, index=result.index, dtype="object")

    series = result["series_id"].astype("string").str.strip().str.upper()
    valid_series = series.str.fullmatch(r"[A-Z0-9_.-]{1,64}", na=False)
    reason.loc[~valid_series] = "invalid_series_id"
    result["series_id"] = series

    names = result["indicator_name"].astype("string").str.strip()
    valid_names = names.str.len().between(1, 128, inclusive="both").fillna(False)
    reason.loc[~valid_names & reason.isna()] = "invalid_indicator_name"
    result["indicator_name"] = names

    parsed_date = pd.to_datetime(result["indicator_date"], errors="coerce", utc=True)
    reason.loc[parsed_date.isna() & reason.isna()] = "invalid_indicator_date"
    result["indicator_date"] = parsed_date.dt.date

    numeric_value = pd.to_numeric(result["value"].replace(".", pd.NA), errors="coerce")
    valid_value = numeric_value.map(
        lambda value: pd.notna(value) and math.isfinite(float(value))
    )
    reason.loc[~valid_value & reason.isna()] = "invalid_value"
    result["value"] = numeric_value

    dropped_by_reason = reason.dropna().value_counts().to_dict()
    valid = result.loc[reason.isna()].copy()
    duplicate_mask = valid.duplicated(["series_id", "indicator_date"], keep="last")
    duplicate_count = int(duplicate_mask.sum())
    if duplicate_count:
        dropped_by_reason["duplicate_key"] = duplicate_count
    valid = valid.loc[~duplicate_mask].sort_values("_input_order", kind="stable")
    valid["value"] = valid["value"].round(6)
    valid = valid.loc[:, MACRO_COLUMNS].reset_index(drop=True)
    rows_valid = len(valid)
    return NormalizationResult(
        dataframe=valid,
        rows_fetched=rows_fetched,
        rows_valid=rows_valid,
        rows_dropped=rows_fetched - rows_valid,
        dropped_by_reason=freeze_reason_counts(dropped_by_reason),
    )


def fetch_macro_data(
    series_id: str,
    indicator_name: str,
    api_key: str,
    request_get: Callable = requests.get,
    timeout: int = 30,
) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "limit": 100,
        "sort_order": "desc",
    }
    try:
        response = request_get(FRED_URL, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except requests.Timeout as exc:
        raise PipelineError("API_TIMEOUT", "FRED request timed out.") from exc
    except (requests.RequestException, ValueError) as exc:
        raise PipelineError("API_RESPONSE_INVALID", "FRED response was invalid.") from exc

    if not isinstance(payload, dict):
        raise PipelineError("API_RESPONSE_INVALID", "FRED response was invalid.")
    if "error_code" in payload or "error_message" in payload:
        raise PipelineError("API_DATA_UNAVAILABLE", "FRED data was unavailable.")
    observations = payload.get("observations")
    if not isinstance(observations, list) or not observations:
        raise PipelineError("API_DATA_UNAVAILABLE", "FRED observations were unavailable.")

    rows = [
        {
            "series_id": series_id,
            "indicator_name": indicator_name,
            "indicator_date": observation.get("date") if isinstance(observation, dict) else None,
            "value": observation.get("value") if isinstance(observation, dict) else None,
        }
        for observation in observations
    ]
    return pd.DataFrame(rows, columns=MACRO_COLUMNS)


def _merge_macro_data(
    normalization: NormalizationResult,
    base_result: dict,
    connection_factory: Callable[[], object],
) -> PipelineRunResult:
    connection = None
    cursor = None
    transaction_started = False
    try:
        connection = connection_factory()
        try:
            ensure_audit_table(connection)
        except Exception as exc:
            raise PipelineError("AUDIT_WRITE_ERROR", "Pipeline audit could not be prepared.") from exc
        cursor = connection.cursor()
        temp_table = validate_temp_table_name(new_temp_table_name("MACRO"))
        cursor.execute(
            f"""CREATE TEMPORARY TABLE {temp_table} (
                series_id VARCHAR(64), indicator_name VARCHAR(128), indicator_date DATE,
                value NUMBER(38,6), loaded_at TIMESTAMP_TZ
            )"""
        )
        cursor.execute("BEGIN")
        transaction_started = True
        insert_sql = f"""INSERT INTO {temp_table} (
            series_id, indicator_name, indicator_date, value, loaded_at
        ) VALUES (%s, %s, %s, %s, %s)"""
        rows = [
            (
                row.series_id,
                row.indicator_name,
                row.indicator_date,
                row.value,
                base_result["started_at"],
            )
            for row in normalization.dataframe.itertuples(index=False)
        ]
        cursor.executemany(insert_sql, rows)
        cursor.execute(
            f"""MERGE INTO FINFLOW_DB.RAW.raw_macro_indicators AS target
            USING {temp_table} AS source
              ON target.indicator_name = source.indicator_name
             AND target.indicator_date = source.indicator_date
            WHEN MATCHED AND target.value IS DISTINCT FROM source.value THEN UPDATE SET
                value = source.value,
                loaded_at = source.loaded_at
            WHEN NOT MATCHED THEN INSERT (
                indicator_name, indicator_date, value, loaded_at
            ) VALUES (
                source.indicator_name, source.indicator_date, source.value, source.loaded_at
            )"""
        )
        merge = extract_merge_result(cursor.description, cursor.fetchone(), normalization.rows_valid)
        status = status_for_counts(
            normalization.rows_valid,
            merge.rows_inserted,
            merge.rows_updated,
            normalization.rows_dropped,
        )
        result = build_run_result(
            **base_result,
            rows_fetched=normalization.rows_fetched,
            rows_valid=normalization.rows_valid,
            rows_inserted=merge.rows_inserted,
            rows_updated=merge.rows_updated,
            rows_unchanged=merge.rows_unchanged,
            rows_dropped=normalization.rows_dropped,
            status=status,
        )
        try:
            insert_audit_row(cursor, result)
        except Exception as exc:
            raise PipelineError("AUDIT_WRITE_ERROR", "Pipeline audit could not be recorded.") from exc
        connection.commit()
        transaction_started = False
        return result
    except PipelineError:
        if transaction_started and connection is not None:
            try:
                connection.rollback()
            except Exception:
                pass
        raise
    except Exception as exc:
        if transaction_started and connection is not None:
            try:
                connection.rollback()
            except Exception:
                pass
        raise PipelineError("SNOWFLAKE_LOAD_ERROR", "Macro data could not be loaded.") from exc
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


def fetch_and_load(
    series_id: str,
    indicator_name: str,
    *,
    connection_factory: Optional[Callable[[], object]] = None,
    request_get: Callable = requests.get,
    environ: Optional[Mapping[str, str]] = None,
) -> PipelineRunResult:
    run_id = str(uuid4())
    started_at = utc_now()
    base_result = {
        "run_id": run_id,
        "pipeline_name": "MACRO_INGESTION",
        "entity_name": str(indicator_name).strip()[:128],
        "started_at": started_at,
    }
    factory = connection_factory or (lambda: create_snowflake_connection(environ))
    normalization = None
    secrets = []
    try:
        if connection_factory is None:
            validate_environment(SNOWFLAKE_VARIABLES, environ)
        api_key = get_fred_api_key(environ)
        secrets.append(api_key)
        raw_data = fetch_macro_data(
            series_id, indicator_name, api_key, request_get=request_get
        )
        normalization = normalize_macro_data(raw_data)
        if normalization.rows_valid == 0:
            result = build_run_result(
                **base_result,
                rows_fetched=normalization.rows_fetched,
                rows_valid=0,
                rows_dropped=normalization.rows_dropped,
                status="NO_VALID_ROWS",
            )
            commit_audit_only(factory, result)
            return result
        return _merge_macro_data(normalization, base_result, factory)
    except PipelineError as exc:
        failed = build_run_result(
            **base_result,
            rows_fetched=normalization.rows_fetched if normalization else 0,
            rows_valid=normalization.rows_valid if normalization else 0,
            rows_dropped=normalization.rows_dropped if normalization else 0,
            status="FAILED",
            error_code=exc.code,
        )
        write_failure_audit(factory, failed, safe_error_message(exc.public_message, secrets), LOGGER)
        return failed
    except Exception:
        failed = build_run_result(
            **base_result,
            rows_fetched=normalization.rows_fetched if normalization else 0,
            rows_valid=normalization.rows_valid if normalization else 0,
            rows_dropped=normalization.rows_dropped if normalization else 0,
            status="FAILED",
            error_code="NORMALIZATION_ERROR",
        )
        write_failure_audit(factory, failed, "Macro processing failed.", LOGGER)
        return failed


def main(argv=None) -> int:
    del argv
    load_dotenv()
    configure_logging()
    failed = False
    for series_id, indicator_name in INDICATORS.items():
        LOGGER.info("Processing macro entity %s", indicator_name)
        result = fetch_and_load(series_id, indicator_name)
        LOGGER.info("Macro entity %s completed with status %s", indicator_name, result.status)
        failed = failed or result.status == "FAILED"
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
