"""Reliable Alpha Vantage stock ingestion for FinFlow."""

from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path
import sys
from typing import Callable, Mapping, Optional
from uuid import uuid4

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
    normalize_ticker,
    safe_error_message,
    status_for_counts,
    utc_now,
    validate_environment,
    validate_temp_table_name,
    write_failure_audit,
)


LOGGER = logging.getLogger(__name__)
TICKERS = ["AAPL", "MSFT", "GOOGL", "JPM", "TSLA"]
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"
STOCK_COLUMNS = (
    "ticker",
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
)


def normalize_stock_data(df: pd.DataFrame) -> NormalizationResult:
    if not isinstance(df, pd.DataFrame):
        raise PipelineError("NORMALIZATION_ERROR", "Stock payload was not tabular.")
    missing = sorted(set(STOCK_COLUMNS) - set(df.columns))
    if missing:
        raise PipelineError(
            "NORMALIZATION_ERROR", "Missing required stock columns: " + ", ".join(missing)
        )

    result = df.loc[:, STOCK_COLUMNS].copy(deep=True)
    rows_fetched = len(result)
    result["_input_order"] = range(rows_fetched)
    reason = pd.Series(pd.NA, index=result.index, dtype="object")

    ticker_text = result["ticker"].astype("string").str.strip().str.upper()
    valid_ticker = ticker_text.str.fullmatch(r"[A-Z][A-Z0-9.-]{0,9}", na=False)
    reason.loc[~valid_ticker] = "invalid_ticker"
    result["ticker"] = ticker_text

    parsed_date = pd.to_datetime(result["trade_date"], errors="coerce", utc=True)
    invalid_date = parsed_date.isna() & reason.isna()
    reason.loc[invalid_date] = "invalid_trade_date"
    result["trade_date"] = parsed_date.dt.date

    price_columns = ["open_price", "high_price", "low_price", "close_price"]
    for column in price_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    invalid_price = result[price_columns].isna().any(axis=1)
    invalid_price |= ~result[price_columns].apply(lambda col: col.map(math.isfinite)).all(axis=1)
    invalid_price |= result[price_columns].le(0).any(axis=1)
    reason.loc[invalid_price & reason.isna()] = "invalid_ohlc"

    inconsistent = (
        (result["high_price"] < result["low_price"])
        | (result["high_price"] < result["open_price"])
        | (result["high_price"] < result["close_price"])
        | (result["low_price"] > result["open_price"])
        | (result["low_price"] > result["close_price"])
    )
    reason.loc[inconsistent & reason.isna()] = "inconsistent_ohlc"

    numeric_volume = pd.to_numeric(result["volume"], errors="coerce")
    finite_volume = numeric_volume.map(lambda value: pd.notna(value) and math.isfinite(float(value)))
    invalid_volume = ~finite_volume | numeric_volume.le(0) | numeric_volume.mod(1).ne(0)
    reason.loc[invalid_volume & reason.isna()] = "invalid_volume"
    result["volume"] = numeric_volume

    dropped_by_reason = reason.dropna().value_counts().to_dict()
    valid = result.loc[reason.isna()].copy()
    duplicate_mask = valid.duplicated(["ticker", "trade_date"], keep="last")
    duplicate_count = int(duplicate_mask.sum())
    if duplicate_count:
        dropped_by_reason["duplicate_key"] = duplicate_count
    valid = valid.loc[~duplicate_mask].sort_values("_input_order", kind="stable")

    for column in price_columns:
        valid[column] = valid[column].round(4)
    if not valid.empty:
        valid["volume"] = valid["volume"].map(int).astype(object)
    valid = valid.loc[:, STOCK_COLUMNS].reset_index(drop=True)

    rows_valid = len(valid)
    return NormalizationResult(
        dataframe=valid,
        rows_fetched=rows_fetched,
        rows_valid=rows_valid,
        rows_dropped=rows_fetched - rows_valid,
        dropped_by_reason=freeze_reason_counts(dropped_by_reason),
    )


def fetch_stock_data(
    ticker: str,
    api_key: str,
    request_get: Callable = requests.get,
    timeout: int = 30,
) -> pd.DataFrame:
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": "compact",
        "apikey": api_key,
    }
    try:
        response = request_get(ALPHA_VANTAGE_URL, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except requests.Timeout as exc:
        raise PipelineError("API_TIMEOUT", "Alpha Vantage request timed out.") from exc
    except (requests.RequestException, ValueError) as exc:
        raise PipelineError("API_RESPONSE_INVALID", "Alpha Vantage response was invalid.") from exc

    if not isinstance(payload, dict):
        raise PipelineError("API_RESPONSE_INVALID", "Alpha Vantage response was invalid.")
    if "Note" in payload or "Information" in payload:
        raise PipelineError("API_RATE_LIMITED", "Alpha Vantage request was rate limited.")
    if "Error Message" in payload:
        raise PipelineError("API_DATA_UNAVAILABLE", "Alpha Vantage data was unavailable.")

    time_series = payload.get("Time Series (Daily)")
    if not isinstance(time_series, dict) or not time_series:
        raise PipelineError("API_DATA_UNAVAILABLE", "Alpha Vantage time series was unavailable.")

    rows = []
    for trade_date, values in time_series.items():
        values = values if isinstance(values, dict) else {}
        rows.append(
            {
                "ticker": ticker,
                "trade_date": trade_date,
                "open_price": values.get("1. open"),
                "high_price": values.get("2. high"),
                "low_price": values.get("3. low"),
                "close_price": values.get("4. close"),
                "volume": values.get("5. volume"),
            }
        )
    return pd.DataFrame(rows, columns=STOCK_COLUMNS)


def _merge_stock_data(
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
        temp_table = validate_temp_table_name(new_temp_table_name("STOCK"))
        cursor.execute(
            f"""CREATE TEMPORARY TABLE {temp_table} (
                ticker VARCHAR(10), trade_date DATE, open_price NUMBER(38,4),
                high_price NUMBER(38,4), low_price NUMBER(38,4),
                close_price NUMBER(38,4), volume NUMBER(38,0), loaded_at TIMESTAMP_TZ
            )"""
        )
        cursor.execute("BEGIN")
        transaction_started = True
        insert_sql = f"""INSERT INTO {temp_table} (
            ticker, trade_date, open_price, high_price, low_price, close_price, volume, loaded_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        rows = [
            (
                row.ticker,
                row.trade_date,
                row.open_price,
                row.high_price,
                row.low_price,
                row.close_price,
                int(row.volume),
                base_result["started_at"],
            )
            for row in normalization.dataframe.itertuples(index=False)
        ]
        cursor.executemany(insert_sql, rows)
        cursor.execute(
            f"""MERGE INTO FINFLOW_DB.RAW.raw_stock_prices AS target
            USING {temp_table} AS source
              ON target.ticker = source.ticker
             AND target.trade_date = source.trade_date
            WHEN MATCHED AND (
                target.open_price IS DISTINCT FROM source.open_price OR
                target.high_price IS DISTINCT FROM source.high_price OR
                target.low_price IS DISTINCT FROM source.low_price OR
                target.close_price IS DISTINCT FROM source.close_price OR
                target.volume IS DISTINCT FROM source.volume
            ) THEN UPDATE SET
                open_price = source.open_price,
                high_price = source.high_price,
                low_price = source.low_price,
                close_price = source.close_price,
                volume = source.volume,
                loaded_at = source.loaded_at
            WHEN NOT MATCHED THEN INSERT (
                ticker, trade_date, open_price, high_price, low_price, close_price, volume, loaded_at
            ) VALUES (
                source.ticker, source.trade_date, source.open_price, source.high_price,
                source.low_price, source.close_price, source.volume, source.loaded_at
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
        raise PipelineError("SNOWFLAKE_LOAD_ERROR", "Stock data could not be loaded.") from exc
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
    ticker: str,
    *,
    connection_factory: Optional[Callable[[], object]] = None,
    request_get: Callable = requests.get,
    environ: Optional[Mapping[str, str]] = None,
) -> PipelineRunResult:
    run_id = str(uuid4())
    started_at = utc_now()
    normalized_ticker = str(ticker).strip().upper()[:128]
    base_result = {
        "run_id": run_id,
        "pipeline_name": "STOCK_INGESTION",
        "entity_name": normalized_ticker,
        "started_at": started_at,
    }
    factory = connection_factory or (lambda: create_snowflake_connection(environ))
    normalization = None
    secrets = []
    try:
        normalized_ticker = normalize_ticker(ticker)
        base_result["entity_name"] = normalized_ticker
        required = ("ALPHA_VANTAGE_KEY",)
        if connection_factory is None:
            required = SNOWFLAKE_VARIABLES + required
        api_config = validate_environment(required, environ)
        secrets.append(api_config["ALPHA_VANTAGE_KEY"])
        raw_data = fetch_stock_data(
            normalized_ticker, api_config["ALPHA_VANTAGE_KEY"], request_get=request_get
        )
        normalization = normalize_stock_data(raw_data)
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
        return _merge_stock_data(normalization, base_result, factory)
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
        write_failure_audit(factory, failed, "Stock processing failed.", LOGGER)
        return failed


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Ingest FinFlow stock prices.")
    parser.add_argument("ticker", nargs="?", help="Optional single ticker symbol")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    load_dotenv()
    configure_logging()
    args = parse_args(argv)
    try:
        tickers = [normalize_ticker(args.ticker)] if args.ticker else TICKERS
    except PipelineError as exc:
        LOGGER.error("%s", exc.public_message)
        return 1

    failed = False
    for ticker in tickers:
        LOGGER.info("Processing stock entity %s", ticker)
        result = fetch_and_load(ticker)
        LOGGER.info("Stock entity %s completed with status %s", ticker, result.status)
        failed = failed or result.status == "FAILED"
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
