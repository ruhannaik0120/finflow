from datetime import datetime, timezone
import importlib
import unittest
from unittest.mock import Mock, patch

import pandas as pd

import ingestion.ingest_macro as macro
import ingestion.ingest_stocks as stocks
from ingestion.pipeline_utils import NormalizationResult, PipelineError, build_run_result, freeze_reason_counts
import run_pipeline


class FakeCursor:
    def __init__(self, merge_row=(1, 0), fail_merge=False, fail_audit=False):
        self.description = [("rows inserted",), ("rows updated",)]
        self.merge_row = merge_row
        self.fail_merge = fail_merge
        self.fail_audit = fail_audit
        self.executed = []
        self.executemany_calls = []
        self.closed = False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if self.fail_merge and "MERGE INTO" in sql:
            raise RuntimeError("database secret should not escape")
        if self.fail_audit and "INSERT INTO FINFLOW_DB.RAW.pipeline_logs" in sql:
            raise RuntimeError("audit database secret should not escape")
        return self

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, rows))
        return self

    def fetchone(self):
        return self.merge_row

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, merge_row=(1, 0), fail_merge=False, fail_audit=False):
        self.audit_cursor = FakeCursor()
        self.data_cursor = FakeCursor(merge_row, fail_merge, fail_audit)
        self.cursors = [self.audit_cursor, self.data_cursor]
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self):
        return self.cursors.pop(0)

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.closed = True


def stock_normalization():
    frame = pd.DataFrame([{
        "ticker": "AAPL", "trade_date": datetime(2026, 1, 2).date(),
        "open_price": 1.0, "high_price": 2.0, "low_price": 1.0,
        "close_price": 2.0, "volume": 10,
    }])
    return NormalizationResult(frame, 1, 1, 0, freeze_reason_counts({}))


def macro_normalization():
    frame = pd.DataFrame([{
        "series_id": "GDP", "indicator_name": "Gross Domestic Product",
        "indicator_date": datetime(2026, 1, 1).date(), "value": 4.2,
    }])
    return NormalizationResult(frame, 1, 1, 0, freeze_reason_counts({}))


def base(pipeline, entity):
    return {
        "run_id": "00000000-0000-0000-0000-000000000001",
        "pipeline_name": pipeline,
        "entity_name": entity,
        "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }


class MergeTransactionTests(unittest.TestCase):
    def test_stock_merge_uses_bound_rows_and_atomic_audit(self):
        connection = FakeConnection()
        result = stocks._merge_stock_data(stock_normalization(), base("STOCK_INGESTION", "AAPL"), lambda: connection)
        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(connection.commit_count, 2)
        self.assertEqual(connection.rollback_count, 0)
        sql, rows = connection.data_cursor.executemany_calls[0]
        self.assertIn("VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", sql)
        self.assertNotIn("AAPL", sql)
        self.assertEqual(rows[0][0], "AAPL")
        merge_sql = next(sql for sql, _ in connection.data_cursor.executed if "MERGE INTO" in sql)
        self.assertIn("target.ticker = source.ticker", merge_sql)
        self.assertIn("IS DISTINCT FROM", merge_sql)
        self.assertIn("WHEN NOT MATCHED THEN INSERT", merge_sql)
        audit_inserts = [sql for sql, _ in connection.data_cursor.executed if "pipeline_logs" in sql]
        self.assertEqual(len(audit_inserts), 1)
        self.assertTrue(connection.closed)
        self.assertTrue(connection.data_cursor.closed)

    def test_stock_identical_rerun_reports_unchanged(self):
        connection = FakeConnection(merge_row=(0, 0))
        result = stocks._merge_stock_data(stock_normalization(), base("STOCK_INGESTION", "AAPL"), lambda: connection)
        self.assertEqual((result.status, result.rows_unchanged), ("NO_CHANGES", 1))

    def test_macro_merge_has_business_key_and_explicit_columns(self):
        connection = FakeConnection()
        macro._merge_macro_data(macro_normalization(), base("MACRO_INGESTION", "GDP"), lambda: connection)
        merge_sql = next(sql for sql, _ in connection.data_cursor.executed if "MERGE INTO" in sql)
        self.assertIn("target.indicator_name = source.indicator_name", merge_sql)
        self.assertIn("target.indicator_date = source.indicator_date", merge_sql)
        self.assertIn("value = source.value", merge_sql)
        self.assertIn("indicator_name, indicator_date, value, loaded_at", merge_sql)

    def test_load_failure_rolls_back_and_cleans_up(self):
        connection = FakeConnection(fail_merge=True)
        with self.assertRaises(PipelineError) as caught:
            stocks._merge_stock_data(stock_normalization(), base("STOCK_INGESTION", "AAPL"), lambda: connection)
        self.assertEqual(caught.exception.code, "SNOWFLAKE_LOAD_ERROR")
        self.assertEqual(connection.rollback_count, 1)
        self.assertTrue(connection.closed)

    def test_success_audit_failure_rolls_back_with_fixed_code(self):
        connection = FakeConnection(fail_audit=True)
        with self.assertRaises(PipelineError) as caught:
            stocks._merge_stock_data(stock_normalization(), base("STOCK_INGESTION", "AAPL"), lambda: connection)
        self.assertEqual(caught.exception.code, "AUDIT_WRITE_ERROR")
        self.assertEqual(connection.rollback_count, 1)


class FailureIsolationTests(unittest.TestCase):
    @staticmethod
    def result(pipeline, entity, status):
        return build_run_result(
            run_id=entity.ljust(36, "0")[:36], pipeline_name=pipeline, entity_name=entity,
            started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            status=status, error_code="API_TIMEOUT" if status == "FAILED" else None,
        )

    def test_first_stock_failure_does_not_stop_later_stocks(self):
        results = [self.result("STOCK_INGESTION", "AAPL", "FAILED")] + [
            self.result("STOCK_INGESTION", ticker, "NO_VALID_ROWS") for ticker in stocks.TICKERS[1:]
        ]
        with patch.object(stocks, "fetch_and_load", side_effect=results) as mocked, patch.object(stocks, "load_dotenv"):
            self.assertEqual(stocks.main([]), 1)
        self.assertEqual(mocked.call_count, len(stocks.TICKERS))

    def test_first_macro_failure_does_not_stop_later_indicators(self):
        names = list(macro.INDICATORS.values())
        results = [self.result("MACRO_INGESTION", names[0], "FAILED")] + [
            self.result("MACRO_INGESTION", name, "NO_VALID_ROWS") for name in names[1:]
        ]
        with patch.object(macro, "fetch_and_load", side_effect=results) as mocked, patch.object(macro, "load_dotenv"):
            self.assertEqual(macro.main([]), 1)
        self.assertEqual(mocked.call_count, len(names))

    def test_each_fetch_attempt_gets_unique_run_id(self):
        def bad_request(*args, **kwargs):
            raise __import__("requests").Timeout()
        factory = Mock(side_effect=PipelineError("CONFIGURATION_ERROR", "No database"))
        env = {"ALPHA_VANTAGE_KEY": "secret"}
        first = stocks.fetch_and_load("AAPL", request_get=bad_request, connection_factory=factory, environ=env)
        second = stocks.fetch_and_load("AAPL", request_get=bad_request, connection_factory=factory, environ=env)
        self.assertNotEqual(first.run_id, second.run_id)


class RunnerTests(unittest.TestCase):
    def test_default_stage_order(self):
        with patch.object(run_pipeline, "run_stage", return_value=0) as stage:
            self.assertEqual(run_pipeline.run_pipeline(), 0)
        self.assertEqual([call.args[0] for call in stage.call_args_list], ["stock ingestion", "macro ingestion", "dbt build"])
        self.assertEqual(stage.call_args_list[0].args[1][0], run_pipeline.sys.executable)
        self.assertEqual(stage.call_args_list[2].args[1][-1], "build")

    def test_dynamic_ticker_skips_macro(self):
        with patch.object(run_pipeline, "run_stage", return_value=0) as stage:
            run_pipeline.run_pipeline("NFLX")
        self.assertEqual([call.args[0] for call in stage.call_args_list], ["stock ingestion", "dbt build"])
        self.assertIn("NFLX", stage.call_args_list[0].args[1])

    def test_ingestion_failure_prevents_dbt(self):
        with patch.object(run_pipeline, "run_stage", return_value=7) as stage:
            self.assertEqual(run_pipeline.run_pipeline(), 7)
        self.assertEqual(stage.call_count, 1)

    def test_timeout_is_bounded_and_returns_nonzero(self):
        with patch.object(run_pipeline.subprocess, "run", side_effect=run_pipeline.subprocess.TimeoutExpired("x", 1)) as process:
            code = run_pipeline.run_stage("test", ["command"], cwd=run_pipeline.REPOSITORY_ROOT, timeout=1)
        self.assertEqual(code, 124)
        self.assertEqual(process.call_args.kwargs["shell"], False)

    def test_runner_import_has_no_subprocess_side_effect(self):
        with patch("subprocess.run") as process:
            importlib.reload(run_pipeline)
        process.assert_not_called()

    def test_ingestion_imports_have_no_external_side_effects(self):
        with patch("snowflake.connector.connect") as connect, patch("requests.get") as request:
            importlib.reload(stocks)
            importlib.reload(macro)
        connect.assert_not_called()
        request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
