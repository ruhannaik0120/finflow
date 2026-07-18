from datetime import datetime, timezone
from decimal import Decimal
import unittest
from unittest.mock import Mock

from ingestion.pipeline_utils import (
    AUDIT_INSERT_SQL,
    AUDIT_TABLE_SQL,
    PipelineError,
    build_run_result,
    extract_merge_result,
    insert_audit_row,
    new_temp_table_name,
    normalize_ticker,
    status_for_counts,
    validate_environment,
    validate_temp_table_name,
)


class MergeMetricTests(unittest.TestCase):
    def test_inserted_and_updated_counts_extracted(self):
        result = extract_merge_result([("number of rows inserted",), ("number of rows updated",)], (3, 2), 7)
        self.assertEqual((result.rows_inserted, result.rows_updated, result.rows_unchanged), (3, 2, 2))

    def test_shuffled_column_order(self):
        result = extract_merge_result([("ROWS_UPDATED",), ("ROWS_INSERTED",)], (2, 1), 5)
        self.assertEqual((result.rows_inserted, result.rows_updated, result.rows_unchanged), (1, 2, 2))

    def test_decimal_integral_counts_accepted(self):
        result = extract_merge_result([("rows inserted",), ("rows updated",)], (Decimal("2"), Decimal("0")), 2)
        self.assertEqual(result.rows_inserted, 2)

    def test_duplicate_columns_rejected(self):
        with self.assertRaises(PipelineError):
            extract_merge_result([("rows inserted",), ("ROWS_INSERTED",)], (1, 1), 2)

    def test_negative_count_rejected(self):
        with self.assertRaises(PipelineError):
            extract_merge_result([("rows inserted",), ("rows updated",)], (-1, 0), 1)

    def test_fractional_count_rejected(self):
        with self.assertRaises(PipelineError):
            extract_merge_result([("rows inserted",), ("rows updated",)], (1.5, 0), 2)
        with self.assertRaises(PipelineError):
            extract_merge_result(
                [("rows inserted",), ("rows updated",)], (Decimal("Infinity"), 0), 2
            )

    def test_boolean_count_rejected(self):
        with self.assertRaises(PipelineError):
            extract_merge_result([("rows inserted",), ("rows updated",)], (True, 0), 1)

    def test_negative_unchanged_rejected(self):
        with self.assertRaises(PipelineError):
            extract_merge_result([("rows inserted",), ("rows updated",)], (2, 1), 2)


class AuditAndValidationTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def result(self, status="SUCCESS", **counts):
        defaults = dict(rows_fetched=1, rows_valid=1, rows_inserted=1, rows_updated=0, rows_unchanged=0, rows_dropped=0)
        defaults.update(counts)
        return build_run_result(
            run_id="00000000-0000-0000-0000-000000000001",
            pipeline_name="STOCK_INGESTION",
            entity_name="AAPL",
            started_at=self.now,
            completed_at=self.now,
            status=status,
            **defaults,
        )

    def test_audit_table_has_required_append_only_columns(self):
        normalized = " ".join(AUDIT_TABLE_SQL.split()).upper()
        self.assertIn("CREATE TABLE IF NOT EXISTS FINFLOW_DB.RAW.PIPELINE_LOGS", normalized)
        self.assertIn("RUN_ID VARCHAR(36) NOT NULL", normalized)
        self.assertNotIn("DROP TABLE", normalized)

    def test_audit_insert_uses_bound_parameters(self):
        cursor = Mock()
        result = self.result()
        insert_audit_row(cursor, result)
        sql, params = cursor.execute.call_args.args
        self.assertEqual(sql, AUDIT_INSERT_SQL)
        self.assertEqual(len(params), 15)
        self.assertNotIn(result.run_id, sql)

    def test_all_nonfailed_status_rules(self):
        self.assertEqual(status_for_counts(0, 0, 0, 2), "NO_VALID_ROWS")
        self.assertEqual(status_for_counts(2, 0, 0, 0), "NO_CHANGES")
        self.assertEqual(status_for_counts(2, 1, 0, 1), "PARTIAL_SUCCESS")
        self.assertEqual(status_for_counts(2, 1, 0, 0), "SUCCESS")

    def test_failed_result_supported(self):
        result = self.result(status="FAILED")
        self.assertEqual(result.status, "FAILED")

    def test_negative_metrics_rejected(self):
        with self.assertRaises(ValueError):
            self.result(rows_inserted=-1)

    def test_inconsistent_normalization_metrics_rejected(self):
        with self.assertRaises(ValueError):
            self.result(rows_fetched=2)
        with self.assertRaises(ValueError):
            self.result(rows_inserted=0)

    def test_temp_table_name_is_safe_and_unique(self):
        first = new_temp_table_name("STOCK")
        second = new_temp_table_name("STOCK")
        self.assertNotEqual(first, second)
        self.assertEqual(validate_temp_table_name(first), first)

    def test_unsafe_temp_table_name_rejected(self):
        with self.assertRaises(ValueError):
            validate_temp_table_name("FF_STOCK_X; DROP TABLE users")

    def test_ticker_validation_and_uppercase(self):
        self.assertEqual(normalize_ticker(" brk.b "), "BRK.B")
        with self.assertRaises(PipelineError):
            normalize_ticker("bad ticker!")
        with self.assertRaises(PipelineError):
            normalize_ticker("AA PL")

    def test_missing_configuration_names_only_missing_variables(self):
        with self.assertRaises(PipelineError) as caught:
            validate_environment(("ONE", "TWO"), {"ONE": "secret"})
        self.assertIn("TWO", caught.exception.public_message)
        self.assertNotIn("secret", caught.exception.public_message)


if __name__ == "__main__":
    unittest.main()
