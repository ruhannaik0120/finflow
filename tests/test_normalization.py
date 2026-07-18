import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from ingestion.ingest_macro import normalize_macro_data
from ingestion.ingest_stocks import normalize_stock_data
from ingestion.pipeline_utils import PipelineError


def stock_frame(**overrides):
    row = {
        "ticker": "AAPL",
        "trade_date": "2026-01-02",
        "open_price": 100.123456,
        "high_price": 110.123456,
        "low_price": 90.123456,
        "close_price": 105.123456,
        "volume": 1000,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def macro_frame(**overrides):
    row = {
        "series_id": "GDP",
        "indicator_name": "Gross Domestic Product",
        "indicator_date": "2026-01-01",
        "value": 123.123456789,
    }
    row.update(overrides)
    return pd.DataFrame([row])


class StockNormalizationTests(unittest.TestCase):
    def test_caller_dataframe_unchanged(self):
        frame = stock_frame()
        original = frame.copy(deep=True)
        normalize_stock_data(frame)
        assert_frame_equal(frame, original)

    def test_required_columns_validated(self):
        with self.assertRaisesRegex(PipelineError, "volume"):
            normalize_stock_data(stock_frame().drop(columns=["volume"]))

    def test_null_close_dropped(self):
        self.assertEqual(normalize_stock_data(stock_frame(close_price=None)).rows_valid, 0)

    def test_zero_close_dropped(self):
        self.assertEqual(normalize_stock_data(stock_frame(close_price=0)).rows_valid, 0)

    def test_non_positive_ohlc_dropped(self):
        self.assertEqual(normalize_stock_data(stock_frame(open_price=-1)).rows_valid, 0)

    def test_inconsistent_ohlc_dropped(self):
        self.assertEqual(normalize_stock_data(stock_frame(high_price=99)).rows_valid, 0)

    def test_invalid_volumes_dropped(self):
        frames = [stock_frame(volume=value) for value in (None, 0, -1, 1.5)]
        combined = pd.concat(frames, ignore_index=True)
        result = normalize_stock_data(combined)
        self.assertEqual((result.rows_valid, result.rows_dropped), (0, 4))

    def test_volume_converted_to_python_int(self):
        value = normalize_stock_data(stock_frame(volume="1000")).dataframe.iloc[0]["volume"]
        self.assertIs(type(value), int)

    def test_prices_rounded_to_four_places(self):
        result = normalize_stock_data(stock_frame()).dataframe.iloc[0]
        self.assertEqual(result["open_price"], 100.1235)
        self.assertEqual(result["close_price"], 105.1235)

    def test_invalid_date_dropped(self):
        self.assertEqual(normalize_stock_data(stock_frame(trade_date="not-a-date")).rows_valid, 0)

    def test_invalid_ticker_dropped(self):
        self.assertEqual(normalize_stock_data(stock_frame(ticker="AAPL;DROP")).rows_valid, 0)

    def test_duplicate_keeps_final_input_occurrence(self):
        frame = pd.concat([stock_frame(close_price=101), stock_frame(close_price=102)], ignore_index=True)
        result = normalize_stock_data(frame)
        self.assertEqual(result.dataframe.iloc[0]["close_price"], 102)
        self.assertEqual(result.dropped_by_reason["duplicate_key"], 1)

    def test_metrics_are_accurate(self):
        frame = pd.concat([stock_frame(), stock_frame(volume=0), stock_frame(ticker="bad ticker!")], ignore_index=True)
        result = normalize_stock_data(frame)
        self.assertEqual((result.rows_fetched, result.rows_valid, result.rows_dropped), (3, 1, 2))

    def test_all_invalid_returns_safe_empty_frame(self):
        result = normalize_stock_data(stock_frame(volume=0))
        self.assertTrue(result.dataframe.empty)
        self.assertEqual(list(result.dataframe.columns), list(stock_frame().columns))


class MacroNormalizationTests(unittest.TestCase):
    def test_caller_dataframe_unchanged(self):
        frame = macro_frame()
        original = frame.copy(deep=True)
        normalize_macro_data(frame)
        assert_frame_equal(frame, original)

    def test_required_columns_validated(self):
        with self.assertRaisesRegex(PipelineError, "series_id"):
            normalize_macro_data(macro_frame().drop(columns=["series_id"]))

    def test_dot_value_rejected(self):
        self.assertEqual(normalize_macro_data(macro_frame(value=".")).rows_valid, 0)

    def test_null_nonnumeric_and_nonfinite_values_rejected(self):
        frame = pd.concat(
            [macro_frame(value=None), macro_frame(value="bad"), macro_frame(value=float("inf"))],
            ignore_index=True,
        )
        self.assertEqual(normalize_macro_data(frame).rows_dropped, 3)

    def test_invalid_date_rejected(self):
        self.assertEqual(normalize_macro_data(macro_frame(indicator_date="bad")).rows_valid, 0)

    def test_duplicate_keeps_final_input_occurrence(self):
        frame = pd.concat([macro_frame(value=1), macro_frame(value=2)], ignore_index=True)
        result = normalize_macro_data(frame)
        self.assertEqual(result.dataframe.iloc[0]["value"], 2)
        self.assertEqual(result.dropped_by_reason["duplicate_key"], 1)

    def test_value_rounded_to_six_places(self):
        value = normalize_macro_data(macro_frame()).dataframe.iloc[0]["value"]
        self.assertEqual(value, 123.123457)

    def test_metrics_are_accurate(self):
        frame = pd.concat([macro_frame(), macro_frame(value="."), macro_frame(indicator_date="bad")], ignore_index=True)
        result = normalize_macro_data(frame)
        self.assertEqual((result.rows_fetched, result.rows_valid, result.rows_dropped), (3, 1, 2))


if __name__ == "__main__":
    unittest.main()
