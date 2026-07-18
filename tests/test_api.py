import unittest

import requests

from ingestion.ingest_macro import fetch_macro_data, get_fred_api_key
from ingestion.ingest_stocks import fetch_stock_data
from ingestion.pipeline_utils import PipelineError, safe_error_message


class FakeResponse:
    def __init__(self, payload=None, json_error=None, http_error=None):
        self.payload = payload
        self.json_error = json_error
        self.http_error = http_error

    def raise_for_status(self):
        if self.http_error:
            raise self.http_error

    def json(self):
        if self.json_error:
            raise self.json_error
        return self.payload


def request_returning(response, capture=None):
    def fake_get(url, **kwargs):
        if capture is not None:
            capture.update({"url": url, **kwargs})
        return response
    return fake_get


class AlphaVantageApiTests(unittest.TestCase):
    def test_success_uses_params_and_timeout(self):
        capture = {}
        response = FakeResponse({"Time Series (Daily)": {"2026-01-02": {
            "1. open": "1", "2. high": "2", "3. low": "1", "4. close": "2", "5. volume": "10"
        }}})
        frame = fetch_stock_data("AAPL", "secret", request_returning(response, capture))
        self.assertEqual(len(frame), 1)
        self.assertNotIn("secret", capture["url"])
        self.assertEqual(capture["params"]["apikey"], "secret")
        self.assertEqual(capture["timeout"], 30)

    def test_rate_limit_response(self):
        with self.assertRaisesRegex(PipelineError, "rate limited") as caught:
            fetch_stock_data("AAPL", "secret", request_returning(FakeResponse({"Note": "secret"})))
        self.assertEqual(caught.exception.code, "API_RATE_LIMITED")

    def test_error_response(self):
        with self.assertRaises(PipelineError) as caught:
            fetch_stock_data("BAD", "secret", request_returning(FakeResponse({"Error Message": "secret"})))
        self.assertEqual(caught.exception.code, "API_DATA_UNAVAILABLE")

    def test_missing_time_series(self):
        with self.assertRaises(PipelineError):
            fetch_stock_data("AAPL", "secret", request_returning(FakeResponse({})))

    def test_request_timeout(self):
        def timeout(*args, **kwargs):
            raise requests.Timeout("apikey=secret")
        with self.assertRaises(PipelineError) as caught:
            fetch_stock_data("AAPL", "secret", timeout)
        self.assertEqual(caught.exception.code, "API_TIMEOUT")
        self.assertNotIn("secret", caught.exception.public_message)

    def test_malformed_json(self):
        with self.assertRaises(PipelineError) as caught:
            fetch_stock_data("AAPL", "secret", request_returning(FakeResponse(json_error=ValueError())))
        self.assertEqual(caught.exception.code, "API_RESPONSE_INVALID")


class FredApiTests(unittest.TestCase):
    def test_success(self):
        capture = {}
        frame = fetch_macro_data(
            "GDP", "Gross Domestic Product", "secret",
            request_returning(FakeResponse({"observations": [{"date": "2026-01-01", "value": "4.2"}]}), capture),
        )
        self.assertEqual(len(frame), 1)
        self.assertNotIn("secret", capture["url"])
        self.assertEqual(capture["timeout"], 30)

    def test_missing_observations(self):
        with self.assertRaises(PipelineError):
            fetch_macro_data("GDP", "GDP", "secret", request_returning(FakeResponse({})))

    def test_invalid_values_are_left_for_normalization(self):
        frame = fetch_macro_data(
            "GDP", "GDP", "secret",
            request_returning(FakeResponse({"observations": [{"date": "2026-01-01", "value": "."}]})),
        )
        self.assertEqual(frame.iloc[0]["value"], ".")

    def test_canonical_and_legacy_fred_keys(self):
        self.assertEqual(get_fred_api_key({"FRED_API_KEY": "new", "FRED_KEY": "old"}), "new")
        with self.assertWarns(DeprecationWarning):
            self.assertEqual(get_fred_api_key({"FRED_KEY": "old"}), "old")

    def test_secret_redaction_is_bounded(self):
        cleaned = safe_error_message("password=hunter2 api_key=secret " + "x" * 600, ["hunter2"])
        self.assertNotIn("hunter2", cleaned)
        self.assertNotIn("secret", cleaned)
        self.assertLessEqual(len(cleaned), 500)


if __name__ == "__main__":
    unittest.main()
