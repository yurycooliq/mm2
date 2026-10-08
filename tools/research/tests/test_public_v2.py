"""Cursor adapter boundaries and exact primary-documented V2 units; no network."""
from copy import deepcopy
from decimal import Decimal
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.research.observe_public_v2 import (MAX_BYTES, MAX_REQUESTS, MAX_SECONDS, PublicHTTPV2,
    envelope, observe_account, project_activity, project_position)
from tools.research.schema import ResearchError, digest, epoch_utc
from tools.research.archive import publish_bundle
from tools.research.console_comparison import build_reference

ACCOUNT = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"
CONDITION = "0x" + "a" * 64


def activity(second=10, token="123"):
    return {"proxy_wallet": ACCOUNT, "timestamp": second, "condition_id": CONDITION, "type": "TRADE",
        "size": Decimal("1.000001"), "usdc_size": Decimal("0.3300003785"),
        "transaction_hash": "0x" + "b" * 64, "price": Decimal("0.3300000485"), "token_id": token,
        "side": "BUY", "outcome_index": 0, "outcome": "Up", "title": "Public enriched market"}


def position(token="123"):
    return {"proxy_wallet": ACCOUNT, "token_id": token, "condition_id": CONDITION, "current_size": Decimal("2.000001"),
        "avg_price": Decimal("0.33"), "entry_cost_usdc": Decimal("0.66"), "current_value": Decimal("0.8000004"),
        "current_price": Decimal("0.4"), "outcome": "Up", "outcome_index": 0, "redeemable": False,
        "mergeable": False, "negative_risk": False, "end_date": "2026-10-08", "status": "OPEN",
        "entry_fees_usdc": Decimal("0.01"), "total_cost_usdc": Decimal("0.67"), "total_size": Decimal("3"),
        "realized_pnl": Decimal("-0.1"), "unrealized_pnl": Decimal("0.1400004"), "total_pnl": Decimal("0.0400004")}


def page(rows, cursor=None, limit=2, offset=0):
    return {"data": rows, "pagination": {"has_more": cursor is not None, "next_cursor": cursor, "limit": limit, "offset": offset}}


class FixtureAPI:
    def __init__(self, activities=None, positions=None):
        self.pages = {"activity": activities or [page([])], "positions": positions or [page([])]}
        self.indices = {key: 0 for key in self.pages}
        self.requests, self.receipts = [], []

    def get(self, endpoint, parameters):
        self.requests.append((endpoint, deepcopy(parameters)))
        index = self.indices[endpoint]
        self.indices[endpoint] += 1
        result = self.pages[endpoint][min(index, len(self.pages[endpoint])-1)]
        receipt = {"endpoint": endpoint, "api_path": "/v2/" + endpoint, "api_version": 2,
            "observed_at_utc": epoch_utc(20), "response_sha256": "c" * 64, "rows": len(result["data"]), "parameters": parameters}
        self.receipts.append(receipt)
        return deepcopy(result), receipt


class V2ObserverTests(unittest.TestCase):
    def capture(self, api, **kwargs):
        return observe_account(api, {"account_id": ACCOUNT}, epoch_utc(10), epoch_utc(20), page_size=2, **kwargs)

    def test_cursor_walk_never_uses_offset_and_full_page_is_not_completeness(self):
        # A full terminal page is complete; only next_cursor controls continuation.
        api = FixtureAPI([page([activity(10), activity(11)], "opaque-A", offset=900), page([activity(12), activity(13)], None, offset=123456)])
        result = self.capture(api)
        self.assertTrue(result["coverage"]["activity_complete"])
        self.assertEqual(len(result["events"]), 4)
        calls = [query for endpoint, query in api.requests if endpoint == "activity"]
        self.assertNotIn("cursor", calls[0]); self.assertEqual(calls[1]["cursor"], "opaque-A")
        self.assertTrue(all("offset" not in query for query in calls))
        self.assertTrue(all(query["sort_direction"] == "ASC" and query["end"] == 19 for query in calls))
        self.assertTrue(all(query["exclude_deposits_withdrawals"] == "false" for query in calls))

    def test_repeated_cursor_or_empty_progress_is_partial_without_silent_retry(self):
        api = FixtureAPI([page([activity(10)], "repeat"), page([activity(11)], "repeat")])
        result = self.capture(api)
        self.assertFalse(result["coverage"]["activity_complete"])
        self.assertEqual(result["coverage"]["gaps"][0]["reason"], "V2_CURSOR_NO_PROGRESS")
        self.assertEqual(len(result["events"]), 2)
        self.assertEqual(api.indices["activity"], 2)
        result = self.capture(FixtureAPI([page([], "no-progress")]))
        self.assertFalse(result["coverage"]["activity_complete"])

    def test_source_v2_units_provenance_title_and_reported_fees_never_grant_accounting(self):
        raw = activity(10)
        # Cash is pico-exact; no float/rounding to make excessive precision fit.
        raw["usdc_size"] = Decimal("0.3300003785")
        api = FixtureAPI([page([raw])], [page([position()])])
        result = self.capture(api)
        event = result["events"][0]
        self.assertEqual(event["amounts"]["quantity_micros"], "1000001")
        self.assertEqual(event["amounts"]["cash_reported_pico"], "330000378500")
        self.assertEqual(event["source"]["kind"], "polymarket-activity-v2")
        self.assertEqual(result["activity_rows"][0]["display_title"], "Public enriched market")
        self.assertIn("token_id", result["activity_rows"][0]["source_v2_row"])
        held = result["snapshot"]["positions"][0]
        self.assertEqual(held["quantity_micros"], "2000001")
        self.assertEqual(held["basis_reported_pico"], "660000000000")
        self.assertIsNone(held["basis_actual_pico"])
        self.assertFalse(result["coverage"]["financial_complete"])
        self.assertIsNone(event["amounts"]["fee_actual_pico"])
        self.assertEqual(result["position_rows"][0]["source_v2_row"]["entry_fees_usdc"], "0.01")

    def test_schema_missing_v2_token_and_legacy_aliases_cannot_fall_back(self):
        raw = activity(10); raw["usdc_size"] = Decimal("0.3")
        raw["asset"] = raw.pop("token_id")
        with self.assertRaises(ResearchError): self.capture(FixtureAPI([page([raw])]))
        raw = position(); raw["size"] = raw.pop("current_size")
        with self.assertRaises(ResearchError): self.capture(FixtureAPI(positions=[page([raw])]))
        with self.assertRaises(ResearchError): envelope([])
        bad = page([]); bad["pagination"]["has_more"] = True
        with self.assertRaises(ResearchError): envelope(bad)

    def test_wrong_account_window_backward_order_and_inexact_units_abort_publication(self):
        for raw in (activity(9), activity(20), activity(10)):
            raw["usdc_size"] = Decimal("0.3")
            if raw["timestamp"] == 10: raw["proxy_wallet"] = "0x" + "9" * 40
            with self.subTest(raw=raw):
                with self.assertRaises(ResearchError): self.capture(FixtureAPI([page([raw])]))
        a, b = activity(12), activity(11)
        a["usdc_size"] = b["usdc_size"] = Decimal("0.3")
        with self.assertRaises(ResearchError): self.capture(FixtureAPI([page([a,b])]))
        raw = activity(10); raw["usdc_size"] = Decimal("0.3300003785000485")
        with self.assertRaises(ResearchError): self.capture(FixtureAPI([page([raw])]))
        raw = activity(10); raw["usdc_size"] = Decimal("0.3"); raw["size"] = Decimal("1.0000001")
        with self.assertRaises(ResearchError): self.capture(FixtureAPI([page([raw])]))

    def test_total_row_quota_and_position_duplicate_are_partial_and_finite(self):
        a, b = activity(10), activity(11); a["usdc_size"] = b["usdc_size"] = Decimal("0.3")
        result = self.capture(FixtureAPI([page([a,b])]), max_rows=1)
        self.assertEqual(len(result["events"]), 1)
        self.assertEqual(result["position_rows"], [])
        self.assertFalse(result["coverage"]["activity_complete"])
        self.assertFalse(result["coverage"]["positions_page_complete"])
        result = self.capture(FixtureAPI(positions=[page([position(), position()])]))
        self.assertFalse(result["coverage"]["positions_page_complete"])
        self.assertEqual(len(result["snapshot"]["positions"]), 1)

    def test_empty_v2_page_is_observed_empty_not_financial_zero_and_open_dust_archived_filters_are_explicit(self):
        api = FixtureAPI()
        result = self.capture(api)
        self.assertEqual(result["events"], [])
        self.assertTrue(result["coverage"]["activity_complete"])
        self.assertIsNone(result["snapshot"]["cash_pico"])
        self.assertFalse(result["snapshot"]["financial_complete"])
        query = next(query for endpoint, query in api.requests if endpoint == "positions")
        self.assertEqual((query["status"], query["filter_amount"], query["include_archived"]), ("OPEN", "0", "true"))

    def test_comparison_preserves_verified_v2_feed_and_position_scope_limits(self):
        result = self.capture(FixtureAPI([page([activity(10)])], [page([position()])]))
        self.assertTrue(result["coverage"]["activity_complete"])
        self.assertTrue(result["coverage"]["positions_page_complete"])
        with tempfile.TemporaryDirectory() as temporary:
            observer = publish_bundle(Path(temporary), {
                "events.jsonl": result["events"], "closing.json": result["snapshot"],
                "coverage.json": result["coverage"], "source-activity.jsonl": result["activity_rows"],
            }, scope={"kind": "one-public-account", "lane": "public-benchmark", "account_id": ACCOUNT, "chain_id": 137},
                utc_range={"start": epoch_utc(1), "end": epoch_utc(20)},
                quality=result["quality"], created_at_utc=epoch_utc(20))
            view, _ = build_reference(observer)
        self.assertTrue(view["coverage"]["actions_complete"])
        for reason in ("V2_DEFAULT_FEED_EXCLUDES_OPT_IN_TIPS", "V2_OPEN_POSITIONS_NOT_COMPLETE_WALLET_LIFECYCLE",
                       "PROFILE_ATTRIBUTION_REVERIFY_REQUIRED"):
            self.assertIn(reason, view["warnings"])
        self.assertFalse(view["coverage"]["financial_complete"])
        self.assertIsNone(view["metrics"]["net_pnl_usd"]["value"])


class V2HTTPTests(unittest.TestCase):
    def test_only_public_v2_get_and_finite_envelope_receipt(self):
        calls = []
        body = b'{"data":[],"pagination":{"has_more":false,"limit":2,"offset":0,"next_cursor":null}}'
        def opener(request, timeout):
            calls.append((request, timeout)); return io.BytesIO(body)
        http = PublicHTTPV2(max_bytes=1000, max_requests=1, max_seconds=1, opener=opener)
        result, receipt = http.get("activity", {"user": ACCOUNT, "limit": 2})
        self.assertEqual(result["data"], [])
        self.assertEqual(calls[0][0].get_method(), "GET")
        self.assertTrue(calls[0][0].full_url.startswith("https://data-api.polymarket.com/v2/activity?"))
        self.assertIsNone(calls[0][0].get_header("Authorization"))
        self.assertEqual(receipt["api_version"], 2)
        with self.assertRaises(ResearchError): http.get("positions", {"user": ACCOUNT})
        with self.assertRaises(ResearchError): http.get("orders", {"user": ACCOUNT})
        with self.assertRaises(ResearchError): http.get("activity", {"user": ACCOUNT, "offset": 1})

    def test_byte_clock_schema_and_hard_budget_failures_never_create_receipt(self):
        for body in (b"[]", b"x" * 101, b'{"data":[],"pagination":{}}'):
            http = PublicHTTPV2(max_bytes=100, max_requests=1, max_seconds=1, opener=lambda *_args, **_kwargs: io.BytesIO(body))
            with self.assertRaises(ResearchError): http.get("activity", {"user": ACCOUNT})
            self.assertEqual(http.receipts, [])
        for overrides in ({"max_bytes": MAX_BYTES+1}, {"max_requests": MAX_REQUESTS+1}, {"max_seconds": MAX_SECONDS+1}, {"max_bytes": True}):
            with self.assertRaises(ResearchError): PublicHTTPV2(**overrides)
        http = PublicHTTPV2(opener=lambda *_args, **_kwargs: io.BytesIO(b"[]"))
        http.deadline = 0
        with self.assertRaises(ResearchError): http.get("activity", {"user": ACCOUNT})

    def test_trickling_body_is_checked_between_reads_and_never_published_after_deadline(self):
        clock = [0]
        class SlowBody(io.BytesIO):
            def read1(self, limit):
                clock[0] += 2
                return super().read1(limit)
        with patch("tools.research.observe_public_v2.time.monotonic", side_effect=lambda: clock[0]):
            http = PublicHTTPV2(max_seconds=1, opener=lambda *_args, **_kwargs: SlowBody(b'{"data":[]}'))
            with self.assertRaises(ResearchError): http.get("activity", {"user": ACCOUNT})
        self.assertEqual(http.receipts, [])
        self.assertEqual(http.request_count, 1)


if __name__ == "__main__":
    unittest.main()
