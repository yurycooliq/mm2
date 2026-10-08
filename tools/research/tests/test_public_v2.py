"""Cursor adapter boundaries and exact primary-documented V2 units; no network."""
from copy import deepcopy
from decimal import Decimal
import io
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from urllib.error import HTTPError
from unittest.mock import patch

from tools.research.observe_public_v2 import (MAX_BYTES, MAX_REQUESTS, MAX_SECONDS, PublicHTTPV2,
    envelope, observe_account, project_activity, project_position, project_reported_pnl, PNL_ENDPOINT)
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


def pnl_evidence(mutate=None, raw_override=None):
    data = {"proxy_wallet": ACCOUNT, "interval": "1d", "fidelity": "1h", "source_fidelity": "1d", "points": []}
    for index, at in enumerate((3600, 7200)):
        data["points"].append({"timestamp": at, "source_block": 100 + index, "trade_count": 1 + index,
            "realized_market_pnl": 100 - index, "realized_lp_pnl": 0, "realized_combo_pnl": 0,
            "realized_pnl": 100 - index, "volume": index, "volume_usdc": index,
            "unrealized_pnl": 0, "position_pnl": 100 - index, "wallet_income": 0,
            "economic_pnl": 100 - index, "deposits": None, "withdrawals": None, "cashflow_net": None})
    if mutate:
        mutate(data)
    raw = raw_override or json.dumps({"data": data}, separators=(",", ":"))
    return {"status": "AVAILABLE", "reason": None, "raw_body_utf8": raw,
            "receipt": {"endpoint": "user-pnl", "api_version": 2, "api_path": "/v2/user-pnl", "source_url": PNL_ENDPOINT,
                "parameters": {"user": ACCOUNT, "interval": "1d", "fidelity": "1h"}, "bytes": len(raw.encode()),
                "response_sha256": hashlib.sha256(raw.encode()).hexdigest(), "observed_at_utc": epoch_utc(10000)}}


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

    def test_optional_unavailable_pnl_never_discards_complete_activity_or_promotes_profit(self):
        api = FixtureAPI([page([activity(10)])])
        result = self.capture(api, include_reported_pnl=True)
        self.assertEqual(len(result["events"]), 1)
        self.assertTrue(result["coverage"]["activity_complete"])
        self.assertEqual(result["reported_pnl_evidence"]["status"], "UNAVAILABLE")
        self.assertFalse(result["coverage"]["financial_complete"])

    def test_valid_optional_exact_response_survives_verified_observer_adapter(self):
        class API(FixtureAPI):
            def get(self, endpoint, parameters):
                if endpoint != "user-pnl":
                    return super().get(endpoint, parameters)
                self.requests.append((endpoint, deepcopy(parameters)))
                self.pnl_evidence = pnl_evidence()
                return {}, self.pnl_evidence["receipt"]
        api = API([page([activity(10)])])
        result = self.capture(api, include_reported_pnl=True)
        self.assertEqual(result["reported_pnl_evidence"]["status"], "AVAILABLE")
        self.assertEqual(sum(endpoint == "user-pnl" for endpoint, _ in api.requests), 1)
        with tempfile.TemporaryDirectory() as temporary:
            observer = publish_bundle(Path(temporary), {"events.jsonl": result["events"], "closing.json": result["snapshot"],
                "coverage.json": result["coverage"], "source-reported-pnl.json": result["reported_pnl_evidence"]},
                scope={"kind": "one-public-account", "lane": "public-benchmark", "account_id": ACCOUNT, "chain_id": 137},
                utc_range={"start": epoch_utc(1), "end": epoch_utc(20)}, quality=result["quality"], created_at_utc=epoch_utc(10000))
            view, _ = build_reference(observer)
        reported = view["reported_performance"]
        self.assertEqual(reported["latest"]["economic_pnl_usd"], "99")
        self.assertEqual(reported["latest"]["unrealized_pnl_usd"], "0")
        self.assertEqual(reported["source_fidelity"], "1d")
        self.assertIn("PNL_WINDOW_DIFFERS_FROM_ACTIVITY", reported["warnings"])
        self.assertFalse(view["coverage"]["financial_complete"])
        self.assertIsNone(view["metrics"]["net_pnl_usd"]["value"])

    def test_optional_decimal_extremes_keep_activity_and_verified_publication(self):
        class API(FixtureAPI):
            def get(self, endpoint, parameters):
                if endpoint != "user-pnl":
                    return super().get(endpoint, parameters)
                self.requests.append((endpoint, deepcopy(parameters)))
                self.pnl_evidence = deepcopy(evidence)
                return {}, self.pnl_evidence["receipt"]
        for source, replacement in (('"economic_pnl":100', '"economic_pnl":1e1000000'),
                                    ('"wallet_income":0', '"wallet_income":1e-10000000')):
            with self.subTest(replacement=replacement):
                raw = pnl_evidence()["raw_body_utf8"].replace(source, replacement, 1)
                evidence = pnl_evidence(raw_override=raw)
                self.assertEqual(project_reported_pnl(evidence, ACCOUNT)["status"], "INVALID")
                result = self.capture(API([page([activity(10)])]), include_reported_pnl=True)
                self.assertEqual(len(result["events"]), 1)
                self.assertTrue(result["coverage"]["activity_complete"])
                self.assertEqual(result["reported_pnl_evidence"]["status"], "INVALID")
                with tempfile.TemporaryDirectory() as temporary:
                    observer = publish_bundle(Path(temporary), {"events.jsonl": result["events"], "closing.json": result["snapshot"],
                        "coverage.json": result["coverage"], "source-reported-pnl.json": result["reported_pnl_evidence"]},
                        scope={"kind": "one-public-account", "lane": "public-benchmark", "account_id": ACCOUNT, "chain_id": 137},
                        utc_range={"start": epoch_utc(1), "end": epoch_utc(20)}, quality=result["quality"], created_at_utc=epoch_utc(10000))
                    view, _ = build_reference(observer)
                self.assertEqual(len(view["actions"]), 1)
                self.assertTrue(view["coverage"]["actions_complete"])
                self.assertEqual(view["reported_performance"]["status"], "INVALID")
                self.assertIsNone(view["reported_performance"]["latest"]["economic_pnl_usd"])
                self.assertFalse(view["coverage"]["financial_complete"])
                self.assertIsNone(view["metrics"]["net_pnl_usd"]["value"])


class PnlProjectionTests(unittest.TestCase):
    def test_zero_and_nullable_reported_amounts_remain_distinct(self):
        def unknown(data):
            for row in data["points"]:
                row.update(realized_market_pnl=0, realized_pnl=0, position_pnl=None, economic_pnl=None, unrealized_pnl=None)
        result = project_reported_pnl(pnl_evidence(unknown), ACCOUNT)
        self.assertEqual(result["status"], "AVAILABLE")
        self.assertEqual(result["latest"]["realized_pnl_usd"], "0")
        self.assertIsNone(result["latest"]["economic_pnl_usd"])

    def test_duplicate_json_wrong_wallet_future_clock_precision_and_unknown_fields_fail_optional_only(self):
        mutations = [lambda data: data.update(proxy_wallet="0x"+"f"*40),
            lambda data: data["points"][-1].update(timestamp=10001),
            lambda data: data["points"][-1].update(timestamp=3600),
            lambda data: data["points"][-1].update(economic_pnl=99.0000000000001),
            lambda data: data["points"][-1].update(secret="ignored-instead-of-exposed"),
            lambda data: data.update(source_fidelity="unknown")]
        for mutation in mutations:
            self.assertEqual(project_reported_pnl(pnl_evidence(mutation), ACCOUNT)["status"], "INVALID")
        for raw in ('{"data":{},"data":{}}', '{"data":NaN}'):
            self.assertEqual(project_reported_pnl(pnl_evidence(raw_override=raw), ACCOUNT)["status"], "INVALID")
        evidence = pnl_evidence(); evidence["receipt"]["response_sha256"] = "f" * 64
        self.assertEqual(project_reported_pnl(evidence, ACCOUNT)["status"], "INVALID")

    def test_original_body_hash_and_current_baseline_are_required(self):
        evidence = pnl_evidence(); evidence["receipt"]["observed_at_utc"] = epoch_utc(20000)
        self.assertEqual(project_reported_pnl(evidence, ACCOUNT)["status"], "INVALID")
        evidence = pnl_evidence(); evidence["raw_body_utf8"] += " "
        self.assertEqual(project_reported_pnl(evidence, ACCOUNT)["status"], "INVALID")


class V2HTTPTests(unittest.TestCase):
    def test_optional_pnl_http_failure_has_one_attempt_no_retry_or_receipt(self):
        calls = []
        def opener(request, timeout):
            calls.append(request)
            raise HTTPError(request.full_url, 429, "rate limit", {}, None)
        http = PublicHTTPV2(opener=opener)
        with patch("tools.research.observe_public_v2.time.sleep") as sleep:
            with self.assertRaises(ResearchError):
                http.get("user-pnl", {"user": ACCOUNT, "interval": "1d", "fidelity": "1h"})
        self.assertEqual(len(calls), 1)
        self.assertEqual(http.request_count, 1)
        self.assertEqual(http.receipts, [])
        self.assertIsNone(http.pnl_evidence)
        sleep.assert_not_called()

    def test_optional_pnl_get_is_fixed_public_shared_budget_and_never_retried(self):
        body = pnl_evidence()["raw_body_utf8"].encode()
        calls = []
        def opener(request, timeout):
            calls.append(request); return io.BytesIO(body)
        http = PublicHTTPV2(max_requests=1, opener=opener)
        http.get("user-pnl", {"user": ACCOUNT, "interval": "1d", "fidelity": "1h"})
        self.assertEqual(http.pnl_evidence["raw_body_utf8"].encode(), body)
        self.assertEqual(http.bytes_read, len(body))
        self.assertIsNone(calls[0].get_header("Authorization"))
        with self.assertRaises(ResearchError): http.get("activity", {"user": ACCOUNT})
        self.assertEqual(len(calls), 1)
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
