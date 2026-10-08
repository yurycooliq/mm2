"""Accounting conservation, uncertain evidence and bounded immutable collection."""
from copy import deepcopy
from decimal import Decimal
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
from tools.research import archive
from tools.research.accounting import calculate_account, snapshot_nav
from tools.research.archive import publish_bundle, read_member, verify_bundle
from tools.research.compare import build_comparison
from tools.research.observe_mo_money import (PublicHTTP, collect_activity, collect_positions, normalize_activity,
                                            observe_account, position_snapshot, update_watermark)
from tools.research.reconcile import journal_coverage_gate, reconcile_events
from tools.research.registry import load_registry, validate_registry
from tools.research.schema import (VERSION, MICRO, PICO, ResearchError, canonical, digest, epoch_utc, integer,
                                   parse_json, quality, units, validate_event, validate_snapshot, window)

ACCOUNT = "0x" + "1" * 40
MARKET = "0x" + "2" * 64
START, END = "2026-10-06T00:00:00Z", "2026-10-07T00:00:00Z"
RANGE = {"start": START, "end": END}
SCOPE = {"account_id": ACCOUNT, "lane": "public-paper", "kind": "fixture-only"}
COMPLETE = quality("COMPLETE", accounting=True)


def amount(value):
    return units(value, PICO)


def position(asset="up", quantity="0", value=None, actual=True, at=None):
    return {"asset_id": asset, "condition_id": MARKET, "quantity_micros": units(quantity, MICRO),
            "basis_actual_pico": None, "basis_bound_pico": None,
            "valuation": None if value is None else {"lower_pico": amount(value), "upper_pico": amount(value),
                "kind": "verified_payout", "complete": actual, "at_utc": at}}


def snapshot(cash="100", positions=None, at=START):
    positions = deepcopy(positions or [])
    for item in positions:
        if item.get("valuation") is not None and item["valuation"].get("at_utc") is None:
            item["valuation"]["at_utc"] = at
    return {"schema_version": VERSION, "account_id": ACCOUNT, "lane": "public-paper", "at_utc": at,
            "cash_pico": amount(cash) if cash is not None else None, "reserved_pico": None,
            "positions": positions, "positions_complete": True, "financial_complete": True}


def event(identity, cash, changes=None, external="0", kind="TRADE", amounts=None):
    return {"schema_version": VERSION, "event_id": identity, "account_id": ACCOUNT, "lane": "public-paper",
            "event_type": kind, "event_time": "2026-10-06T12:00:00Z", "observed_at_utc": "2026-10-06T12:00:01Z",
            "available_at": "2026-10-06T12:00:01Z", "evidence_class": "synthetic", "source": {"kind": "test-fixture", "record_hash": "3" * 64},
            "effects": {"cash_delta_pico": units(cash, PICO, signed=True) if cash is not None else None,
                "external_contribution_pico": units(external, PICO, signed=True) if external is not None else None,
                "position_deltas": [{"asset_id": asset, "condition_id": MARKET,
                    "quantity_delta_micros": units(quantity, MICRO, signed=True)} for asset, quantity in changes or []]},
            "amounts": amounts or {}, "quality": deepcopy(COMPLETE)}


def calculate(events, opening=None, closing=None, q=None):
    return calculate_account(events, opening, closing, utc_range=RANGE, scope=SCOPE, source_quality=q or COMPLETE)


def activity(timestamp, index=0, kind="TRADE"):
    return {"proxyWallet": ACCOUNT, "timestamp": timestamp, "conditionId": MARKET, "type": kind,
            "size": "5", "usdcSize": "2", "transactionHash": "0x" + format(index, "064x"), "price": "0.4",
            "asset": "123", "side": "BUY", "outcomeIndex": 0, "outcome": "Up"}


class FixtureAPI:
    def __init__(self, activity_rows=None, position_rows=None):
        self.activity_rows = activity_rows or []
        self.position_rows = position_rows or []
        self.requests = []

    def get(self, endpoint, params):
        self.requests.append((endpoint, params))
        records = self.activity_rows if endpoint == "activity" else self.position_rows
        if endpoint == "activity":
            records = [r for r in records if params["start"] <= r["timestamp"] <= params["end"]]
        offset = params["offset"]
        page = deepcopy(records[offset:offset + params["limit"]])
        return page, {"endpoint": endpoint, "parameters": params, "observed_at_utc": "2026-10-07T00:00:02Z", "rows": len(page)}


class ExactSchemaTests(unittest.TestCase):
    def test_decimal_no_binary_float_excess_precision_or_nonfinite(self):
        self.assertEqual(units(Decimal("0.123456"), MICRO), "123456")
        self.assertEqual(units("1234567890123456789.123456", MICRO), "1234567890123456789123456")
        for value in (0.1, True, "NaN", "Infinity", "0.0000001"):
            with self.assertRaises(ResearchError):
                units(value, MICRO)

    def test_json_duplicate_key_float_nonfinite_and_secret(self):
        for raw in ('{"x":1,"x":2}', '{"x":0.2}', '{"x":NaN}', '{"api_secret":"secret"}'):
            with self.assertRaises(ResearchError):
                parse_json(raw)
        self.assertEqual(parse_json('{"x":0.2}', api=True)["x"], Decimal("0.2"))

    def test_no_false_event_timestamp_or_actual_cost(self):
        invalid = event("x", "-1")
        invalid["available_at"] = "2026-10-06T12:00:02Z"
        with self.assertRaises(ResearchError):
            validate_event(invalid)
        invalid = event("x", "-1", amounts={"cost_actual_pico": "100", "cost_bound_pico": "99"})
        with self.assertRaises(ResearchError):
            validate_event(invalid)
        with self.assertRaises(ResearchError):
            window(END, START)
        invalid = event("future", "0")
        invalid.update({"lane": "real", "evidence_class": "authenticated", "event_time": END})
        with self.assertRaises(ResearchError):
            validate_event(invalid)
        invalid = event("synthetic-live", "0")
        invalid["lane"] = "real"
        with self.assertRaises(ResearchError):
            validate_event(invalid)

    def test_duplicate_snapshot_asset(self):
        with self.assertRaises(ResearchError):
            validate_snapshot(snapshot(positions=[position("up", "1", "1"), position("up", "2", "2")]))


class AccountingTests(unittest.TestCase):
    def test_unknown_bound_differs_from_explicit_zero_and_preserves_known_subtotal(self):
        known = event("known", "0", amounts={"cost_bound_pico": "123"})
        unknown = event("unknown", "0")
        result = calculate([known, unknown], snapshot(), snapshot(at=END))
        self.assertEqual(result["components"]["observed_cost_bound_pico"], "123")
        self.assertIsNone(result["components"]["conservative_cost_bound_pico"])
        self.assertEqual(result["components"]["cost_evidence_coverage"]["missing_cost_bound"], 1)
        # Full NAV accounting remains valid independently of cost decomposition.
        self.assertEqual(result["net_pnl_pico"], "0")
        unknown["amounts"]["cost_bound_pico"] = "0"
        self.assertEqual(calculate([known, unknown], snapshot(), snapshot(at=END))["components"]["conservative_cost_bound_pico"], "123")

    def test_empty_partial_window_has_unknown_bound_not_free_execution(self):
        result = calculate([], None, snapshot(at=END), quality("PARTIAL"))
        self.assertEqual(result["components"]["observed_cost_bound_pico"], "0")
        for name in ("conservative_cost_bound_pico", "actual_cost_total_pico", "actual_fees_total_pico"):
            self.assertIsNone(result["components"][name])
        self.assertEqual(calculate([], snapshot(), snapshot(at=END))["components"]["conservative_cost_bound_pico"], "0")

    def test_closeout_unknown_cost_applicability_keeps_complete_bound_unknown(self):
        events = [event("trade", "0", amounts={"cost_bound_pico": "100"}), event("closeout", "0", kind="MERGE")]
        result = calculate(events, snapshot(), snapshot(at=END))
        self.assertIsNone(result["components"]["conservative_cost_bound_pico"])
        self.assertEqual(result["components"]["cost_evidence_coverage"]["cost_bound_records"], 2)
        self.assertIsNone(result["components"]["actual_cost_total_pico"])
        self.assertIsNone(result["components"]["actual_fees_total_pico"])

    def test_fee_inclusive_cash_not_charged_twice_and_reservation_not_loss(self):
        opening = snapshot()
        opening["reserved_pico"] = amount("4.5")
        buy = event("buy", "-4.1", [("up", "10")], amounts={"cost_actual_pico": amount("4.1"),
            "cost_bound_pico": amount("4.5"), "fee_actual_pico": amount("0.1")})
        closing = snapshot("95.9", [position("up", "10", "4.99")], END)
        result = calculate([buy], opening, closing)
        self.assertEqual(result["net_pnl_pico"], amount("0.89"))
        self.assertEqual(result["nav"]["opening"]["actual_pico"], amount("100"))
        self.assertEqual(result["components"]["observed_actual_fees_pico"], amount("0.1"))
        self.assertEqual(result["components"]["conservative_cost_bound_pico"], amount("4.5"))

    def test_positive_pairs_do_not_hide_losing_unpaired_residue(self):
        events = [event("up-buy", "-4", [("up", "10")]), event("down-buy", "-4.4", [("down", "8")]),
                  event("merge", "8", [("up", "-8"), ("down", "-8")], kind="MERGE")]
        result = calculate(events, snapshot(), snapshot("99.6", [position("up", "2", "0")], END))
        self.assertEqual(result["net_pnl_pico"], units("-0.4", PICO, signed=True))
        self.assertEqual(result["retained_positions"][0]["quantity_micros"], "2000000")
        self.assertIsNone(result["components"]["realized_pnl_pico"])

    def test_deposit_and_transferred_tokens_are_not_profit(self):
        result = calculate([event("deposit", "100", external="100", kind="DEPOSIT")], snapshot(), snapshot("200", at=END))
        self.assertEqual(result["net_pnl_pico"], "0")
        self.assertIsNone(result["roi"])
        token = event("transfer", "0", [("up", "10")], external="10", kind="TRANSFER")
        result = calculate([token], snapshot(), snapshot("100", [position("up", "10", "8")], END))
        self.assertEqual(result["net_pnl_pico"], units("-2", PICO, signed=True))
        token["effects"]["external_contribution_pico"] = None
        self.assertIsNone(calculate([token], snapshot(), snapshot("100", [position("up", "10", "8")], END))["net_pnl_pico"])

    def test_opening_unknown_basis_does_not_invent_lifetime_pnl(self):
        result = calculate([event("sell", "10", [("up", "-10")])],
                           snapshot("100", [position("up", "10", "5")]), snapshot("110", at=END))
        self.assertEqual(result["net_pnl_pico"], amount("5"))
        self.assertIsNone(result["components"]["realized_pnl_pico"])

    def test_interval_requires_complete_cashflows_and_all_old_positions(self):
        closing = snapshot("90", [position("up", "10", "10", actual=False)], END)
        closing["positions"][0]["valuation"]["lower_pico"] = "0"
        result = calculate([event("buy", "-10", [("up", "10")])], snapshot(), closing)
        self.assertIsNone(result["net_pnl_pico"])
        self.assertEqual(result["net_pnl_interval_pico"], {"lower": units("-10", PICO, signed=True), "upper": "0"})
        closing["positions_complete"] = False
        self.assertIsNone(calculate([event("buy", "-10", [("up", "10")])], snapshot(), closing)["net_pnl_interval_pico"])

    def test_missing_opening_and_public_data_never_claim_roi(self):
        result = calculate([], None, snapshot(at=END), quality("PARTIAL", ["MISSING_ACTIVITY"]))
        self.assertIsNone(result["net_pnl_pico"])
        self.assertIsNone(result["roi"])
        self.assertIn("OPENING_STATE_UNKNOWN", result["quality"]["reasons"])

    def test_stale_or_future_position_mark_never_values_current_boundary(self):
        for mark in (START, "2026-10-08T00:00:00Z", None):
            with self.subTest(mark=mark):
                closing = snapshot("90", [position("up", "10", "20")], END)
                closing["positions"][0]["valuation"]["at_utc"] = mark
                result = calculate([event("buy", "-10", [("up", "10")])], snapshot(), closing)
                self.assertIsNone(result["net_pnl_pico"])
                self.assertIsNone(result["net_pnl_interval_pico"])
                self.assertIsNone(result["nav"]["closing"]["actual_pico"])
                self.assertIn("POSITION_VALUATION_NOT_AT_SNAPSHOT_BOUNDARY", result["quality"]["reasons"])

    def test_cash_or_position_conflict_fails_financial_result(self):
        result = calculate([event("buy", "-4", [("up", "10")])], snapshot(), snapshot("95", [position("up", "9", "4")], END))
        self.assertEqual(result["quality"]["status"], "CONFLICTED")
        self.assertIsNone(result["net_pnl_pico"])
        self.assertIsNone(result["net_pnl_interval_pico"])

    def test_duplicate_receipt_id_idempotent_conflict_quarantined(self):
        first = event("deposit", "1", external="1", kind="DEPOSIT")
        result = calculate([first, first], snapshot(), snapshot("101", at=END))
        self.assertEqual(result["duplicates"], 1)
        self.assertEqual(result["net_pnl_pico"], "0")
        altered = deepcopy(first)
        altered["effects"]["cash_delta_pico"] = "2"
        self.assertEqual(calculate([first, altered], snapshot(), snapshot("101", at=END))["quality"]["status"], "CONFLICTED")

    def test_overlap_reobservation_does_not_create_false_financial_conflict(self):
        earlier = event("deposit", "1", external="1", kind="DEPOSIT")
        later = deepcopy(earlier)
        later["observed_at_utc"] = later["available_at"] = "2026-10-06T12:01:00Z"
        later["source"]["request_receipt_hash"] = "5" * 64
        reconciled = reconcile_events([later, earlier], ACCOUNT, RANGE)
        self.assertEqual(reconciled["quality"]["status"], "COMPLETE")
        self.assertEqual(reconciled["duplicates"], 1)
        self.assertEqual(reconciled["events"][0]["observed_at_utc"], earlier["observed_at_utc"])

    def test_session_checkpoint_bound_is_not_actual_financial_evidence(self):
        coverage = {"committed_continuity": True, "financial_effects_complete": False,
                    "facts": {"cash_effects": "bound", "fill_cost_basis": "bound"}}
        self.assertFalse(journal_coverage_gate(coverage)["eligibility"]["accounting"])
        result = calculate_account([], snapshot(), snapshot(at=END), utc_range=RANGE, scope=SCOPE,
                                   source_quality=COMPLETE, journal_coverage=coverage)
        self.assertIsNone(result["net_pnl_pico"])
        self.assertIsNone(result["net_pnl_interval_pico"])

    def test_empty_or_contradictory_source_quality_cannot_grant_accounting(self):
        for evidence in (quality("PARTIAL"), quality("PARTIAL", accounting=True), quality("CONFLICTED", accounting=True)):
            with self.subTest(status=evidence["status"]):
                result = calculate_account([], snapshot(), snapshot(at=END), utc_range=RANGE, scope=SCOPE,
                                           source_quality=evidence)
                self.assertIsNone(result["net_pnl_pico"])
                self.assertIsNone(result["net_pnl_interval_pico"])
                self.assertFalse(result["quality"]["eligibility"]["accounting"])
                self.assertIn("SOURCE_FINANCIAL_COMPLETENESS_UNPROVEN", result["quality"]["reasons"])

    def test_journal_complete_flag_and_event_quality_must_be_explicit(self):
        coverage = {"committed_continuity": True, "facts": {name: "observed" for name in
                    ("cash_effects", "position_effects", "external_cashflows", "actual_fees", "fill_cost_basis")}}
        self.assertFalse(journal_coverage_gate(coverage)["eligibility"]["accounting"])
        coverage["financial_effects_complete"] = True
        self.assertTrue(journal_coverage_gate(coverage)["eligibility"]["accounting"])
        record = event("e", "0")
        record["quality"] = quality("PARTIAL", accounting=True)
        result = calculate([record], snapshot(), snapshot(at=END))
        self.assertIsNone(result["net_pnl_pico"])
        self.assertIsNone(result["net_pnl_interval_pico"])
        record["quality"] = deepcopy(COMPLETE)
        record["evidence_class"] = "inferred"
        result = calculate([record], snapshot(), snapshot(at=END))
        self.assertIsNone(result["net_pnl_pico"])
        self.assertIn("EVENT_ACCOUNTING_AUTHORITY_UNPROVEN", result["quality"]["reasons"])

    def test_snapshot_outside_end_and_missing_occurrence_do_not_score(self):
        closing = snapshot(at="2026-10-07T00:00:01Z")
        self.assertIsNone(calculate([], snapshot(), closing)["net_pnl_interval_pico"])
        missing = event("e", "0")
        missing["event_time"] = None
        missing_result = calculate([missing], snapshot(), snapshot(at=END))
        self.assertIsNone(missing_result["net_pnl_pico"])
        self.assertIsNone(missing_result["net_pnl_interval_pico"])


class ObserverTests(unittest.TestCase):
    def test_activity_half_open_dense_windows_split_and_keep_multiplicity(self):
        rows = [activity(second, second * 10 + i) for second in (1, 2, 3) for i in (0, 1)]
        api = FixtureAPI(rows)
        result = collect_activity(api, ACCOUNT, epoch_utc(1), epoch_utc(4), page_size=2, offset_cap=2)
        self.assertTrue(result["complete"])
        self.assertEqual(len(result["rows"]), 6)
        self.assertTrue(all(p["excludeDepositsWithdrawals"] == "false" for endpoint, p in api.requests))
        self.assertTrue(all(1 <= r["row"]["timestamp"] < 4 for r in result["rows"]))
        same = activity(1)
        api = FixtureAPI([same, same])
        result = observe_account(api, {"account_id": ACCOUNT}, epoch_utc(1), epoch_utc(2), page_size=2)
        self.assertEqual(len(result["events"]), 2)
        self.assertNotEqual(result["events"][0]["event_id"], result["events"][1]["event_id"])

    def test_single_dense_second_cannot_silently_drop_rows(self):
        api = FixtureAPI([activity(1, i) for i in range(10)])
        result = collect_activity(api, ACCOUNT, epoch_utc(1), epoch_utc(2), page_size=2, offset_cap=2)
        self.assertFalse(result["complete"])
        self.assertEqual(result["gaps"][0]["reason"], "DENSE_SECOND_OFFSET_CAP")

    def test_pagination_byte_row_budget_or_wrong_account_gives_partial(self):
        result = collect_activity(FixtureAPI([activity(1, i) for i in range(4)]), ACCOUNT,
                                  epoch_utc(1), epoch_utc(2), page_size=2, max_rows=3)
        self.assertFalse(result["complete"])
        self.assertLessEqual(len(result["rows"]), 3)
        bad = activity(1)
        bad["proxyWallet"] = "0x" + "9" * 40
        self.assertFalse(collect_activity(FixtureAPI([bad]), ACCOUNT, epoch_utc(1), epoch_utc(2))["complete"])

    def test_positions_include_dust_archived_and_nonatomic_flag(self):
        api = FixtureAPI(position_rows=[{"proxyWallet": ACCOUNT, "asset": "1", "conditionId": MARKET,
                     "size": "0.02", "initialValue": "0.01", "currentValue": "0", "outcome": "Down"}])
        result = collect_positions(api, ACCOUNT)
        self.assertTrue(result["complete"])
        self.assertEqual(api.requests[0][1]["sizeThreshold"], "0")
        self.assertEqual(api.requests[0][1]["includeArchived"], "true")
        snapshot_result = position_snapshot(result, ACCOUNT)
        self.assertEqual(snapshot_result["positions"][0]["quantity_micros"], "20000")
        self.assertIsNone(snapshot_result["cash_pico"])
        self.assertFalse(snapshot_result["financial_complete"])

    def test_redeem_outcome_rows_are_distinct_and_zero_loser_retained(self):
        first = activity(1, 9, "REDEEM")
        second = deepcopy(first)
        second.update({"asset": "456", "outcome": "Down", "usdcSize": "0", "outcomeIndex": 1})
        def normalize(row):
            return normalize_activity({"row": row, "receipt_hash": "4" * 64,
                                       "observed_at_utc": "2026-10-07T00:00:02Z"}, ACCOUNT)
        a, b = normalize(first), normalize(second)
        self.assertNotEqual(a["event_id"], b["event_id"])
        self.assertEqual(b["effects"]["cash_delta_pico"], "0")
        self.assertFalse(b["quality"]["eligibility"]["accounting"])

    def test_unknown_type_and_missing_fees_remain_unknown(self):
        row = activity(1)
        item = {"row": row, "receipt_hash": "4" * 64, "observed_at_utc": "2026-10-07T00:00:02Z"}
        normalized = normalize_activity(item, ACCOUNT)
        self.assertIsNone(normalized["amounts"]["fee_actual_pico"])
        self.assertIsNone(normalized["amounts"]["cost_actual_pico"])
        row["type"] = "UNKNOWN_NEW_FINANCIAL_TYPE"
        self.assertIsNone(normalize_activity(item, ACCOUNT)["effects"]["cash_delta_pico"])

    def test_real_captured_public_pages_preserve_api_precision_and_missing_asset(self):
        fixtures = Path(__file__).resolve().parent / "fixtures"
        for name in ("mo-money-activity.json", "mm2-public-activity.json"):
            fixture = parse_json((fixtures / name).read_bytes())
            self.assertEqual(fixture["fixture_kind"], "captured-public-report-only")
            self.assertFalse(fixture["financial_complete"])
            for row in fixture["rows"]:
                normalized = normalize_activity({"row": row, "receipt_hash": digest(fixture["receipt"]),
                    "observed_at_utc": fixture["capture_at_utc"]}, fixture["request"]["user"])
                self.assertFalse(normalized["quality"]["eligibility"]["accounting"])
                self.assertEqual(normalized["reported_price_decimal"], str(row["price"]))
                if row["type"] == "REDEEM" and not row["asset"]:
                    self.assertIn("REDEMPTION_TOKEN_MAPPING_REQUIRED", normalized["quality"]["reasons"])
                    self.assertEqual(normalized["effects"]["position_deltas"], [])
        fixture = parse_json((fixtures / "mo-money-activity.json").read_bytes())
        fractional = next(row for row in fixture["rows"] if row["price"] == "0.3300000485")
        normalized = normalize_activity({"row": fractional, "receipt_hash": digest(fixture["receipt"]),
            "observed_at_utc": fixture["capture_at_utc"]}, fixture["request"]["user"])
        self.assertIsNone(normalized["amounts"]["price_micros"])

    def test_watermark_only_after_complete_publication_and_monotonic(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            state = Path(temp) / "cursor.json"
            self.assertFalse(update_watermark(state, ACCOUNT, END, "4" * 64, False))
            self.assertFalse(state.exists())
            self.assertTrue(update_watermark(state, ACCOUNT, END, "4" * 64, True))
            self.assertFalse(update_watermark(state, ACCOUNT, START, "5" * 64, True))
            self.assertFalse(json.loads(state.read_text())["accounts"][ACCOUNT]["financial_complete"])

    def test_http_only_public_get_no_redirect_and_finite_response(self):
        class Response(io.BytesIO):
            status = 200
        http = PublicHTTP(max_bytes=30, max_requests=1, max_seconds=1,
                          opener=lambda request, timeout: Response(b"[]"))
        self.assertEqual(http.get("activity", {"user": ACCOUNT})[0], [])
        with self.assertRaises(ResearchError):
            http.get("activity", {"user": ACCOUNT})
        with self.assertRaises(ResearchError):
            http.get("orders", {})


class ArchiveComparisonTests(unittest.TestCase):
    @staticmethod
    def windows_access_denied():
        error = PermissionError("simulated Windows rename access denied")
        error.winerror = 5
        return error

    def test_transient_windows_rename_reuses_verified_staging_without_rewriting(self):
        original = archive.os.rename
        calls = []
        def transient(source, target):
            calls.append((source, target))
            if len(calls) == 1:
                raise self.windows_access_denied()
            return original(source, target)
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            with patch.object(archive.os, "rename", side_effect=transient), patch.object(archive.time, "sleep") as sleep:
                bundle = publish_bundle(temp, {"events.jsonl": []}, scope=SCOPE,
                    utc_range=RANGE, quality=COMPLETE, created_at_utc=END)
            self.assertEqual(calls[0], calls[1])
            sleep.assert_called_once_with(0.05)
            self.assertEqual(verify_bundle(bundle)["bundle_id"], bundle.name)
            self.assertEqual(len(list(Path(temp).iterdir())), 1)

    def test_permanent_windows_denial_is_bounded_and_retains_verified_staging(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            with patch.object(archive.os, "rename", side_effect=self.windows_access_denied()) as rename, patch.object(archive.time, "sleep") as sleep:
                with self.assertRaises(PermissionError):
                    publish_bundle(temp, {"events.jsonl": []}, scope=SCOPE,
                        utc_range=RANGE, quality=COMPLETE, created_at_utc=END)
            self.assertEqual(rename.call_count, 4)
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.05, 0.15, 0.3])
            staging = list(Path(temp).glob("publishing-*"))
            self.assertEqual(len(staging), 1)
            verify_bundle(staging[0])
            self.assertEqual(len(list(Path(temp).iterdir())), 1)

    def test_changed_staging_after_windows_denial_cannot_publish(self):
        def corrupt(source, target):
            (source / "events.jsonl").write_bytes(b"{}\n")
            raise self.windows_access_denied()
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            with patch.object(archive.os, "rename", side_effect=corrupt) as rename, patch.object(archive.time, "sleep") as sleep:
                with self.assertRaises(ResearchError):
                    publish_bundle(temp, {"events.jsonl": []}, scope=SCOPE,
                        utc_range=RANGE, quality=COMPLETE, created_at_utc=END)
            self.assertEqual(rename.call_count, 1)
            sleep.assert_not_called()
            self.assertEqual(len(list(Path(temp).glob("publishing-*"))), 1)

    def test_windows_denial_with_conflicting_destination_never_retries_or_adopts(self):
        def conflict(source, target):
            shutil.copytree(source, target)
            (target / "events.jsonl").write_bytes(b"{}\n")
            raise self.windows_access_denied()
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            with patch.object(archive.os, "rename", side_effect=conflict) as rename, patch.object(archive.time, "sleep") as sleep:
                with self.assertRaises(ResearchError):
                    publish_bundle(temp, {"events.jsonl": []}, scope=SCOPE,
                        utc_range=RANGE, quality=COMPLETE, created_at_utc=END)
            self.assertEqual(rename.call_count, 1)
            sleep.assert_not_called()

    def test_staging_mutation_during_windows_backoff_is_rejected_before_next_rename(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            def mutate_during_backoff(_):
                staging = next(Path(temp).glob("publishing-*"))
                (staging / "events.jsonl").write_bytes(b"{}\n")
            with patch.object(archive.os, "rename", side_effect=self.windows_access_denied()) as rename, patch.object(archive.time, "sleep", side_effect=mutate_during_backoff):
                with self.assertRaises(ResearchError):
                    publish_bundle(temp, {"events.jsonl": []}, scope=SCOPE,
                        utc_range=RANGE, quality=COMPLETE, created_at_utc=END)
            self.assertEqual(rename.call_count, 1)
            self.assertEqual(len(list(Path(temp).glob("publishing-*"))), 1)

    def test_bundle_integrity_idempotency_and_no_path_escape(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            args = {"scope": SCOPE, "utc_range": RANGE, "quality": COMPLETE, "created_at_utc": END}
            bundle = publish_bundle(temp, {"events.jsonl": [event("x", "0")]}, **args)
            self.assertEqual(bundle, publish_bundle(temp, {"events.jsonl": [event("x", "0")]}, **args))
            self.assertEqual(verify_bundle(bundle)["bundle_id"], bundle.name)
            self.assertEqual(len(read_member(bundle, "events.jsonl")), 1)
            with self.assertRaises(ResearchError):
                publish_bundle(temp, {"../escape.json": {}}, **args)
            (bundle / "events.jsonl").write_text("{}\n")
            with self.assertRaises(ResearchError):
                verify_bundle(bundle)

    def test_concurrent_directory_publication_errors_adopt_only_identical_verified_bundle(self):
        for exception in (PermissionError("simulated Windows destination exists"), OSError("simulated Linux ENOTEMPTY")):
            with self.subTest(error=type(exception).__name__), tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
                def competing_publication(source, target):
                    shutil.copytree(source, target)
                    raise exception
                with patch.object(archive.os, "rename", side_effect=competing_publication):
                    bundle = publish_bundle(temp, {"events.jsonl": [event("x", "0")]}, scope=SCOPE,
                        utc_range=RANGE, quality=COMPLETE, created_at_utc=END)
                self.assertEqual(verify_bundle(bundle)["bundle_id"], bundle.name)
                self.assertEqual(len(list(Path(temp).glob("publishing-*"))), 1, "Race evidence is retained")
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp:
            with patch.object(archive.os, "rename", side_effect=PermissionError("no competing final")):
                with self.assertRaises(PermissionError):
                    publish_bundle(temp, {"events.jsonl": []}, scope=SCOPE,
                        utc_range=RANGE, quality=COMPLETE, created_at_utc=END)

    def test_three_comparisons_no_synthetic_real_promotion(self):
        candidate = calculate([], snapshot(), snapshot(at=END))
        original = deepcopy(candidate)
        original["lane"] = "public-benchmark"
        result = build_comparison(candidate, original, baseline=candidate)
        self.assertIsNotNone(result["candidate_vs_baseline"])
        self.assertEqual(result["matched_cohort_status"], "NO_VERIFIED_MATCHED_COHORTS")
        self.assertIn("DIFFERENT_EXECUTION_EVIDENCE", result["whole_account"]["reasons"])
        self.assertFalse(result["whole_account"]["outperformance_claim"])
        self.assertFalse(result["candidate_vs_baseline"]["promotion_allowed"])
        self.assertIsNone(result["whole_account"]["observed_dollar_difference_pico"])

    def test_different_catchup_windows_withhold_numeric_gap(self):
        candidate = calculate([], snapshot(), snapshot(at=END))
        original = deepcopy(candidate)
        original["utc_range"]["start"] = "2026-10-05T00:00:00Z"
        for item in (candidate, original):
            item["net_pnl_interval_pico"] = {"lower": "0", "upper": "100"}
        result = build_comparison(candidate, original)["whole_account"]
        self.assertIn("DIFFERENT_UTC_WINDOWS", result["reasons"])
        self.assertIsNone(result["observed_dollar_difference_pico"])
        self.assertIsNone(result["difference_interval_pico"])

    def test_registry_cannot_silently_change_profile_identity(self):
        registry = load_registry()
        self.assertEqual(len(registry["accounts"]), 2)
        registry["accounts"][0]["profile_url"] += "?fake"
        with self.assertRaises(ValueError):
            validate_registry(registry)


if __name__ == "__main__":
    unittest.main()
