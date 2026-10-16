"""Exact roll-forward, NAV intervals and explicit unknown economics, outside trading.

Reservations remain part of cash/NAV. Fee-inclusive cash effects are never charged
twice. Public reference values and conservative cost bounds are not actual profit.
"""
import argparse
from decimal import Decimal, localcontext
from pathlib import Path
from .archive import publish_bundle, read_member, verify_bundle
from .reconcile import compare_positions, journal_coverage_gate, reconcile_events
from .schema import (VERSION, PICO, ResearchError, address, canonical, digest, integer, load_json,
                     quality, utc, validate_snapshot, window)


def snapshot_nav(snapshot):
    if snapshot is None:
        return {"lower_pico": None, "upper_pico": None, "actual_pico": None,
                "reasons": ["SNAPSHOT_MISSING"], "positions": 0, "valued_positions": 0}
    validate_snapshot(snapshot)
    cash = integer(snapshot.get("cash_pico"), nullable=True)
    reasons = []
    if cash is None:
        reasons.append("CASH_UNKNOWN")
    if snapshot.get("positions_complete") is not True:
        reasons.append("POSITION_COVERAGE_INCOMPLETE")
    lower, upper = cash, cash
    actual = snapshot.get("financial_complete") is True and not reasons
    valued, unknown_assets, conditional_assets = 0, [], []
    for position in snapshot["positions"]:
        if integer(position["quantity_micros"]) == 0:
            continue
        valuation = position.get("valuation") or {}
        lo = integer(valuation.get("lower_pico"), nullable=True)
        hi = integer(valuation.get("upper_pico"), nullable=True)
        valuation_at = valuation.get("at_utc")
        if valuation_at is None or utc(valuation_at) != utc(snapshot["at_utc"]):
            # A quote valid at another time cannot value this boundary. A payout
            # claim may be carried forward only by an explicit new as-of mark.
            reasons.append("POSITION_VALUATION_NOT_AT_SNAPSHOT_BOUNDARY")
            lo = hi = None
        if lo is None or hi is None:
            reasons.append("POSITION_VALUE_UNKNOWN")
            unknown_assets.append(position["asset_id"])
            lower = upper = None
            actual = False
            continue
        valued += 1
        if lower is not None:
            lower += lo
        if upper is not None:
            upper += hi
        if (valuation.get("complete") is not True or valuation.get("kind") not in {"executable_net", "verified_payout"}
                or lo != hi):
            actual = False
            reasons.append("CONDITIONAL_POSITION_VALUATION")
            conditional_assets.append(position["asset_id"])
        # A payable claim replaces token value; there is no separate additive claim field.
    if snapshot.get("positions_complete") is not True:
        lower = upper = None
    return {"lower_pico": str(lower) if lower is not None else None,
            "upper_pico": str(upper) if upper is not None else None,
            "actual_pico": str(lower) if actual and lower == upper and lower is not None else None,
            "reasons": sorted(set(reasons)), "positions": len(snapshot["positions"]), "valued_positions": valued,
            "valuation_details": {"unknown_count": len(unknown_assets), "unknown_examples": unknown_assets[:32],
                                  "conditional_count": len(conditional_assets), "conditional_examples": conditional_assets[:32]}}


def _quantity_map(snapshot):
    if snapshot is None:
        return {}
    return {position["asset_id"]: integer(position["quantity_micros"]) for position in snapshot["positions"]}


def calculate_account(events, opening=None, closing=None, *, utc_range, scope, source_quality=None, journal_coverage=None):
    window(**utc_range)
    account_id = address(scope["account_id"])
    lane = scope.get("lane")
    if opening is not None:
        validate_snapshot(opening)
        if address(opening["account_id"]) != account_id or opening["lane"] != lane:
            raise ResearchError("opening snapshot scope differs")
        if utc(opening["at_utc"]) != utc(utc_range["start"]):
            raise ResearchError("opening must match window boundary")
    if closing is not None:
        validate_snapshot(closing)
        if address(closing["account_id"]) != account_id or closing["lane"] != lane:
            raise ResearchError("closing snapshot scope differs")
    reconciled = reconcile_events(events, account_id, utc_range)
    events = reconciled["events"]
    if any(event["lane"] != lane for event in events):
        raise ResearchError("paper/live/public lanes cannot mix")
    reasons = list(reconciled["quality"]["reasons"])
    source_complete = ((source_quality or {}).get("status") == "COMPLETE"
                       and (source_quality or {}).get("eligibility", {}).get("accounting") is True)
    if not source_complete:
        reasons.append("SOURCE_FINANCIAL_COMPLETENESS_UNPROVEN")
        reasons.extend((source_quality or {}).get("reasons", []))
    journal_complete = True
    if journal_coverage is not None:
        journal_gate = journal_coverage_gate(journal_coverage)
        reasons.extend(journal_gate["reasons"])
        journal_complete = journal_gate["eligibility"]["accounting"] is True
    if opening is None:
        reasons.append("OPENING_STATE_UNKNOWN")
    elif opening.get("financial_complete") is not True:
        reasons.append("OPENING_STATE_UNVERIFIED")
    if closing is None:
        reasons.append("CLOSING_STATE_UNKNOWN")
    else:
        if utc(closing["at_utc"]) != utc(utc_range["end"]):
            reasons.append("CLOSING_SNAPSHOT_NOT_AT_WINDOW_BOUNDARY")
        if closing.get("financial_complete") is not True:
            reasons.append("CLOSING_STATE_UNVERIFIED")
    cash = integer(opening.get("cash_pico"), nullable=True) if opening else None
    positions = _quantity_map(opening)
    positions_known = opening is not None and opening.get("positions_complete") is True
    external, external_known, reported_cash = 0, True, 0
    actual_cost, bound_cost, actual_fee, reported_returns, turnover = 0, 0, 0, 0, 0
    cost_missing, fee_missing, bound_missing, trade_events = 0, 0, 0, 0
    asset_conditions = {p["asset_id"]: p["condition_id"] for p in opening["positions"]} if opening else {}
    conflicts = []
    for event in events:
        effects, amounts = event.get("effects", {}), event.get("amounts", {})
        delta = integer(effects.get("cash_delta_pico"), signed=True, nullable=True)
        contribution = integer(effects.get("external_contribution_pico"), signed=True, nullable=True)
        if delta is None:
            cash = None
        else:
            reported_cash += delta
            if cash is not None:
                cash += delta
        if contribution is None:
            external_known = False
        else:
            external += contribution
        if event["event_time"] is None:
            reasons.append("FINANCIAL_OCCURRED_TIME_UNKNOWN")
        for change in effects.get("position_deltas", []):
            asset, market = change["asset_id"], change["condition_id"]
            if asset in asset_conditions and asset_conditions[asset] != market:
                conflicts.append("ASSET_CONDITION_CONFLICT")
            asset_conditions[asset] = market
            positions[asset] = positions.get(asset, 0) + integer(change["quantity_delta_micros"], signed=True)
        actual_cost += integer(amounts.get("cost_actual_pico"), nullable=True) or 0
        bound_cost += integer(amounts.get("cost_bound_pico"), nullable=True) or 0
        # An absent declaration is unknown applicability/cost, including closeout
        # and non-trade events. Explicit zero is evidence; an empty subtotal is not.
        bound_missing += amounts.get("cost_bound_pico") is None
        actual_fee += integer(amounts.get("fee_actual_pico"), nullable=True) or 0
        cost_missing += amounts.get("cost_actual_pico") is None
        fee_missing += amounts.get("fee_actual_pico") is None
        reported = integer(amounts.get("cash_reported_pico"), nullable=True)
        if event["event_type"] == "TRADE":
            trade_events += 1
            if reported is not None:
                turnover += reported
        if event["event_type"] in {"MERGE", "REDEEM", "REWARD", "MAKER_REBATE", "TAKER_REBATE", "YIELD"} and delta is not None:
            reported_returns += delta
    # Aggregate window conservation does not invent FIFO inside coarse/tied source clocks.
    # Prefix solvency/queue timing needs a separately proven native event order.
    if cash is not None and cash < 0:
        conflicts.append("NEGATIVE_ROLL_FORWARD_CASH")
    if positions_known and any(value < 0 for value in positions.values()):
        conflicts.append("NEGATIVE_ROLL_FORWARD_POSITION")
    differences = []
    if opening and closing and positions_known and closing.get("positions_complete") is True:
        differences = compare_positions(positions, _quantity_map(closing))
        if differences:
            conflicts.append("POSITION_RECONCILIATION_DELTA")
    observed_cash = integer(closing.get("cash_pico"), nullable=True) if closing else None
    if cash is not None and observed_cash is not None and cash != observed_cash:
        conflicts.append("CASH_RECONCILIATION_DELTA")
    start_nav, end_nav = snapshot_nav(opening), snapshot_nav(closing)
    reasons.extend(start_nav["reasons"])
    reasons.extend(end_nav["reasons"])
    if not external_known:
        reasons.append("EXTERNAL_CONTRIBUTIONS_UNKNOWN")
    is_conflicted = bool(conflicts) or reconciled["quality"]["status"] == "CONFLICTED"
    reasons.extend(conflicts)
    complete = not reasons and not is_conflicted
    interval, exact, roi = None, None, None
    # A complete source/window is required even for an interval: unseen effects have no finite bound.
    financial_window_complete = (source_complete and journal_complete
        and reconciled["quality"]["eligibility"]["accounting"] is True
        and opening is not None and closing is not None
        and utc(closing["at_utc"]) == utc(utc_range["end"]))
    if external_known and financial_window_complete and not is_conflicted:
        lo0, hi0 = start_nav["lower_pico"], start_nav["upper_pico"]
        lo1, hi1 = end_nav["lower_pico"], end_nav["upper_pico"]
        if all(value is not None for value in (lo0, hi0, lo1, hi1)):
            interval = {"lower": str(int(lo1) - int(hi0) - external), "upper": str(int(hi1) - int(lo0) - external)}
    if complete and start_nav["actual_pico"] is not None and end_nav["actual_pico"] is not None:
        exact = str(int(end_nav["actual_pico"]) - int(start_nav["actual_pico"]) - external)
        # Window return is not TWR; only no external flows permits this simple ratio.
        any_flows = any(integer(event["effects"].get("external_contribution_pico"), signed=True, nullable=True) not in (None, 0) for event in events)
        if not any_flows and int(start_nav["actual_pico"]) > 0:
            with localcontext() as context:
                context.prec = 60
                roi = str(Decimal(exact) / Decimal(start_nav["actual_pico"]))
    result = {"schema_version": VERSION, "account_id": account_id, "lane": lane, "utc_range": utc_range,
        "scope": scope, "quality": quality("CONFLICTED" if is_conflicted else "COMPLETE" if complete else "PARTIAL", reasons,
            accounting=complete), "inputs_hash": digest({"events": events, "opening": opening, "closing": closing,
                                                        "source_quality": source_quality, "journal_coverage": journal_coverage}),
        "event_count": len(events), "duplicates": reconciled["duplicates"],
        "cash_roll_forward": {"expected_pico": str(cash) if cash is not None else None, "observed_pico": str(observed_cash) if observed_cash is not None else None,
                              "reported_delta_pico": str(reported_cash)},
        "position_differences": differences, "retained_positions": closing["positions"] if closing else [],
        "nav": {"opening": start_nav, "closing": end_nav},
        "external_contributions_pico": str(external) if external_known else None,
        "net_pnl_pico": exact, "net_pnl_interval_pico": interval,
        "roi": roi, "roi_kind": "window-no-external-flows" if roi is not None else None,
        "twr": None, "twr_reason": "REQUIRES_COMPLETE_VALUATIONS_AROUND_EVERY_EXTERNAL_FLOW",
        "components": {"observed_actual_cost_pico": str(actual_cost),
            "observed_cost_bound_pico": str(bound_cost),
            "conservative_cost_bound_pico": str(bound_cost) if financial_window_complete and not is_conflicted and not bound_missing else None,
            "observed_actual_fees_pico": str(actual_fee), "reported_returns_pico": str(reported_returns),
            "reported_trade_turnover_pico": str(turnover), "realized_pnl_pico": None,
            "actual_cost_total_pico": str(actual_cost) if financial_window_complete and not is_conflicted and not cost_missing else None,
            "actual_fees_total_pico": str(actual_fee) if financial_window_complete and not is_conflicted and not fee_missing else None,
            "cost_evidence_coverage": {"trade_events": trade_events, "missing_actual_cost": cost_missing,
                                       "missing_actual_fee": fee_missing, "actual_cost_records": len(events),
                                       "cost_bound_records": len(events), "missing_cost_bound": bound_missing,
                                       "source_window_complete": financial_window_complete and not is_conflicted,
                                       "bound_scope": "declared event costs; unreported categories are not inferred"}},
        "cost_semantics": "Actual/upper-bound/report-only costs are separate; cash effects already include costs. No second fee deduction.",
        "financial_claim": "synthetic" if lane in {"public-paper", "offline-emulator"} else "observed" if exact is not None else "unverified"}
    return result


def account_bundle(bundle, opening_path=None, closing_path=None, coverage_path=None):
    manifest = verify_bundle(bundle)
    names = {f["path"] for f in manifest["files"]}
    opening = load_json(opening_path) if opening_path else read_member(bundle, "opening.json") if "opening.json" in names else None
    closing = load_json(closing_path) if closing_path else read_member(bundle, "closing.json") if "closing.json" in names else None
    coverage = load_json(coverage_path) if coverage_path else read_member(bundle, "journal-coverage.json") if "journal-coverage.json" in names else None
    result = calculate_account(read_member(bundle, "events.jsonl"), opening, closing, utc_range=manifest["utc_range"],
                               scope=manifest["scope"], source_quality=manifest["quality"], journal_coverage=coverage)
    result["source_bundle_id"] = manifest["bundle_id"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--opening")
    parser.add_argument("--closing")
    parser.add_argument("--journal-coverage", help="safe exporter field-coverage object; cannot supply fills")
    parser.add_argument("--out", help="publish immutable accounting bundle")
    args = parser.parse_args()
    result = account_bundle(args.bundle, args.opening, args.closing, args.journal_coverage)
    if args.out:
        output = publish_bundle(args.out, {"accounting.json": result}, scope=result["scope"],
                                utc_range=result["utc_range"], quality=result["quality"], config_hash=result["inputs_hash"])
        print(canonical({"bundle": str(output), "quality": result["quality"]}).decode())
    else:
        print(canonical(result).decode())


if __name__ == "__main__":
    main()
