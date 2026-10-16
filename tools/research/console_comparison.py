"""Publish a bounded public comparison view from a verified observer bundle.

No network, engine commands or delivery. A refresher may atomically publish a
current.json pointer only after this immutable bundle has been verified.
"""
import argparse
from pathlib import Path
import re

from .accounting import account_bundle
from .archive import publish_bundle, read_member, verify_bundle
from .registry import DEFAULT_ACCOUNTS
from .observe_public_v2 import empty_reported_pnl, project_reported_pnl
from .schema import ResearchError, canonical, digest, integer, quality, utc

MAX_VIEW_BYTES = 524_288


def decimal(value, scale=12):
    amount = integer(value, signed=True, nullable=True)
    if amount is None:
        return None
    sign = "-" if amount < 0 else ""
    whole, fraction = divmod(abs(amount), 10**scale)
    return sign + str(whole) + (("." + str(fraction).zfill(scale).rstrip("0")) if fraction else "")


def metric(value=None, scope="observed public-wallet source period", reason=None, unit="USD"):
    return {"value": value, "unit": unit, "scope": scope, "reason": reason}


def epoch(value):
    return int(utc(value).timestamp() * 1000)


def period_summary(events, first_ms, last_ms, coverage, complete=False, accounting=None):
    selected = [event for event in events if first_ms <= epoch(event["event_time"]) < last_ms]
    turnover, turnover_known = 0, True
    counts = {key: 0 for key in ("buy_count", "sell_count", "trade_count", "merge_count", "redeem_count")}
    for event in selected:
        kind = event["event_type"]
        if kind == "TRADE":
            counts["trade_count"] += 1
            side = event.get("side")
            if side in {"BUY", "SELL"}:
                counts[side.lower() + "_count"] += 1
            amount = integer(event.get("amounts", {}).get("cash_reported_pico"), nullable=True)
            if amount is not None:
                turnover += amount
            else:
                turnover_known = False
        elif kind in {"MERGE", "REDEEM"}:
            counts[kind.lower() + "_count"] += 1
    return {"period": {"start_ms": first_ms, "end_ms": last_ms},
            "activity_summary": {key: str(value) for key, value in counts.items()},
            "observed_events": str(len(selected)), "turnover_usd": decimal(str(turnover)) if turnover_known else None,
            "financial_complete": complete,
            "net_pnl_usd": decimal(accounting.get("net_pnl_pico")) if complete and accounting else None,
            "actions_complete": coverage.get("activity_complete") is True,
            "observed_actions": len(selected)}


def build_reference(observer_bundle):
    manifest = verify_bundle(observer_bundle)
    account = DEFAULT_ACCOUNTS["mo-money"]["account_id"]
    scope = manifest.get("scope", {})
    if (scope.get("kind") != "one-public-account" or scope.get("lane") != "public-benchmark"
            or scope.get("account_id") != account or scope.get("chain_id") != 137):
        raise ResearchError("verified mo-money public observer bundle required")
    names = {item["path"] for item in manifest["files"]}
    if not {"events.jsonl", "closing.json", "coverage.json"}.issubset(names):
        raise ResearchError("observer activity, positions and coverage required")
    events = read_member(observer_bundle, "events.jsonl")
    closing = read_member(observer_bundle, "closing.json")
    coverage = read_member(observer_bundle, "coverage.json")
    accounting = account_bundle(observer_bundle)
    if accounting.get("account_id") != account or closing.get("account_id") != account:
        raise ResearchError("public account identity mismatch")
    complete = (accounting.get("quality", {}).get("status") == "COMPLETE"
                and accounting.get("quality", {}).get("eligibility", {}).get("accounting") is True)
    events = sorted(events, key=lambda event: (event["event_time"], event["event_id"]))
    for event in events:
        if event.get("account_id") != account or event.get("lane") != "public-benchmark":
            raise ResearchError("mixed public activity identity")
        if not utc(manifest["utc_range"]["start"]) <= utc(event["event_time"]) < utc(manifest["utc_range"]["end"]):
            raise ResearchError("public action outside source period")
    # Public names/prices are report-only metadata, never execution or identity.
    source_rows = read_member(observer_bundle, "source-activity.jsonl") if "source-activity.jsonl" in names else []
    by_hash = {digest(item["row"]): item for item in source_rows if isinstance(item, dict) and isinstance(item.get("row"), dict)}
    actions = []
    for event in events[-256:]:
        if event.get("account_id") != account or event.get("lane") != "public-benchmark":
            raise ResearchError("mixed public activity identity")
        if not utc(manifest["utc_range"]["start"]) <= utc(event["event_time"]) < utc(manifest["utc_range"]["end"]):
            raise ResearchError("public action outside source period")
        amounts = event.get("amounts", {})
        deltas = event.get("effects", {}).get("position_deltas", [])
        position = deltas[0] if len(deltas) == 1 else {}
        source_item = by_hash.get(event.get("source", {}).get("record_hash"), {})
        title = source_item.get("display_title", source_item.get("row", {}).get("title"))
        title = title[:128] if isinstance(title, str) and all(ord(char) >= 32 for char in title) else None
        price = event.get("reported_price_decimal")
        # A public average price is report metadata, never a native tick/order.
        price = price if isinstance(price, str) and re.fullmatch(r"(?:0|1|0\.[0-9]{1,12}|1\.0{1,12})", price) else None
        actions.append({"id": event["event_id"], "at_ms": epoch(event["event_time"]), "kind": event["event_type"],
            "market": position.get("condition_id"), "side": event.get("side"), "outcome": position.get("outcome"),
            "quantity_shares": decimal(amounts.get("quantity_micros"), 6),
            "amount_usd": decimal(amounts.get("cash_reported_pico")), "source": "public-observation",
            "title": title, "price_usd": price, "order_id": None})
    raw_positions = closing.get("positions", [])
    if not isinstance(raw_positions, list):
        raise ResearchError("invalid public position snapshot")
    positions = [{"market": row.get("condition_id"), "outcome": row.get("outcome"),
                  "quantity_shares": decimal(row.get("quantity_micros"), 6),
                  "value_usd": decimal(row.get("valuation", {}).get("lower_pico"))}
                 for row in raw_positions[:64]]
    components = accounting.get("components", {})
    costs = components.get("cost_evidence_coverage", {})
    bound_known = (costs.get("source_window_complete") is True and costs.get("missing_cost_bound") == 0
                   and costs.get("cost_bound_records") == accounting["event_count"])
    metrics = {key: metric(reason="PUBLIC_CASH_UNKNOWN") for key in ("cash_usd", "available_usd", "reserved_usd", "nav_usd")}
    metrics.update(net_pnl_usd=metric(decimal(accounting.get("net_pnl_pico")) if complete else None,
                       reason="PUBLIC_WALLET_NOT_BOT_ATTRIBUTED" if complete else "INCOMPLETE_ACCOUNTING"),
        equity_change_usd=metric(reason="INCOMPLETE_ACCOUNTING"),
        turnover_usd=metric(decimal(components.get("reported_trade_turnover_pico")), reason="PUBLIC_ACTIVITY_NOT_AUTHENTICATED_FILLS"),
        observed_events=metric(str(accounting["event_count"]), unit="count", reason="PUBLIC_ACTIVITY_NOT_AUTHENTICATED_FILLS"),
        cost_bound_usd=metric(decimal(components.get("conservative_cost_bound_pico")) if bound_known else None,
                             reason="DECLARED_EVENT_COSTS_ONLY" if bound_known else "COST_BOUND_COVERAGE_UNPROVEN"))
    known_values = [integer(row.get("valuation", {}).get("lower_pico"), nullable=True) for row in raw_positions]
    metrics["position_value_usd"] = metric(decimal(str(sum(value for value in known_values if value is not None))) if raw_positions and any(value is not None for value in known_values) else None,
        scope="observed public-reference position value subtotal; excludes cash", reason="PUBLIC_REFERENCE_VALUATION")
    warnings = ["PUBLIC_WALLET_NOT_BOT_ATTRIBUTED", "PUBLIC_ACTIVITY_NOT_AUTHENTICATED_FILLS", "PUBLIC_REFERENCE_VALUATION", "PUBLIC_CASH_UNKNOWN"]
    if len(events) > len(actions):
        warnings.append("ACTIONS_TRUNCATED")
    if len(raw_positions) > len(positions):
        warnings.append("POSITIONS_TRUNCATED")
    source_reasons = manifest.get("quality", {}).get("reasons", [])
    for reason in ("PROFILE_ATTRIBUTION_REVERIFY_REQUIRED", "V2_DEFAULT_FEED_EXCLUDES_OPT_IN_TIPS",
                   "V2_OPEN_POSITIONS_NOT_COMPLETE_WALLET_LIFECYCLE"):
        if reason in source_reasons:
            warnings.append(reason)
    first_ms, last_ms = epoch(manifest["utc_range"]["start"]), epoch(manifest["utc_range"]["end"])
    periods = {}
    for key, duration in (("1h", 3_600_000), ("6h", 21_600_000), ("24h", 86_400_000), ("all", None)):
        period_first = max(first_ms, last_ms - duration) if duration is not None else first_ms
        periods[key] = period_summary(events, period_first, last_ms, coverage,
                                      complete=complete and period_first == first_ms, accounting=accounting)
        periods[key]["source_period_shorter"] = duration is not None and last_ms - first_ms < duration
    view = {"schema_version": 1, "account_id": account, "source_bundle_id": manifest["bundle_id"],
            "source_at_utc": manifest["created_at_utc"], "coverage": {"actions_complete": coverage.get("activity_complete") is True and len(events) == len(actions),
                "financial_complete": complete, "reason": "PUBLIC_ACTIVITY_NOT_AUTHENTICATED_FILLS", "observed_actions": len(events),
                "omitted_actions": len(events) - len(actions)}, "metrics": metrics, "actions": actions, "positions": positions,
            "periods": periods, "warnings": warnings}
    reported = project_reported_pnl(read_member(observer_bundle, "source-reported-pnl.json"), account) if "source-reported-pnl.json" in names else empty_reported_pnl(account)
    if reported["source_at_ms"] is not None and reported["source_at_ms"] > epoch(manifest["created_at_utc"]):
        reported = empty_reported_pnl(account, "INVALID", "PNL_SOURCE_INVALID")
    if reported["status"] == "AVAILABLE" and reported["period"] != {"start_ms": first_ms, "end_ms": last_ms}:
        reported["warnings"].append("PNL_WINDOW_DIFFERS_FROM_ACTIVITY")
    view["reported_performance"] = reported
    if len(canonical(view)) + 1 > MAX_VIEW_BYTES:
        raise ResearchError("comparison view byte quota exceeded")
    return view, manifest


def publish_reference(observer_bundle, output_root, *, source_revision="unknown", created_at_utc=None):
    view, manifest = build_reference(observer_bundle)
    created = created_at_utc or manifest["created_at_utc"]
    if utc(created) < utc(manifest["created_at_utc"]):
        raise ResearchError("view publication predates observer")
    return publish_bundle(Path(output_root), {"view.json": view},
        scope={"kind": "console-comparison-reference", "lane": "public-benchmark", "account_id": view["account_id"]},
        utc_range=manifest["utc_range"], quality=quality("PARTIAL", ["PUBLIC_WALLET_NOT_BOT_ATTRIBUTED"]),
        source_revision=source_revision, created_at_utc=created)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observer-bundle", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--source-revision", default="unknown")
    args = parser.parse_args()
    path = publish_reference(args.observer_bundle, args.out, source_revision=args.source_revision)
    print(canonical({"bundle": str(path), "bundle_id": path.name, "read_only": True, "messages_sent": False}).decode())


if __name__ == "__main__":
    main()
