"""Bounded read-only comparison; a browser request never calls an engine or venue."""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from .observability import execution_summary
from .telegram_reports import deposit_value, holdings, valuation_boot, valuation_context

REFERENCE_ACCOUNT = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"
MAX_VIEW_BYTES = 524_288
PERIODS = {"1h": 3_600_000, "6h": 21_600_000, "24h": 86_400_000, "all": None}
METRICS = ("cash_usd", "available_usd", "reserved_usd", "nav_usd", "net_pnl_usd",
           "equity_change_usd", "turnover_usd", "observed_events", "cost_bound_usd", "position_value_usd")


def obj(value):
    return value if isinstance(value, dict) else {}


def uint(value):
    if type(value) is int and 0 <= value < 2**128:
        return value
    if isinstance(value, str) and re.fullmatch(r"(?:0|[1-9][0-9]{0,38})", value):
        amount = int(value)
        return amount if amount < 2**128 else None
    return None


def decimal(value, scale=12):
    """Exact finite decimal text, including a negative observed NAV change."""
    if type(value) is not int or abs(value) >= 10**40:
        return None
    sign = "-" if value < 0 else ""
    whole, fraction = divmod(abs(value), 10**scale)
    suffix = ("." + str(fraction).zfill(scale).rstrip("0")) if fraction else ""
    return sign + str(whole) + suffix


def metric(value=None, *, scope, reason=None, unit="USD"):
    return {"value": value, "unit": unit, "scope": scope, "reason": reason}


def empty_lane(name, lane, status, reason):
    return {"name": name, "lane": lane, "status": status, "mode": None, "account_id": None,
            "period": {"start_ms": None, "end_ms": None},
            "freshness": {"observed_at_ms": None, "age_ms": None, "fresh": False},
            "source_refs": [], "coverage": {"actions_complete": False, "financial_complete": False,
                "reason": reason, "observed_actions": 0, "omitted_actions": None},
            "metrics": {key: metric(scope=lane, reason=reason, unit="count" if key == "observed_events" else "USD")
                        for key in METRICS}, "actions": [], "positions": [], "equity": [], "activity_summary": {}, "warnings": [reason]}


def stamp(value):
    if not isinstance(value, str) or len(value) > 40 or not value.endswith("Z"):
        raise ValueError("invalid UTC clock")
    parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    if parsed.utcoffset().total_seconds() != 0:
        raise ValueError("invalid UTC clock")
    return int(parsed.timestamp() * 1000)


def _read_json(path, maximum):
    if path.is_symlink() or not path.is_file():
        raise ValueError("invalid artifact member")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("artifact too large")
    def unique(pairs):
        if len({key for key, _ in pairs}) != len(pairs):
            raise ValueError("duplicate JSON field")
        return dict(pairs)
    return raw, json.loads(raw, object_pairs_hook=unique,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                                     allow_nan=False).encode()).hexdigest()


def _text(value, maximum=128):
    return value if isinstance(value, str) and 0 < len(value) <= maximum and all(ord(c) >= 32 for c in value) else None


def _code(value):
    return value if isinstance(value, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", value) else None


def _amount(value):
    return value if isinstance(value, str) and re.fullmatch(r"-?(?:0|[1-9][0-9]{0,39})(?:\.[0-9]{1,12})?", value) else None


def load_reference(root, now_ms, start_ms, period="all"):
    result = empty_lane("mo-money", "public-benchmark", "UNAVAILABLE", "REFERENCE_NOT_PUBLISHED")
    root = Path(root)
    if not root.exists():
        return result
    try:
        if root.is_symlink() or not root.is_dir():
            raise ValueError("invalid root")
        if (root / "current.json").exists():
            _, pointer = _read_json(root / "current.json", 256)
            if not isinstance(pointer, dict) or set(pointer) != {"bundle_id"} or not re.fullmatch(r"[0-9a-f]{64}", str(pointer["bundle_id"])):
                raise ValueError("invalid pointer")
            root = root / pointer["bundle_id"]
        if root.is_symlink() or not root.is_dir():
            raise ValueError("invalid bundle")
        _, manifest = _read_json(root / "manifest.json", 16_384)
        if (not isinstance(manifest, dict) or manifest.get("schema_version") != 1
                or manifest.get("bundle_id") != _hash({k: v for k, v in manifest.items() if k != "bundle_id"})
                or manifest.get("scope") != {"kind": "console-comparison-reference", "lane": "public-benchmark", "account_id": REFERENCE_ACCOUNT}
                or len(manifest.get("files", [])) != 1 or manifest["files"][0].get("path") != "view.json"):
            raise ValueError("invalid manifest")
        raw, view = _read_json(root / "view.json", MAX_VIEW_BYTES)
        member = manifest["files"][0]
        if member.get("bytes") != len(raw) or member.get("sha256") != hashlib.sha256(raw).hexdigest():
            raise ValueError("member integrity mismatch")
        if set(p.name for p in root.iterdir()) != {"view.json", "manifest.json"}:
            raise ValueError("unexpected artifact member")
        published = stamp(manifest["created_at_utc"])
        first, last = stamp(manifest["utc_range"]["start"]), stamp(manifest["utc_range"]["end"])
        if not first < last <= published <= now_ms + 5000:
            raise ValueError("invalid observation clock")
        if (not isinstance(view, dict) or view.get("schema_version") != 1
                or view.get("account_id") != REFERENCE_ACCOUNT
                or type(view.get("actions")) is not list or len(view["actions"]) > 256
                or type(view.get("positions")) is not list or len(view["positions"]) > 64):
            raise ValueError("invalid public view")
        observed = stamp(view.get("source_at_utc"))
        source_id = view.get("source_bundle_id")
        if not first < last <= observed <= published or not isinstance(source_id, str) or not re.fullmatch(r"[0-9a-f]{64}", source_id):
            raise ValueError("invalid source provenance")
        result.update(status="AVAILABLE" if now_ms - observed <= 3_600_000 else "STALE", account_id=REFERENCE_ACCOUNT,
                      period={"start_ms": first, "end_ms": last},
                      freshness={"observed_at_ms": observed, "age_ms": max(0, now_ms - observed), "fresh": now_ms - observed <= 3_600_000})
        result["source_refs"] = [{"kind": "verified_local_reference_view", "id": manifest["bundle_id"], "sha256": hashlib.sha256(raw).hexdigest(), "at_ms": published},
                                 {"kind": "producer_verified_public_observer_bundle", "id": source_id, "sha256": None, "at_ms": observed}]
        coverage = obj(view.get("coverage"))
        result["coverage"] = {"actions_complete": coverage.get("actions_complete") is True,
            "financial_complete": coverage.get("financial_complete") is True,
            "reason": _code(coverage.get("reason")) or "PUBLIC_ACTIVITY_NOT_AUTHENTICATED_FILLS",
            "observed_actions": uint(coverage.get("observed_actions")) or 0,
            "omitted_actions": uint(coverage.get("omitted_actions"))}
        result["warnings"] = [code for code in view.get("warnings", [])[:16] if _code(code)] if isinstance(view.get("warnings"), list) else []
        if "PUBLIC_WALLET_NOT_BOT_ATTRIBUTED" not in result["warnings"]:
            result["warnings"].append("PUBLIC_WALLET_NOT_BOT_ATTRIBUTED")
        for key in METRICS:
            item = obj(obj(view.get("metrics")).get(key))
            value = _amount(item.get("value"))
            if key == "net_pnl_usd" and coverage.get("financial_complete") is not True:
                value = None
            result["metrics"][key] = metric(value, unit="count" if key == "observed_events" else "USD",
                scope=_text(item.get("scope")) or "public-wallet observed period", reason=_code(item.get("reason")))
        selected = obj(obj(view.get("periods")).get(period))
        if selected:
            selected_period = obj(selected.get("period"))
            selected_first, selected_last = uint(selected_period.get("start_ms")), uint(selected_period.get("end_ms"))
            if (selected_first is None or selected_last is None or not first <= selected_first < selected_last == last):
                raise ValueError("invalid selected source period")
            result["period"] = {"start_ms": selected_first, "end_ms": selected_last}
            selected_complete = (coverage.get("financial_complete") is True
                                 and selected.get("financial_complete") is True and selected_first == first)
            result["metrics"]["observed_events"] = metric(_amount(selected.get("observed_events")), unit="count",
                scope="selected observed source period", reason="PUBLIC_ACTIVITY_NOT_AUTHENTICATED_FILLS")
            result["metrics"]["turnover_usd"] = metric(_amount(selected.get("turnover_usd")),
                scope="selected observed source period", reason="PUBLIC_ACTIVITY_NOT_AUTHENTICATED_FILLS")
            result["metrics"]["net_pnl_usd"] = metric(_amount(selected.get("net_pnl_usd")) if selected_complete else None,
                scope="selected public-wallet source period", reason="PUBLIC_WALLET_NOT_BOT_ATTRIBUTED" if selected_complete else "INCOMPLETE_ACCOUNTING")
            if selected_first != first:
                result["metrics"]["cost_bound_usd"] = metric(scope="selected public-wallet source period", reason="COST_BOUND_COVERAGE_UNPROVEN")
            result["activity_summary"] = {key: str(uint(value)) for key, value in obj(selected.get("activity_summary")).items()
                if key in {"buy_count", "sell_count", "trade_count", "merge_count", "redeem_count"} and uint(value) is not None}
            result["coverage"]["financial_complete"] = selected_complete
            result["coverage"]["observed_actions"] = uint(selected.get("observed_actions")) or 0
            if selected.get("source_period_shorter") is True:
                result["warnings"].append("SOURCE_PERIOD_SHORTER_THAN_REQUESTED")
            if selected_last < now_ms:
                result["warnings"].append("SOURCE_CUTOFF_LAGS_REQUEST")
            start_ms = selected_first
        for row in view["actions"]:
            if not isinstance(row, dict) or _text(row.get("id")) is None:
                raise ValueError("invalid action")
            at = uint(row.get("at_ms"))
            if at is None or not first <= at < last:
                raise ValueError("action outside source period")
            if at > now_ms or (start_ms is not None and at < start_ms):
                continue
            result["actions"].append({"id": _text(row.get("id")), "at_ms": at, "kind": _text(row.get("kind"), 32) or "UNKNOWN",
                "market": _text(row.get("market")), "side": row.get("side") if row.get("side") in {"BUY", "SELL"} else None,
                "outcome": _text(row.get("outcome"), 64), "quantity_shares": _amount(row.get("quantity_shares")),
                "amount_usd": _amount(row.get("amount_usd")), "source": "public-observation",
                "title": _text(row.get("title")), "price_usd": _amount(row.get("price_usd")), "order_id": None})
        if selected:
            shown = len(result["actions"])
            observed_count = result["coverage"]["observed_actions"]
            if observed_count < shown:
                raise ValueError("inconsistent action coverage")
            result["coverage"]["omitted_actions"] = observed_count - shown
            result["coverage"]["actions_complete"] = selected.get("actions_complete") is True and observed_count == shown
            if observed_count == shown:
                result["warnings"] = [code for code in result["warnings"] if code != "ACTIONS_TRUNCATED"]
        for row in view["positions"]:
            row = obj(row)
            result["positions"].append({"market": _text(row.get("market")), "outcome": _text(row.get("outcome"), 64),
                "quantity_shares": _amount(row.get("quantity_shares")), "value_usd": _amount(row.get("value_usd"))})
        # Period metrics belong to the complete source window, never a requested
        # subset. An empty intersection does not become a zero-activity period.
        if not selected and start_ms is not None and (first < start_ms or last <= start_ms):
            result["warnings"].append("REFERENCE_OUTSIDE_REQUESTED_PERIOD")
        if result["status"] == "STALE":
            result["warnings"].append("REFERENCE_SOURCE_STALE")
        result["actions"].reverse()
        return result
    except (OSError, ValueError, KeyError, TypeError, RecursionError, AttributeError):
        return empty_lane("mo-money", "public-benchmark", "INVALID", "REFERENCE_ARTIFACT_INVALID")


def own_lane(envelope, store, now_ms, start_ms):
    state = obj(envelope.get("state"))
    connected = envelope.get("connected") is True
    result = empty_lane("MM2", "simulation", "UNAVAILABLE", "NO_FRESH_ENGINE_SNAPSHOT")
    if not state:
        return result
    summary = execution_summary(state, connected)
    paper = summary["paper"]
    if not paper["selected"]:
        result["warnings"] = ["SIMULATION_NOT_SELECTED"]
        return result
    observed = uint(envelope.get("observed_at_ms"))
    result.update(status="AVAILABLE" if connected else "STALE", mode=summary["profile"] + "/" + (_text(state.get("mode"), 32) or "unknown"),
        freshness={"observed_at_ms": observed, "age_ms": max(0, now_ms - observed) if observed is not None else None, "fresh": connected})
    result["source_refs"] = [{"kind": "cached_engine_simulation", "id": "current-sample", "sha256": None, "at_ms": observed}]
    result["warnings"] = ["SIMULATION_SYNTHETIC_EXECUTION", "RECENT_AUDIT_NOT_COMPLETE_LEDGER", "CURRENT_SNAPSHOT_NOT_PERIOD_RESULT"]
    for key in ("turnover_usd", "cost_bound_usd", "position_value_usd"):
        result["metrics"][key] = metric(scope="simulation evidence", reason="FULL_SIMULATION_PERIOD_LEDGER_REQUIRED")
    for key in ("cash_usd", "available_usd", "reserved_usd"):
        result["metrics"][key] = metric(paper.get(key) if connected else None, scope="current simulation snapshot",
            reason="CURRENT_SNAPSHOT_NOT_PERIOD_RESULT" if connected else "NO_FRESH_ENGINE_SNAPSHOT")
    result["metrics"]["nav_usd"] = metric(decimal(deposit_value(state, "paper", now_ms)) if connected else None,
        scope="current synthetic best-bid NAV estimate", reason="SAMPLED_NAV_NOT_REALIZED_PROFIT")
    result["metrics"]["net_pnl_usd"] = metric(scope="simulation period result", reason="FULL_BOT_LEDGER_REQUIRED")
    simulation = obj(state.get("simulation"))
    audit = simulation.get("recent_audit")
    audit = audit[-256:] if isinstance(audit, list) else []
    now_sim = uint(simulation.get("now_ms"))
    wall_clock = now_sim is not None and observed is not None and abs(now_sim - observed) <= 60_000
    if not wall_clock:
        result["warnings"].append("SIMULATION_CLOCK_NOT_WALL_TIME")
    market = _text(state.get("simulation_market_condition"))
    epoch = uint(state.get("reset_epoch"))
    quotes, conflicting = {}, set()
    same_market = market is not None and obj(state.get("market")).get("condition_id") == market
    if simulation.get("simulated") is True and same_market:
        for name in ("venue_orders", "orders"):
            orders = simulation.get(name)
            for row in orders[:1024] if isinstance(orders, list) else []:
                row = obj(row)
                identity, quote = uint(row.get("id")), obj(row.get("quote"))
                price = uint(quote.get("price_micros"))
                if identity is None or quote.get("outcome") not in {"up", "down"} or price is None or not 0 < price < 1_000_000:
                    continue
                if identity in quotes and quotes[identity] != quote:
                    conflicting.add(identity)
                quotes[identity] = quote
        for identity in conflicting:
            quotes.pop(identity, None)
    for row in audit:
        row = obj(row)
        seq = uint(row.get("sequence"))
        if seq is None:
            continue
        at = uint(row.get("at_ms")) if wall_clock else None
        if at is not None and (at > now_ms or (start_ms is not None and at < start_ms)):
            continue
        order_id = uint(row.get("order_id"))
        quote = quotes.get(order_id, {})
        result["actions"].append({"id": f"paper:{epoch}:{market}:{seq}"[:128], "at_ms": at,
            "kind": _text(row.get("kind"), 32) or "unknown", "market": market, "side": "BUY" if quote else None,
            "outcome": _text(quote.get("outcome"), 64), "quantity_shares": decimal(uint(row.get("quantity_micros")), 6),
            "amount_usd": None, "source": "synthetic-recent-audit", "order_id": str(order_id) if order_id is not None else None,
            "title": _text(obj(state.get("market")).get("title")) if same_market else None,
            "price_usd": decimal(uint(quote.get("price_micros")), 6)})
    result["coverage"] = {"actions_complete": False, "financial_complete": False, "reason": "RECENT_AUDIT_NOT_COMPLETE_LEDGER",
        "observed_actions": len(result["actions"]), "omitted_actions": uint(simulation.get("audit_evicted"))}
    result["actions"].reverse()
    result["metrics"]["observed_events"] = metric(str(len(result["actions"])), unit="count", scope="retained recent simulation audit", reason="RECENT_AUDIT_NOT_COMPLETE_LEDGER")
    positions, complete = holdings(state, "paper")
    result["positions"] = [{"market": row["market"], "outcome": row["outcome"],
                            "quantity_shares": decimal(row["quantity"], 6), "value_usd": None}
                           for row in positions[:64]]
    if not complete or len(positions) > 64:
        result["warnings"].append("POSITIONS_TRUNCATED")
    try:
        points = store.equity_history(1440, now_ms)
    except sqlite3.Error:
        points = []
        result["warnings"].append("SECONDARY_EQUITY_UNAVAILABLE")
    points = [row for row in points if (start_ms is None or row["observed_at_ms"] >= start_ms)]
    result["equity"] = [{"at_ms": row["observed_at_ms"], "value_usd": decimal(uint(row.get("paper_pico"))),
                         "context": row["context"]} for row in points]
    context = valuation_context(state)
    if (valuation_boot(state) is not None and len(points) >= 2
            and all(row["context"] == context and uint(row.get("paper_pico")) is not None for row in points)
            and all(b["observed_at_ms"] - a["observed_at_ms"] <= 120_000 for a, b in zip(points, points[1:]))):
        result["metrics"]["equity_change_usd"] = metric(decimal(int(points[-1]["paper_pico"]) - int(points[0]["paper_pico"])),
            scope="change between retained same-context sampled NAV estimates", reason="SAMPLED_NAV_NOT_REALIZED_PROFIT")
    else:
        result["metrics"]["equity_change_usd"] = metric(scope="retained sampled NAV estimates", reason="EQUITY_CONTINUITY_UNPROVEN")
    result["period"] = {"start_ms": points[0]["observed_at_ms"] if points else None, "end_ms": points[-1]["observed_at_ms"] if points else None}
    return result


def comparison(envelope, store, root, now_ms, period="1h"):
    if period not in PERIODS:
        raise ValueError("unsupported comparison period")
    start = now_ms - PERIODS[period] if PERIODS[period] is not None else None
    own = own_lane(envelope, store, now_ms, start)
    reference = load_reference(root, now_ms, start, period)
    return {"schema_version": 1, "generated_at_ms": now_ms, "requested_period": period,
            "period": {"start_ms": start, "end_ms": now_ms}, "own": own, "reference": reference,
            "comparison": {"status": "INCONCLUSIVE", "reasons": ["SIMULATION_VS_PUBLIC_OBSERVATION", "MATCHED_COMPLETE_ACCOUNTING_REQUIRED"]}}
