"""Bounded read-only comparison; a browser request never calls an engine or venue."""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from .observability import execution_summary
from .audit_projection import digest, project_audit
from .telegram_reports import deposit_value, holdings, valuation_boot, valuation_context

REFERENCE_ACCOUNT = "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3"
MAX_VIEW_BYTES = 524_288
PERIODS = {"1h": 3_600_000, "6h": 21_600_000, "24h": 86_400_000, "all": None}
METRICS = ("cash_usd", "available_usd", "reserved_usd", "nav_usd", "net_pnl_usd",
           "equity_change_usd", "turnover_usd", "observed_events", "cost_bound_usd", "position_value_usd")
PNL_ENDPOINT = "https://data-api.polymarket.com/v2/user-pnl"
PNL_KEYS = ("economic_pnl_usd", "realized_pnl_usd", "unrealized_pnl_usd", "wallet_income_usd")


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


def empty_reported_performance(status="UNAVAILABLE", reason="PNL_SOURCE_UNAVAILABLE"):
    return {"status": status, "evidence_class": "venue-reported-wallet-estimate", "wallet": REFERENCE_ACCOUNT,
            "endpoint": PNL_ENDPOINT, "interval": "1d", "fidelity": "1h", "source_fidelity": None,
            "source_at_ms": None, "response_sha256": None, "latest_at_ms": None,
            "latest": {key: None for key in PNL_KEYS}, "period": {"start_ms": None, "end_ms": None},
            "change_economic_pnl_usd": None, "series": [], "warnings": [reason]}


def _pnl_pico(value):
    if value is None:
        return None
    if _amount(value) is None:
        raise ValueError("invalid reported amount")
    negative = value.startswith("-")
    whole, _, fraction = value.lstrip("-").partition(".")
    amount = (-1 if negative else 1) * (int(whole) * 10**12 + int(fraction.ljust(12, "0") or "0"))
    if abs(amount) >= 10**40:
        raise ValueError("reported amount outside exact bound")
    return amount


def model_result(state, connected, at_ms):
    fields = {"initial_cash_usd": "initial_cash_pico", "cash_usd": "cash_pico", "spent_usd": "spent_pico",
              "fees_usd": "fees_pico", "returned_usd": "returned_pico", "realized_pnl_usd": "realized_pnl_pico"}
    result = {"status": "UNAVAILABLE", "kind": "cumulative-synthetic-ledger", "at_ms": at_ms,
              **{key: None for key in fields}, "reason": "MODEL_LEDGER_UNAVAILABLE"}
    if not connected:
        return result
    portfolio = obj(state.get("paper_portfolio"))
    if not all(key in portfolio for key in fields.values()):
        return result
    try:
        unsigned = {key: uint(portfolio[source]) for key, source in fields.items() if key != "realized_pnl_usd"}
        signed = portfolio["realized_pnl_pico"]
        if not isinstance(signed, str) or not re.fullmatch(r"(?:0|-?[1-9][0-9]{0,38})", signed):
            raise ValueError("invalid synthetic signed result")
        realized = int(signed)
        if (any(value is None for value in unsigned.values()) or not -(2**127) <= realized < 2**127
                or unsigned["initial_cash_usd"] + unsigned["returned_usd"] != unsigned["cash_usd"] + unsigned["spent_usd"]
                or unsigned["fees_usd"] > unsigned["spent_usd"]):
            raise ValueError("invalid synthetic conservation")
        result.update({key: decimal(value) for key, value in unsigned.items()})
        result.update(status="AVAILABLE", realized_pnl_usd=decimal(realized), reason="SYNTHETIC_CUMULATIVE_NOT_PERIOD_RESULT")
    except (ValueError, TypeError):
        result.update(status="INVALID", reason="MODEL_LEDGER_INVALID")
    return result


def load_reported_performance(value, now_ms, source_observed_ms, source_first, source_last, selected_first, selected_last):
    """Optional provider estimates stay separate from authenticated/account results."""
    if not isinstance(value, dict):
        return empty_reported_performance()
    if value.get("status") != "AVAILABLE":
        return empty_reported_performance("INVALID" if value.get("status") == "INVALID" else "UNAVAILABLE",
            "PNL_SOURCE_INVALID" if value.get("status") == "INVALID" else "PNL_SOURCE_UNAVAILABLE")
    try:
        source_at, latest_at = uint(value.get("source_at_ms")), uint(value.get("latest_at_ms"))
        if (value.get("evidence_class") != "venue-reported-wallet-estimate" or value.get("wallet") != REFERENCE_ACCOUNT
                or value.get("endpoint") != PNL_ENDPOINT or value.get("interval") != "1d" or value.get("fidelity") != "1h"
                or value.get("source_fidelity") != "1d" or source_at is None or latest_at is None
                or not latest_at <= source_at + 5000 <= source_observed_ms + 5000 or source_at > now_ms + 5000
                or not isinstance(value.get("response_sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", value["response_sha256"])):
            raise ValueError("invalid reported provenance")
        series = value.get("series")
        if not isinstance(series, list) or not 2 <= len(series) <= 64:
            raise ValueError("invalid reported series")
        cleaned, previous = [], None
        for row in series:
            at = uint(row.get("at_ms")) if isinstance(row, dict) else None
            if at is None or not source_at - 86_400_000 <= at <= source_at + 5000 or (previous is not None and at - previous != 3_600_000):
                raise ValueError("invalid reported point clock")
            if set(row) != {"at_ms", *PNL_KEYS}:
                raise ValueError("invalid reported point fields")
            cleaned.append({"at_ms": at, **{key: decimal(_pnl_pico(row.get(key))) for key in PNL_KEYS}})
            previous = at
        if cleaned[-1]["at_ms"] != latest_at or source_at - latest_at > 3_600_000:
            raise ValueError("invalid reported latest baseline")
        if value.get("latest") != {key: cleaned[-1][key] for key in PNL_KEYS} or value.get("period") != {"start_ms": cleaned[0]["at_ms"], "end_ms": latest_at}:
            raise ValueError("inconsistent reported latest/window")
        warnings = ["PNL_ESTIMATE_NOT_AUTHENTICATED_BOT_RESULT", "PNL_DAILY_SOURCE_ON_HOURLY_GRID"]
        if "PNL_CASHFLOWS_UNKNOWN" in value.get("warnings", []):
            warnings.append("PNL_CASHFLOWS_UNKNOWN")
        if cleaned[0]["at_ms"] != source_first or latest_at != source_last:
            warnings.append("PNL_WINDOW_DIFFERS_FROM_ACTIVITY")
        selected = [row for row in cleaned if (selected_first is None or row["at_ms"] >= selected_first) and row["at_ms"] <= selected_last]
        result = {"status": "AVAILABLE", "evidence_class": "venue-reported-wallet-estimate", "wallet": REFERENCE_ACCOUNT,
            "endpoint": PNL_ENDPOINT, "interval": "1d", "fidelity": "1h", "source_fidelity": "1d",
            "source_at_ms": source_at, "response_sha256": value["response_sha256"], "latest_at_ms": latest_at,
            "latest": {key: cleaned[-1][key] for key in PNL_KEYS},
            "period": {"start_ms": selected[0]["at_ms"] if selected else None, "end_ms": selected[-1]["at_ms"] if selected else None},
            "change_economic_pnl_usd": None, "series": selected, "warnings": warnings}
        if len(selected) >= 2 and selected[0]["economic_pnl_usd"] is not None and selected[-1]["economic_pnl_usd"] is not None:
            result["change_economic_pnl_usd"] = decimal(_pnl_pico(selected[-1]["economic_pnl_usd"]) - _pnl_pico(selected[0]["economic_pnl_usd"]))
        else:
            result["warnings"].append("PNL_PERIOD_BASELINE_INSUFFICIENT")
        if now_ms - latest_at > 3_600_000:
            result["status"] = "STALE"
            result["warnings"].append("PNL_SOURCE_STALE")
        return result
    except (ValueError, TypeError, KeyError, OverflowError, AttributeError):
        return empty_reported_performance("INVALID", "PNL_SOURCE_INVALID")


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
        result["reported_performance"] = load_reported_performance(view.get("reported_performance"), now_ms, observed,
            first, last, result["period"]["start_ms"] if selected else start_ms, result["period"]["end_ms"])
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
    result["model_result"] = model_result(state, connected, observed)
    result["warnings"] = ["SIMULATION_SYNTHETIC_EXECUTION", "RECENT_AUDIT_NOT_COMPLETE_LEDGER", "CURRENT_SNAPSHOT_NOT_PERIOD_RESULT"]
    for key in ("turnover_usd", "cost_bound_usd", "position_value_usd"):
        result["metrics"][key] = metric(scope="simulation evidence", reason="FULL_SIMULATION_PERIOD_LEDGER_REQUIRED")
    for key in ("cash_usd", "available_usd", "reserved_usd"):
        result["metrics"][key] = metric(paper.get(key) if connected else None, scope="current simulation snapshot",
            reason="CURRENT_SNAPSHOT_NOT_PERIOD_RESULT" if connected else "NO_FRESH_ENGINE_SNAPSHOT")
    result["metrics"]["nav_usd"] = metric(decimal(deposit_value(state, "paper", now_ms)) if connected else None,
        scope="current synthetic best-bid NAV estimate", reason="SAMPLED_NAV_NOT_REALIZED_PROFIT")
    result["metrics"]["net_pnl_usd"] = metric(scope="simulation period result", reason="FULL_BOT_LEDGER_REQUIRED")
    try:
        history = store.action_history(state, observed, now_ms, start_ms)
    except (sqlite3.Error, ValueError, TypeError, OverflowError):
        # Keep the current ring visible even if the secondary store is down.
        # No history is invented, imported or persisted by an API request.
        projected = project_audit(state, observed, store.action_observer) if observed is not None else None
        actions = []
        if projected is not None:
            generation = digest([projected["base"], "initial"])
            for action in projected["actions"]:
                action, _ = store._action(action, generation)
                at = action["at_ms"] if action["at_ms"] is not None else observed
                if at is not None and at <= now_ms and (start_ms is None or at >= start_ms):
                    action["observation"] = "current-snapshot"
                    actions.append(action)
        history = {"actions": list(reversed(actions)), "observed_actions": len(actions), "omitted_actions": 0,
            "retention_omitted_observations": None, "scope_id": None,
            "warnings": (projected["warnings"] if projected else []) + ["SECONDARY_ACTION_HISTORY_UNAVAILABLE"]}
    result["actions"] = history["actions"]
    result["warnings"].extend(history["warnings"])
    telemetry = obj(envelope.get("telemetry"))
    if telemetry.get("action_history_error") is not None:
        result["warnings"].append("SECONDARY_ACTION_HISTORY_UNAVAILABLE")
    if uint(telemetry.get("action_history_dropped")):
        result["warnings"].append("ACTION_HISTORY_SAMPLE_GAPS")
    if history["scope_id"] is not None:
        result["source_refs"].append({"kind": "secondary_sampled_simulation_audit", "id": history["scope_id"],
                                     "sha256": None, "at_ms": observed})
    result["coverage"] = {"actions_complete": False, "financial_complete": False,
        "reason": "SAMPLED_ACTION_HISTORY_NOT_COMPLETE_LEDGER", "observed_actions": history["observed_actions"],
        "omitted_actions": history["omitted_actions"],
        "retention_omitted_observations": history["retention_omitted_observations"],
        "source_audit_evicted": uint(obj(state.get("simulation")).get("audit_evicted"))}
    result["metrics"]["observed_events"] = metric(str(history["observed_actions"]), unit="count",
        scope="retained sampled simulation observations within requested period", reason="SAMPLED_ACTION_HISTORY_NOT_COMPLETE_LEDGER")
    result["warnings"] = sorted(set(result["warnings"]))
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
