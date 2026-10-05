"""Bounded secondary summaries. Never infer real PnL or mix real and paper funds."""
import json


def _object(value):
    return value if isinstance(value, dict) else {}


def _units(value):
    if type(value) is int and 0 <= value < 2**128:
        return value
    if isinstance(value, str) and 0 < len(value) <= 39 and value.isascii() and value.isdigit():
        number = int(value)
        return number if number < 2**128 else None
    return None


def money(value, scale=1_000_000):
    """Exact decimal text or None. Bool, negatives and floats are never balances."""
    number = _units(value)
    if number is None:
        return None
    digits = 12 if scale == 1_000_000_000_000 else 6
    whole, fraction = divmod(number, scale)
    return f"{whole}.{fraction:0{digits}d}".rstrip("0").rstrip(".")


def _text(value):
    return value[:256] if isinstance(value, str) and value else None


def journal_summary(state, lane="engine"):
    state = _object(state)
    if lane == "paper":
        parallel = _object(state.get("parallel_simulation"))
        state = {"journal": parallel.get("journal")}
    elif lane == "live":
        live = _object(state.get("live"))
        state = {"journal": {"total_bytes": live.get("journal_bytes"),
                             "total_max_bytes": live.get("journal_max_bytes")}}
    elif lane != "engine":
        raise ValueError("unsupported journal lane")
    raw = _object(state.get("journal"))
    used = _units(raw.get("total_bytes", state.get("journal_bytes")))
    limit = _units(raw.get("total_max_bytes"))
    stage = "unknown"
    if used is not None and limit:
        stage = "full" if used >= limit else "critical" if used * 100 >= limit * 95 else "warning" if used * 100 >= limit * 80 else "normal"
    # Independent identity/operation caps can stop the ledger before its byte limit.
    for count_name, limit_name in (("archive_segments", "max_segments"),
                                   ("owner_commands", "max_commands"),
                                   ("execution_operations", "max_execution_operations")):
        count, maximum = _units(raw.get(count_name)), _units(raw.get(limit_name))
        if count is not None and maximum:
            candidate = "full" if count >= maximum else "critical" if count * 100 >= maximum * 95 else "warning" if count * 100 >= maximum * 80 else "normal"
            rank = {"unknown": 0, "normal": 1, "warning": 2, "critical": 3, "full": 4}
            if rank[candidate] > rank[stage]:
                stage = candidate
    return {"used_bytes": used, "limit_bytes": limit, "stage": stage,
            "archive_segments": _units(raw.get("archive_segments"))}


def execution_summary(state, connected=True):
    state = _object(state)
    profile = state.get("execution_mode")
    if profile not in {"emulator", "simulation", "shadow", "trading", "both"}:
        profile = "unknown"
    live = _object(state.get("live"))
    portfolio = _object(state.get("paper_portfolio"))
    simulation = _object(state.get("simulation"))
    account = _object(simulation.get("account"))
    parallel = _object(state.get("parallel_simulation"))
    real_known = bool(profile in {"trading", "both"} and connected
                      and live.get("simulated") is False and live.get("account_reconciled") is True)
    collateral, available = money(live.get("collateral_micros")), money(live.get("available_micros"))
    real_known = real_known and collateral is not None and available is not None
    cash = _units(portfolio.get("cash_pico"))
    reserved = _units(account.get("reserved_pico"))
    portfolio_reserved = _units(portfolio.get("reserved_pico"))
    # The aggregate portfolio adapter holds filled positions; current Core holds orders.
    # The larger reported reserve is conservative if a future adapter includes both.
    if cash is not None:
        reserve = max(reserved or 0, portfolio_reserved or 0)
        virtual_cash = money(cash, 1_000_000_000_000)
        virtual_reserved = money(reserve, 1_000_000_000_000)
        allocated = _units(account.get("available_pico"))
        virtual_available = money(min(max(0, cash - reserve), allocated), 1_000_000_000_000) if allocated is not None else None
    else:
        initial, spent = _units(account.get("initial_cash_micros")), _units(account.get("spent_pico"))
        virtual_cash = money(max(0, initial * 1_000_000 - spent), 1_000_000_000_000) if initial is not None and spent is not None else None
        virtual_reserved = money(reserved, 1_000_000_000_000)
        virtual_available = money(account.get("available_pico"), 1_000_000_000_000)
    return {
        "profile": profile,
        "real": {"selected": profile in {"trading", "both"}, "balances_known": real_known,
                 "phase": _text(live.get("phase")), "account_reconciled": live.get("account_reconciled") is True,
                 "user_stream_connected": live.get("user_stream_connected") is True,
                 "can_resume": connected and live.get("can_resume") is True,
                 "collateral_usd": collateral if real_known else None,
                 "available_usd": available if real_known else None,
                 "reserved_usd": money(live.get("reserved_pico"), 1_000_000_000_000) if real_known else None,
                 "fault": _text(live.get("fault")), "prepare_error": _text(live.get("last_prepare_error")),
                 "journal": journal_summary(state, "live"), "pnl_usd": None,
                 "daily_loss_bound_usd": money(live.get("daily_loss_micros")),
                 "daily_loss_metric": _text(live.get("daily_loss_metric")),
                 "returned_collateral_usd": money(live.get("returned_collateral_micros")),
                 "chain_pending_hash": _text(live.get("chain_pending_hash")),
                 "chain_pending_kind": live.get("chain_pending_kind") if live.get("chain_pending_kind") in ("batch", "transaction") else None,
                 "chain_error": _text(live.get("last_chain_error")),
                 "session_gas_cost_wei": _text(live.get("session_gas_cost_wei"))},
        "paper": {"selected": profile in {"emulator", "simulation", "both"},
                  "cash_usd": virtual_cash, "available_usd": virtual_available,
                  "reserved_usd": virtual_reserved, "pnl_usd": None,
                  "balance_scope": "global_cash_and_current_market_available",
                  "phase": _text(parallel.get("mode")) or _text(state.get("mode")),
                  "fault": _text(state.get("simulation_error")) or _text(parallel.get("fault")) or _text(portfolio.get("halted")),
                  "journal": journal_summary(state, "paper"),
                  "last_sample_only": not connected},
        "engine_fault": _text(state.get("fault")), "journal": journal_summary(state),
    }


def health_identity(state):
    summary = execution_summary(state)
    source = _object(state.get("source_status"))
    real, paper = summary["real"], summary["paper"]
    return (state.get("boot_id"), state.get("command_version"), state.get("mode"),
            summary["profile"], summary["engine_fault"], source.get("phase"), state.get("feed_ready"),
            real["phase"], real["account_reconciled"], real["user_stream_connected"], real["fault"],
            paper["phase"], paper["fault"], summary["journal"]["stage"],
            real["journal"]["stage"], paper["journal"]["stage"], real["prepare_error"], real["chain_pending_hash"], real["chain_error"], real["returned_collateral_usd"])


def health_log(state, connected=True):
    summary = execution_summary(state, connected)
    real, paper, journal = summary["real"], summary["paper"], summary["journal"]
    errors = [("engine", summary["engine_fault"]), ("live", real["fault"]), ("paper", paper["fault"])]
    failures = "; ".join(f"{kind}: {text}" for kind, text in errors if text)
    warning = (not connected or bool(real["prepare_error"]) or bool(real["chain_error"])
               or any(item["stage"] in {"warning", "critical", "full"}
                      for item in (journal, real["journal"], paper["journal"])))
    level = "ERROR" if failures else "WARN" if warning else "INFO"
    text = (f"Engine {state.get('mode', 'unknown')} | {summary['profile']}; "
            f"feeds {'fresh' if connected and state.get('feed_ready') else 'not ready'}; "
            f"live {real['phase'] or 'unavailable'}; paper {paper['phase'] or 'unavailable'}; "
            f"journal {journal['stage']}; live journal {real['journal']['stage']}; "
            f"paper journal {paper['journal']['stage']}; version {state.get('command_version')}")
    if failures:
        # Store caps messages to 256 chars, so errors must appear before routine details.
        text = failures + "; " + text
    elif not connected:
        text = "Engine publication stale or invalid; " + text
    elif real["prepare_error"]:
        text = "live prepare: " + real["prepare_error"] + "; " + text
    elif real["chain_error"]:
        text = "live closeout: " + real["chain_error"] + "; " + text
    elif real["chain_pending_hash"]:
        text = "live closeout pending " + real["chain_pending_hash"][:18] + "; " + text
    return level, text


def history_projection(state, max_bytes=65_536):
    """Keep the current UI snapshot intact, compact only oversized secondary history."""
    encoded = json.dumps(state, separators=(",", ":"), allow_nan=False).encode()
    if len(encoded) <= max_bytes:
        return state
    keys = ("schema_version", "boot_id", "sequence", "published_at_ms", "command_version", "mode",
            "execution_mode", "feed_source", "api_ready", "feed_ready", "live_eligible", "shadow_ready",
            "fault", "simulation_error", "source_status", "limits", "metrics", "journal", "journal_bytes")
    result = {key: state[key] for key in keys if key in state}
    result["execution_summary"] = execution_summary(state)
    result["history_projection"] = {"compacted": True, "original_bytes": len(encoded),
                                    "omitted": "order, position, book and unbounded detail payloads"}
    result["pnl"] = None
    # A malicious unknown field retained inside metrics must not defeat the history cap.
    if len(json.dumps(result, separators=(",", ":"), allow_nan=False).encode()) > max_bytes:
        result = {key: state.get(key) for key in ("published_at_ms", "command_version", "mode", "execution_mode")}
        result["execution_summary"] = execution_summary(state)
        result["history_projection"] = {"compacted": True, "original_bytes": len(encoded), "omitted": "oversized secondary details"}
    return result
