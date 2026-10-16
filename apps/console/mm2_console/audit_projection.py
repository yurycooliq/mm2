"""Sanitized sampled paper audit, outside the engine and primary journal."""
import hashlib
import json
import re
from urllib.parse import urlsplit

from .observability import _units, money
from .telegram_reports import valuation_boot

MAX_ACTIONS = 256
KINDS = {"book", "cancel_arrived", "cancel_requested", "cancel_transport_unknown", "fault_injected",
         "post_accepted", "post_requested", "post_only_rejected", "post_transport_unknown", "query_reconciled",
         "query_requested", "query_unavailable", "rate_limited", "venue_fill", "observation"}


def obj(value):
    return value if isinstance(value, dict) else {}


def text(value, maximum=128):
    return value if isinstance(value, str) and 0 < len(value) <= maximum and all(ord(c) >= 32 for c in value) else None


def digest(value):
    return hashlib.sha256(json.dumps(value, separators=(",", ":"), sort_keys=True).encode()).hexdigest()


def observer_identity(endpoint):
    parsed = urlsplit(endpoint)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password
            or parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
        raise ValueError("invalid public observer endpoint")
    return digest([parsed.scheme, parsed.hostname.lower(), parsed.port or (443 if parsed.scheme == "https" else 80)])


def locator(state):
    # This is a published directory locator, NOT a globally unique account ID.
    if state.get("execution_mode") == "both":
        return None
    prefix = obj(obj(state.get("journal")).get("committed_prefix"))
    segment = obj(prefix.get("segment"))
    hashes = (prefix.get("ledger_id"), prefix.get("last_hash"), segment.get("previous_hash"))
    integers = (prefix.get("next_sequence"), prefix.get("byte_offset"), prefix.get("manifest_bytes"),
                segment.get("index"), segment.get("start_sequence"))
    if (prefix.get("schema_version") != 1 or prefix.get("source_kind") != "engine"
            or any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value) for value in hashes)
            or any(type(value) is not int or not 0 <= value < 2**64 for value in integers)
            or segment.get("name") != "commands.jsonl" or segment["start_sequence"] > prefix["next_sequence"]
            or prefix["byte_offset"] > 67_108_864 or prefix["manifest_bytes"] > 134_217_728):
        return None
    return {"ledger_locator": prefix["ledger_id"], "prefix_sequence": prefix["next_sequence"],
            "prefix_hash": prefix["last_hash"]}


def project_audit(state, observed_at_ms, observer):
    """A bounded source-only projection; never invent wall time or order details."""
    if type(observed_at_ms) is not int or not 0 <= observed_at_ms < 253_402_300_800_000:
        raise ValueError("invalid action observation clock")
    if not isinstance(observer, str) or not re.fullmatch(r"[0-9a-f]{64}", observer):
        raise ValueError("invalid action observer")
    state = obj(state)
    profile = state.get("execution_mode")
    simulation = obj(state.get("simulation"))
    boot, epoch = valuation_boot(state), _units(state.get("reset_epoch"))
    if profile not in {"emulator", "simulation", "both"} or boot is None or epoch is None or epoch >= 2**64:
        return None
    binding = locator(state)
    public_source = state.get("feed_source") if state.get("feed_source") in {"public", "demo"} else "unknown"
    base = digest([observer, profile, public_source, epoch, binding["ledger_locator"] if binding else ["unbound", boot]])
    audit_sequence = _units(simulation.get("audit_sequence"))
    result = {"base": base, "boot_id": boot, "epoch": epoch, "profile": profile,
              "ledger_locator": binding["ledger_locator"] if binding else None,
              "prefix_sequence": binding["prefix_sequence"] if binding else None,
              "prefix_hash": binding["prefix_hash"] if binding else None, "observed_at_ms": observed_at_ms,
              "audit_sequence": audit_sequence if audit_sequence is not None and audit_sequence < 2**64 else None,
              "market": text(state.get("simulation_market_condition")),
              "actions": [], "warnings": ["SAMPLED_ACTION_HISTORY_NOT_COMPLETE_LEDGER"]}
    if binding is None:
        result["warnings"].append("ACTION_HISTORY_LOCATOR_UNBOUND")
    # A recovered Paused engine has a durable portfolio/prefix and no active
    # PaperSession. Its current binding can still select prior observations.
    if state.get("simulation") is None:
        return result
    if simulation.get("simulated") is not True or result["market"] is None or result["audit_sequence"] is None:
        result["warnings"].append("ACTION_HISTORY_INVALID_SOURCE")
        return result
    now_sim = _units(simulation.get("now_ms"))
    wall_clock = now_sim is not None and abs(now_sim - observed_at_ms) <= 60_000
    if not wall_clock:
        result["warnings"].append("SIMULATION_CLOCK_NOT_WALL_TIME")
    audit = simulation.get("recent_audit")
    if not isinstance(audit, list) or len(audit) > MAX_ACTIONS:
        result["warnings"].append("ACTION_HISTORY_INVALID_SOURCE")
        return result
    quotes, conflicting = {}, set()
    same_market = result["market"] is not None and obj(state.get("market")).get("condition_id") == result["market"]
    if same_market:
        for name in ("venue_orders", "orders"):
            values = simulation.get(name)
            for raw in values[:1024] if isinstance(values, list) else []:
                raw, quote = obj(raw), obj(obj(raw).get("quote"))
                identity, price = _units(raw.get("id")), _units(quote.get("price_micros"))
                if identity is None or price is None or not 0 < price < 1_000_000 or quote.get("outcome") not in {"up", "down"}:
                    continue
                value = {"outcome": quote["outcome"], "price_micros": price}
                if identity in quotes and quotes[identity] != value:
                    conflicting.add(identity)
                quotes[identity] = value
        for identity in conflicting:
            quotes.pop(identity, None)
        if conflicting:
            result["warnings"].append("ACTION_HISTORY_INVALID_SOURCE")
    previous = None
    for raw in audit:
        raw = obj(raw)
        seq, source_at = _units(raw.get("sequence")), _units(raw.get("at_ms"))
        order, quantity = _units(raw.get("order_id")), _units(raw.get("quantity_micros"))
        if (seq is None or not 0 < seq < 2**64 or source_at is None or source_at >= 253_402_300_800_000 or raw.get("kind") not in KINDS
                or now_sim is None or source_at > now_sim
                or (previous is not None and seq <= previous)
                or (result["audit_sequence"] is not None and seq > result["audit_sequence"])
                or (raw.get("order_id") is not None and (order is None or order >= 2**64))
                or (raw.get("quantity_micros") is not None and (quantity is None or quantity >= 2**64))):
            result["warnings"].append("ACTION_HISTORY_INVALID_SOURCE")
            continue
        if previous is not None and seq != previous + 1:
            result["warnings"].append("ACTION_HISTORY_SAMPLE_GAPS")
        previous = seq
        quote = quotes.get(order, {})
        event_id = "paper:" + digest([base, boot, epoch, result["market"], seq])
        result["actions"].append({"id": event_id, "at_ms": source_at if wall_clock else None,
            "observed_at_ms": observed_at_ms, "source_time_ms": source_at, "clock_kind": "wall" if wall_clock else "logical",
            "boot_id": boot, "ledger_locator": result["ledger_locator"], "sequence": seq, "reset_epoch": epoch,
            "kind": raw["kind"], "market": result["market"], "side": "BUY" if quote else None,
            "outcome": quote.get("outcome"), "quantity_shares": money(quantity), "amount_usd": None,
            "source": "synthetic-recent-audit", "observation": "persisted-secondary-sample",
            "order_id": str(order) if order is not None else None,
            "title": text(obj(state.get("market")).get("title")) if same_market else None,
            "price_usd": money(quote.get("price_micros"))})
    result["warnings"] = sorted(set(result["warnings"]))
    return result
