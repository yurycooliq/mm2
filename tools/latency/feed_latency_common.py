"""Canonical public-feed identities and local clock metadata, without auth."""

import decimal
import hashlib
import importlib.util
import json
import pathlib
import re
import time


SPEC = importlib.util.spec_from_file_location(
    "readonly_http_probe", pathlib.Path(__file__).with_name("clob-latency-probe.py"),
)
HTTP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HTTP)
CANONICAL_VERSION = 2
SCHEMA_VERSION = 1


def label(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value):
        raise ValueError("invalid label")
    return value


def token(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,100}", value):
        raise ValueError("invalid token ID")
    return value


def integer(value):
    if isinstance(value, bool):
        raise ValueError("invalid integer")
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,20}", value):
        value = int(value)
    if not isinstance(value, int) or not 0 <= value <= 2**63 - 1:
        raise ValueError("invalid integer")
    return value


def numeric(value):
    if (not isinstance(value, str) or len(value) > 80
            or not re.fullmatch(r"[0-9]{1,40}(?:\.[0-9]{1,40})?", value)):
        raise ValueError("invalid decimal")
    try:
        parsed = decimal.Decimal(value)
    except decimal.InvalidOperation:
        raise ValueError("invalid decimal") from None
    if not parsed.is_finite() or parsed < 0 or parsed > decimal.Decimal("1e30"):
        raise ValueError("invalid decimal")
    text = format(parsed, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def hex_value(value, optional=False):
    if optional and value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"(?:0x)?[0-9a-fA-F]{1,128}", value):
        raise ValueError("invalid public hash")
    return value.lower()


def side(value):
    if value not in ("BUY", "SELL"):
        raise ValueError("invalid side")
    return value


def event(feed, payload, identity, eligible, aggregate_id=None):
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    event_hash = hashlib.sha256((feed + ":" + canonical).encode()).hexdigest()
    return {
        "feed": feed, "event_type": payload["event_type"],
        "instrument": payload["instrument"], "event_hash": event_hash,
        "identity": identity or "payload:" + event_hash,
        "comparison_eligible": eligible,
        "exchange_event_ms": payload["exchange_event_ms"],
        "exchange_trade_ms": payload.get("exchange_trade_ms"),
        "aggregate_id": aggregate_id,
        "identity_source": "aggregate_trade_id" if feed == "binance" else "canonical_payload",
        "delta": {name: payload[name] for name in ("side", "price", "size", "hash")}
        if feed == "clob" and payload["event_type"] == "price_change" else None,
    }


def normalize_clob(raw, permitted_tokens):
    if not isinstance(raw, dict):
        raise ValueError("invalid market event")
    kind = raw.get("event_type")
    if kind not in ("book", "price_change", "last_trade_price", "tick_size_change"):
        return []
    stamp = integer(raw.get("timestamp"))
    market = hex_value(raw.get("market"), optional=True)
    output = []
    changes = raw.get("price_changes") if kind == "price_change" else [raw]
    if not isinstance(changes, list) or len(changes) > 256:
        raise ValueError("invalid price-change batch")
    for change in changes:
        if not isinstance(change, dict):
            raise ValueError("invalid price change")
        asset = token(change.get("asset_id"))
        if asset not in permitted_tokens:
            continue
        payload = {"event_type": kind, "instrument": asset,
                   "market": market, "exchange_event_ms": stamp}
        public_hash = hex_value(change.get("hash"), optional=True)
        if kind == "book":
            for name in ("bids", "asks"):
                levels = raw.get(name)
                if not isinstance(levels, list) or len(levels) > 5000:
                    raise ValueError("invalid book levels")
                payload[name] = sorted(
                    [{"price": numeric(item["price"]), "size": numeric(item["size"])}
                     for item in levels], key=lambda item: decimal.Decimal(item["price"]),
                )
        elif kind in ("price_change", "last_trade_price"):
            payload.update(price=numeric(change.get("price")), size=numeric(change.get("size")),
                           side=side(change.get("side")))
            for name in ("best_bid", "best_ask", "fee_rate_bps"):
                if change.get(name) is not None:
                    payload[name] = numeric(change[name])
            if raw.get("transaction_hash") is not None:
                payload["transaction_hash"] = hex_value(raw["transaction_hash"])
        else:
            payload.update(old_tick_size=numeric(raw.get("old_tick_size")),
                           new_tick_size=numeric(raw.get("new_tick_size")))
        payload["hash"] = public_hash
        # Snapshots are connection-dependent; trade payloads without a unique
        # trade ID can collide. Only hash-bearing deltas enter arrival comparison.
        eligible = kind == "price_change" and public_hash is not None
        value = event("clob", payload, None, eligible)
        # The book-state hash may be shared by several changed price levels in
        # one millisecond. It is not a unique update ID.
        value["identity"] = f"{kind}:{asset}:{stamp}:{value['event_hash']}"
        output.append(value)
    return output


def normalize_binance(raw, permitted_symbols):
    if not isinstance(raw, dict):
        raise ValueError("invalid Binance event")
    raw = raw.get("data", raw)
    if not isinstance(raw, dict) or raw.get("e") != "aggTrade":
        return []
    symbol = raw.get("s")
    if not isinstance(symbol, str) or symbol.lower() not in permitted_symbols:
        return []
    aggregate = integer(raw.get("a"))
    maker = raw.get("m")
    if not isinstance(maker, bool):
        raise ValueError("invalid maker flag")
    payload = {
        "event_type": "aggTrade", "instrument": symbol.lower(), "aggregate_id": aggregate,
        "exchange_event_ms": integer(raw.get("E")), "exchange_trade_ms": integer(raw.get("T")),
        "first_trade_id": integer(raw.get("f")), "last_trade_id": integer(raw.get("l")),
        "price": numeric(raw.get("p")), "size": numeric(raw.get("q")), "buyer_is_maker": maker,
    }
    return [event("binance", payload, f"aggTrade:{symbol.lower()}:{aggregate}", True, aggregate)]


def normalize_message(feed, message, tokens, symbols):
    if message in (b"PONG", "PONG", b"PING", "PING"):
        return []
    parsed = json.loads(message)
    packets = parsed if isinstance(parsed, list) else [parsed]
    if len(packets) > 256:
        raise ValueError("message event limit")
    output = []
    for packet in packets:
        output.extend(normalize_clob(packet, tokens) if feed == "clob"
                      else normalize_binance(packet, symbols))
    return output


class Clock:
    def __init__(self, offset_ms=0, uncertainty_ms=None, source="unknown", step_ms=10):
        self.offset_ns = round(offset_ms * 1e6)
        self.uncertainty_ms = uncertainty_ms
        self.source = source
        self.step_ms = step_ms
        self.anchor = self.sample()
        self.max_residual_ms = 0
        self.step_detected = False

    @staticmethod
    def sample():
        before = time.perf_counter_ns()
        utc = time.time_ns()
        after = time.perf_counter_ns()
        return {"receive_utc_ns": utc, "receive_mono_ns": (before + after) // 2,
                "clock_sample_uncertainty_ms": (after - before) / 2e6}

    def stamp(self):
        value = self.sample()
        residual = ((value["receive_utc_ns"] - self.anchor["receive_utc_ns"])
                    - (value["receive_mono_ns"] - self.anchor["receive_mono_ns"])) / 1e6
        self.max_residual_ms = max(self.max_residual_ms, abs(residual))
        self.step_detected = self.step_detected or abs(residual) > self.step_ms
        value["elapsed_ms"] = (value["receive_mono_ns"] - self.anchor["receive_mono_ns"]) / 1e6
        value["wall_mono_residual_ms"] = round(residual, 6)
        return value

    def metadata(self):
        return {"offset_ms": self.offset_ns / 1e6, "uncertainty_ms": self.uncertainty_ms,
                "source": self.source, "bound_evidence": "operator_supplied" if self.uncertainty_ms is not None else "unknown",
                "step_threshold_ms": self.step_ms}
