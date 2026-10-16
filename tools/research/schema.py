"""Exact, bounded research contracts; untrusted input never grants completeness."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
from pathlib import Path
import re

VERSION = 1
PICO = 10 ** 12
MICRO = 10 ** 6
MAX_INTEGER = 10 ** 40
MAX_FILE = 64 * 1024 * 1024
MAX_RECORD = 256 * 1024
LANES = {"real", "public-paper", "offline-emulator", "public-benchmark", "shadow"}
EVIDENCE = {"authenticated", "canonical-chain", "public-observation", "synthetic", "inferred"}
STATUSES = {"COMPLETE", "PARTIAL", "CONFLICTED", "UNAVAILABLE"}
SECRET_KEYS = {"private_key", "secret", "api_secret", "api_key", "token", "bot_token", "init_data",
               "signed_intents", "signed_bytes", "signed_body", "raw_transaction", "rpc_url", "password"}


class ResearchError(ValueError):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def utc(value):
    if not isinstance(value, str) or len(value) > 40 or not value.endswith("Z"):
        raise ResearchError("timestamp must be explicit UTC ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ResearchError("invalid UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        raise ResearchError("invalid UTC offset")
    return parsed


def epoch_utc(seconds):
    if type(seconds) is not int or not 0 <= seconds <= 32_503_680_000:
        raise ResearchError("invalid source epoch seconds")
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def window(start, end):
    if utc(start) >= utc(end):
        raise ResearchError("UTC window must be nonempty and half-open")
    return {"start": start, "end": end}


def integer(value, name="integer", signed=False, nullable=False):
    if value is None and nullable:
        return None
    pattern = r"-?(?:0|[1-9][0-9]*)" if signed else r"(?:0|[1-9][0-9]*)"
    if not isinstance(value, str) or len(value) > 42 or not re.fullmatch(pattern, value):
        raise ResearchError(f"{name} must be an exact integer string")
    amount = int(value)
    if abs(amount) > MAX_INTEGER or value == "-0":
        raise ResearchError(f"{name} outside finite bound")
    return amount


def units(value, scale, name="amount", signed=False):
    """Accept API JSON Decimal, never binary float; reject excess precision."""
    if isinstance(value, bool) or isinstance(value, float) or not isinstance(value, (str, int, Decimal)):
        raise ResearchError(f"{name} must be decimal text or JSON Decimal")
    try:
        with localcontext() as context:
            context.prec = 80
            number = Decimal(value)
            amount = number * scale
    except (InvalidOperation, ValueError) as exc:
        raise ResearchError(f"invalid {name}") from exc
    if not number.is_finite() or amount != amount.to_integral_value() or abs(amount) > MAX_INTEGER:
        raise ResearchError(f"inexact or excessive {name}")
    if not signed and amount < 0:
        raise ResearchError(f"negative {name}")
    return str(int(amount))


def address(value):
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{40}", value):
        raise ResearchError("invalid public account address")
    return value.lower()


def condition(value):
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value):
        raise ResearchError("invalid condition identity")
    return value.lower()


def text(value, name="text", limit=256):
    if not isinstance(value, str) or not value or len(value) > limit or any(ord(c) < 32 for c in value):
        raise ResearchError(f"invalid {name}")
    return value


def safe_tree(value, depth=0):
    if depth > 24:
        raise ResearchError("JSON nesting exceeds bound")
    if isinstance(value, dict):
        if len(value) > 4096:
            raise ResearchError("object exceeds bound")
        for key, child in value.items():
            if not isinstance(key, str) or len(key) > 128 or key.lower() in SECRET_KEYS:
                raise ResearchError("unknown/secret key rejected")
            safe_tree(child, depth + 1)
    elif isinstance(value, list):
        if len(value) > 100_000:
            raise ResearchError("array exceeds bound")
        for child in value:
            safe_tree(child, depth + 1)
    elif isinstance(value, float) or isinstance(value, Decimal):
        raise ResearchError("canonical JSON requires exact decimal strings")
    elif value is not None and not isinstance(value, (str, int, bool)):
        raise ResearchError("unsupported JSON type")
    elif isinstance(value, str) and len(value) > MAX_RECORD:
        raise ResearchError("text exceeds bound")


def canonical(value):
    safe_tree(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ResearchError("duplicate JSON key")
        result[key] = value
    return result


def parse_json(data, api=False):
    if not isinstance(data, (str, bytes)) or len(data) > MAX_FILE:
        raise ResearchError("JSON exceeds finite byte bound")
    try:
        value = json.loads(data, parse_float=Decimal if api else lambda _: (_ for _ in ()).throw(ResearchError("binary number forbidden")),
                           parse_constant=lambda _: (_ for _ in ()).throw(ResearchError("nonfinite number forbidden")), object_pairs_hook=_pairs)
    except (json.JSONDecodeError, UnicodeDecodeError, RecursionError) as exc:
        raise ResearchError("invalid JSON") from exc
    if not api:
        safe_tree(value)
    return value


def load_json(path, max_bytes=MAX_FILE):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > max_bytes:
        raise ResearchError("input is missing, symlinked or oversized")
    return parse_json(path.read_bytes())


def quality(status="PARTIAL", reasons=None, **eligibility):
    if status not in STATUSES:
        raise ResearchError("invalid quality state")
    return {"status": status, "reasons": sorted(set(reasons or [])), "eligibility": {
        "accounting": False, "scoring": False, "holdout": False, "latency": False, **eligibility}}


def validate_event(event):
    safe_tree(event)
    if not isinstance(event, dict) or event.get("schema_version") != VERSION:
        raise ResearchError("unsupported event schema")
    text(event.get("event_id"), "event id")
    address(event.get("account_id"))
    if event.get("lane") not in LANES or event.get("evidence_class") not in EVIDENCE:
        raise ResearchError("invalid lane/evidence")
    if event["evidence_class"] == "synthetic" and event["lane"] in {"real", "public-benchmark"}:
        raise ResearchError("synthetic financial evidence cannot enter an actual account lane")
    text(event.get("event_type"), "event type", 64)
    observed = utc(event.get("observed_at_utc"))
    occurred, available = event.get("event_time"), event.get("available_at")
    if occurred is not None:
        occurrence = utc(occurred)
        if event["evidence_class"] != "synthetic" and occurrence > observed:
            raise ResearchError("actual event cannot occur after its observation")
    if available is not None and utc(available) > observed:
        raise ResearchError("available time later than observation")
    source = event.get("source")
    if not isinstance(source, dict) or not re.fullmatch(r"[0-9a-f]{64}", source.get("record_hash", "")):
        raise ResearchError("source provenance required")
    text(source.get("kind"), "source kind", 64)
    receipt = source.get("request_receipt_hash")
    if receipt is not None and not re.fullmatch(r"[0-9a-f]{64}", receipt):
        raise ResearchError("invalid request receipt provenance")
    effects = event.get("effects", {})
    for key in ("cash_delta_pico", "external_contribution_pico"):
        integer(effects.get(key), key, signed=True, nullable=True)
    deltas = effects.get("position_deltas", [])
    if not isinstance(deltas, list) or len(deltas) > 64:
        raise ResearchError("position delta bound exceeded")
    for delta in deltas:
        text(delta.get("asset_id"), "asset id")
        condition(delta.get("condition_id"))
        integer(delta.get("quantity_delta_micros"), "quantity delta", signed=True)
    for name, value in event.get("amounts", {}).items():
        integer(value, name, nullable=True)
    for prefix in ("cost", "fee"):
        actual = integer(event.get("amounts", {}).get(prefix + "_actual_pico"), nullable=True)
        bound = integer(event.get("amounts", {}).get(prefix + "_bound_pico"), nullable=True)
        if actual is not None and bound is not None and actual > bound:
            raise ResearchError("actual cost exceeds declared upper bound")
    if len(canonical(event)) > MAX_RECORD:
        raise ResearchError("event exceeds record bound")
    return event


def validate_snapshot(snapshot):
    safe_tree(snapshot)
    if not isinstance(snapshot, dict) or snapshot.get("schema_version") != VERSION:
        raise ResearchError("unsupported snapshot schema")
    address(snapshot.get("account_id"))
    utc(snapshot.get("at_utc"))
    if snapshot.get("lane") not in LANES:
        raise ResearchError("invalid snapshot lane")
    integer(snapshot.get("cash_pico"), "cash", nullable=True)
    integer(snapshot.get("reserved_pico"), "reserved", nullable=True)
    positions = snapshot.get("positions")
    if not isinstance(positions, list) or len(positions) > 4096:
        raise ResearchError("snapshot positions missing or excessive")
    identities = set()
    for position in positions:
        asset = text(position.get("asset_id"), "asset id")
        if asset in identities:
            raise ResearchError("duplicate snapshot asset")
        identities.add(asset)
        condition(position.get("condition_id"))
        integer(position.get("quantity_micros"), "position quantity")
        for field in ("basis_actual_pico", "basis_bound_pico", "basis_reported_pico"):
            integer(position.get(field), field, nullable=True)
        valuation = position.get("valuation")
        if valuation is not None:
            lower = integer(valuation.get("lower_pico"), "lower value", nullable=True)
            upper = integer(valuation.get("upper_pico"), "upper value", nullable=True)
            if lower is not None and upper is not None and lower > upper:
                raise ResearchError("valuation interval reversed")
            text(valuation.get("kind"), "valuation kind", 64)
            if valuation.get("at_utc") is not None:
                utc(valuation["at_utc"])
    return snapshot
