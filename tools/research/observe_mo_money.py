"""Bounded public-only activity/positions observer for mo-money and the owner wallet.

Pinned legacy Data API adapter; no orders, RPC credentials or private endpoints.
Official contracts checked 2026-10-06:
https://docs.polymarket.com/api-reference/core/get-user-activity
https://docs.polymarket.com/api-reference/core/get-current-positions-for-a-user
API coverage is NOT reconciled whole-wallet accounting completeness.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request
from .archive import publish_bundle
from .registry import load_registry
from .schema import (VERSION, MICRO, PICO, ResearchError, address, canonical, condition, digest,
                     epoch_utc, load_json, parse_json, quality, units, utc, utc_now, validate_event, window)

BASE = "https://data-api.polymarket.com"
ACTIVITY_TYPES = {"TRADE", "SPLIT", "MERGE", "REDEEM", "REWARD", "CONVERSION", "DEPOSIT",
                  "WITHDRAWAL", "YIELD", "MAKER_REBATE", "TAKER_REBATE", "REFERRAL_REWARD"}
ACTIVITY_FIELDS = {"proxyWallet", "timestamp", "conditionId", "type", "size", "usdcSize", "transactionHash",
                   "price", "asset", "side", "outcomeIndex", "outcome", "isCombo", "id", "logIndex"}
POSITION_FIELDS = {"proxyWallet", "asset", "conditionId", "size", "avgPrice", "initialValue", "currentValue",
                   "curPrice", "outcome", "outcomeIndex", "redeemable", "mergeable", "negativeRisk", "endDate"}


def _exact_public(value):
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ResearchError("nonfinite API amount")
        return str(value)
    if isinstance(value, float):
        raise ResearchError("binary API float forbidden")
    if isinstance(value, dict):
        return {k: _exact_public(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_exact_public(v) for v in value]
    return value


def _public_row(raw, fields):
    if not isinstance(raw, dict):
        raise ResearchError("API row is not an object")
    return _exact_public({key: raw[key] for key in sorted(fields) if key in raw})


def display_title(raw):
    # Cosmetic labels are outside the financial row digest/event identity. They
    # remain bounded public source evidence inside the verified observer bundle.
    value = raw.get("title")
    return value if isinstance(value, str) and 0 < len(value) <= 128 and all(ord(char) >= 32 for char in value) else None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise ResearchError("public API redirect refused")


class PublicHTTP:
    def __init__(self, max_bytes=16_000_000, max_requests=240, max_seconds=120, opener=None):
        if any(type(n) is not int or n <= 0 for n in (max_bytes, max_requests, max_seconds)):
            raise ResearchError("finite HTTP budgets required")
        if max_bytes > 64_000_000 or max_requests > 10_000 or max_seconds > 3600:
            raise ResearchError("HTTP budget exceeds hard limit")
        self.max_bytes, self.max_requests = max_bytes, max_requests
        self.deadline = time.monotonic() + max_seconds
        self.opener = opener or urllib.request.build_opener(NoRedirect()).open
        self.bytes_read, self.request_count, self.receipts = 0, 0, []

    def get(self, endpoint, parameters):
        if endpoint not in {"activity", "positions"}:
            raise ResearchError("public GET endpoint not allowed")
        allowed = ({"user", "start", "end", "limit", "offset", "sortBy", "sortDirection", "excludeDepositsWithdrawals"}
                   if endpoint == "activity" else {"user", "sizeThreshold", "includeArchived", "limit", "offset", "sortBy", "sortDirection"})
        if not isinstance(parameters, dict) or set(parameters) - allowed or len(canonical(parameters)) > 2048:
            raise ResearchError("public GET query outside allowlist/bound")
        address(parameters.get("user"))
        url = BASE + "/" + endpoint + "?" + urllib.parse.urlencode(parameters)
        for attempt in range(3):
            remaining = self.deadline - time.monotonic()
            budget = self.max_bytes - self.bytes_read
            if self.request_count >= self.max_requests or remaining <= 0 or budget <= 0:
                raise ResearchError("public capture budget exhausted")
            self.request_count += 1
            limit = min(budget, 4_000_000)
            request = urllib.request.Request(url, headers={"Accept": "application/json", "Accept-Encoding": "identity",
                                                          "User-Agent": "mm2-public-research/1"})
            try:
                with self.opener(request, timeout=min(15, remaining)) as response:
                    if getattr(response, "status", 200) != 200:
                        raise ResearchError("public API non-success status")
                    data = response.read(limit + 1)
                    self.bytes_read += len(data)
                    if len(data) > limit:
                        raise ResearchError("public response byte limit exceeded")
                    value = parse_json(data, api=True)
                    if not isinstance(value, list) or len(value) > 500 or any(not isinstance(row, dict) for row in value):
                        raise ResearchError("public API envelope/page changed")
                    receipt = {"endpoint": endpoint, "parameters": parameters, "bytes": len(data),
                               "response_sha256": hashlib.sha256(data).hexdigest(), "observed_at_utc": utc_now(),
                               "rows": len(value)}
                    self.receipts.append(receipt)
                    return value, receipt
            except urllib.error.HTTPError as exc:
                if exc.code not in {429, 500, 502, 503, 504} or attempt == 2:
                    raise ResearchError(f"public API HTTP {exc.code}") from None
                delay = min(2 ** attempt, max(0, self.deadline - time.monotonic()))
                if delay > 0:
                    time.sleep(delay)
            except (urllib.error.URLError, TimeoutError, OSError):
                if attempt == 2:
                    raise ResearchError("public API transport failed") from None
        raise ResearchError("public API retries exhausted")


def collect_activity(client, account_id, start, end, page_size=500, max_rows=100_000, offset_cap=5000):
    """Inclusive API seconds -> disjoint half-open windows. Dense seconds stay PARTIAL."""
    account_id = address(account_id)
    if not 1 <= page_size <= 500 or not 1 <= max_rows <= 100_000:
        raise ResearchError("invalid capture row/page quota")
    start_dt, end_dt = utc(start), utc(end)
    if start_dt.microsecond or end_dt.microsecond:
        raise ResearchError("public activity window boundaries require whole seconds")
    start_s, end_s = int(start_dt.timestamp()), int(end_dt.timestamp())
    if not 0 <= start_s < end_s or end_s - start_s > 31 * 86400:
        raise ResearchError("activity window must be 1 second..31 days")
    rows, gaps, visited = [], [], 0

    def capture(lo, hi):
        nonlocal visited
        visited += 1
        if visited > 8192:
            gaps.append({"start": epoch_utc(lo), "end": epoch_utc(hi), "reason": "WINDOW_SPLIT_QUOTA"})
            return
        selected, offset = [], 0
        while offset <= offset_cap:
            params = {"user": account_id, "start": lo, "end": hi - 1, "limit": page_size, "offset": offset,
                      "sortBy": "TIMESTAMP", "sortDirection": "ASC", "excludeDepositsWithdrawals": "false"}
            try:
                page, receipt = client.get("activity", params)
                for raw in page:
                    if type(raw.get("timestamp")) is not int or not lo <= raw["timestamp"] < hi:
                        raise ResearchError("activity row outside requested interval")
                    if address(raw.get("proxyWallet")) != account_id:
                        raise ResearchError("activity returned wrong account")
                    projected = _public_row(raw, ACTIVITY_FIELDS)
                    canonical(projected)
                    selected.append({"row": projected, "receipt_hash": digest(receipt),
                                     "observed_at_utc": receipt["observed_at_utc"], "display_title": display_title(raw)})
                if len(page) < page_size:
                    if len(rows) + len(selected) > max_rows:
                        raise ResearchError("capture row budget exhausted")
                    rows.extend(selected)
                    return
            except (ResearchError, ValueError, KeyError):
                remaining = max_rows - len(rows)
                rows.extend(selected[:remaining])
                gaps.append({"start": epoch_utc(lo), "end": epoch_utc(hi), "reason": "ACTIVITY_PARTIAL_OR_INVALID"})
                return
            offset += page_size
        if hi - lo <= 1:
            remaining = max_rows - len(rows)
            rows.extend(selected[:remaining])
            gaps.append({"start": epoch_utc(lo), "end": epoch_utc(hi), "reason": "DENSE_SECOND_OFFSET_CAP"})
        else:
            # Discard truncated parent pages before fetching disjoint children.
            middle = lo + (hi - lo) // 2
            capture(lo, middle)
            capture(middle, hi)

    capture(start_s, end_s)
    return {"rows": rows, "complete": not gaps, "gaps": gaps, "utc_range": window(start, end)}


def collect_positions(client, account_id, page_size=500, max_rows=10_000, offset_cap=10000):
    account_id = address(account_id)
    result, offset, reasons, seen = [], 0, [], set()
    while offset <= offset_cap:
        parameters = {"user": account_id, "sizeThreshold": "0", "includeArchived": "true",
                      "limit": page_size, "offset": offset, "sortBy": "TOKENS", "sortDirection": "ASC"}
        try:
            page, receipt = client.get("positions", parameters)
            for raw in page:
                if address(raw.get("proxyWallet")) != account_id:
                    raise ResearchError("positions returned wrong account")
                projected = _public_row(raw, POSITION_FIELDS)
                asset = projected.get("asset")
                if not isinstance(asset, str) or not asset or asset in seen:
                    raise ResearchError("positions shifted/duplicate asset")
                seen.add(asset)
                if len(result) >= max_rows:
                    raise ResearchError("positions row quota exceeded")
                result.append({"row": projected, "receipt_hash": digest(receipt), "observed_at_utc": receipt["observed_at_utc"]})
            if len(page) < page_size:
                return {"rows": result, "complete": True, "reasons": ["NONATOMIC_PUBLIC_POSITIONS_SNAPSHOT"]}
        except (ResearchError, ValueError):
            reasons.append("POSITIONS_PARTIAL_OR_INVALID")
            break
        offset += page_size
    return {"rows": result, "complete": False, "reasons": reasons or ["POSITIONS_OFFSET_CAP"]}


def normalize_activity(item, account_id, occurrence=0):
    row = item["row"]
    kind = row.get("type")
    cash = units(row.get("usdcSize"), PICO, "reported cash")
    quantity = units(row.get("size", "0"), MICRO, "reported quantity")
    source_hash = digest(row)
    effects = {"cash_delta_pico": None, "external_contribution_pico": "0", "position_deltas": []}
    reasons = ["PUBLIC_ACTIVITY_NOT_CANONICAL_RECEIPT"]
    amounts = {"cash_reported_pico": cash, "quantity_micros": quantity, "fee_actual_pico": None,
               "fee_bound_pico": None, "cost_actual_pico": None, "cost_bound_pico": None, "price_micros": None}
    if kind not in ACTIVITY_TYPES or row.get("isCombo") is True:
        reasons.append("UNSUPPORTED_ACTIVITY_SEMANTICS")
    elif kind in {"DEPOSIT", "WITHDRAWAL"}:
        direction = 1 if kind == "DEPOSIT" else -1
        effects["cash_delta_pico"] = effects["external_contribution_pico"] = str(direction * int(cash))
    elif kind in {"REWARD", "YIELD", "MAKER_REBATE", "TAKER_REBATE", "REFERRAL_REWARD"}:
        effects["cash_delta_pico"] = cash
    elif kind in {"TRADE", "REDEEM"}:
        asset, market = row.get("asset"), condition(row.get("conditionId"))
        if kind == "TRADE":
            if row.get("side") not in {"BUY", "SELL"}:
                raise ResearchError("unknown trade side")
            sign = 1 if row["side"] == "BUY" else -1
            effects["cash_delta_pico"] = str(-sign * int(cash))
            # Live API effective/average prices can exceed native tick precision.
            # Preserve them losslessly without inventing a native order price.
            try:
                amounts["price_micros"] = units(row.get("price"), MICRO, "reported price")
            except ResearchError:
                reasons.append("REPORTED_PRICE_NOT_NATIVE_MICRO_ALIGNED")
            reasons.append("ACTUAL_TRADE_FEES_UNKNOWN")
        else:
            sign = -1
            effects["cash_delta_pico"] = cash
            reasons.append("REDEMPTION_FINALITY_UNVERIFIED")
        if not isinstance(asset, str) or not asset:
            reasons.append("REDEMPTION_TOKEN_MAPPING_REQUIRED" if kind == "REDEEM" else "TRADE_TOKEN_MAPPING_REQUIRED")
        else:
            effects["position_deltas"] = [{"asset_id": asset, "condition_id": market, "outcome": row.get("outcome"),
                                           "quantity_delta_micros": str(sign * int(quantity))}]
    elif kind in {"MERGE", "SPLIT"}:
        effects["cash_delta_pico"] = cash if kind == "MERGE" else str(-int(cash))
        reasons.append("COMPLETE_SET_TOKEN_MAPPING_REQUIRED")
    elif kind == "CONVERSION":
        reasons.append("CONVERSION_MAPPING_REQUIRED")
    event = {"schema_version": VERSION, "event_id": "activity:" + source_hash + ":" + str(occurrence),
             "account_id": address(account_id), "lane": "public-benchmark", "event_type": kind or "UNKNOWN",
             "event_time": epoch_utc(row["timestamp"]), "observed_at_utc": item["observed_at_utc"],
             "available_at": item["observed_at_utc"], "timestamp_precision": "seconds",
             "evidence_class": "public-observation", "source": {"kind": "polymarket-activity-v1",
                 "record_hash": source_hash, "request_receipt_hash": item["receipt_hash"], "multiplicity_ordinal": occurrence},
             "transaction_hash": row.get("transactionHash"), "effects": effects, "amounts": amounts,
             "side": row.get("side"),
             "reported_price_decimal": str(row["price"]) if row.get("price") is not None else None,
             "quality": quality("PARTIAL", reasons)}
    return validate_event(event)


def position_snapshot(collected, account_id):
    positions, errors = [], []
    for item in collected["rows"]:
        raw = item["row"]
        try:
            quantity = units(raw.get("size"), MICRO)
            value = units(raw["currentValue"], PICO) if raw.get("currentValue") is not None else None
            positions.append({"asset_id": raw["asset"], "condition_id": condition(raw["conditionId"]),
                "outcome": raw.get("outcome"), "quantity_micros": quantity,
                "basis_actual_pico": None, "basis_bound_pico": None,
                "basis_reported_pico": units(raw["initialValue"], PICO) if raw.get("initialValue") is not None else None,
                "valuation": {"kind": "public_reference", "lower_pico": value, "upper_pico": value,
                              "at_utc": item["observed_at_utc"], "complete": False},
                "source_record_hash": digest(raw)})
        except (ResearchError, KeyError):
            errors.append("INVALID_POSITION_ROW")
    observed = max((x["observed_at_utc"] for x in collected["rows"]), default=utc_now())
    return {"schema_version": VERSION, "account_id": address(account_id), "lane": "public-benchmark",
        "at_utc": observed, "cash_pico": None, "reserved_pico": None, "positions": positions,
        "positions_complete": collected["complete"] and not errors, "financial_complete": False,
        "quality": quality("PARTIAL", ["PUBLIC_CASH_UNKNOWN", "NONATOMIC_PUBLIC_SNAPSHOT"] + collected["reasons"] + errors)}


def observe_account(client, account, start, end, *, page_size=500, max_rows=100_000):
    activity = collect_activity(client, account["account_id"], start, end, page_size, max_rows)
    positions = collect_positions(client, account["account_id"], page_size=page_size)
    events, errors, occurrences = [], [], Counter()
    for item in activity["rows"]:
        fingerprint = digest(item["row"])
        occurrence = occurrences[fingerprint]
        occurrences[fingerprint] += 1
        try:
            events.append(normalize_activity(item, account["account_id"], occurrence))
        except (ResearchError, ValueError, KeyError):
            errors.append({"record_hash": fingerprint, "reason": "INVALID_ACTIVITY_ROW"})
    reasons = ["STARTING_CAPITAL_UNKNOWN", "PUBLIC_CASH_UNKNOWN", "PUBLIC_ACTIVITY_RECONCILIATION_REQUIRED"]
    if not activity["complete"] or errors:
        reasons.append("ACTIVITY_INCOMPLETE")
    if not positions["complete"]:
        reasons.append("POSITIONS_INCOMPLETE")
    if account.get("identity_verified_at") is None:
        reasons.append("PROFILE_ATTRIBUTION_REVERIFY_REQUIRED")
    if account.get("purpose") == "owner-account-reconciliation":
        reasons.append("PUBLIC_OWNER_ACTIVITY_MM2_ATTRIBUTION_UNPROVEN")
    snapshot = position_snapshot(positions, account["account_id"])
    coverage = {"activity_complete": activity["complete"] and not errors, "positions_page_complete": positions["complete"],
                "gaps": activity["gaps"], "invalid_rows": errors, "late_events_possible": True,
                "snapshot_atomic": False, "financial_complete": False, "purpose": account.get("purpose"),
                "mm2_order_attribution": False}
    return {"events": events, "snapshot": snapshot, "coverage": coverage, "quality": quality("PARTIAL", reasons),
            "activity_rows": activity["rows"], "position_rows": positions["rows"]}


def update_watermark(path, account_id, end, bundle_id, complete, overlap_seconds=600):
    """Coverage cursor only. No financial/finality claim; monotonic after publication."""
    if complete is not True:
        return False
    if not isinstance(bundle_id, str) or len(bundle_id) != 64 or any(c not in "0123456789abcdef" for c in bundle_id):
        raise ResearchError("invalid publication id for cursor")
    if type(overlap_seconds) is not int or not 0 <= overlap_seconds <= 86400:
        raise ResearchError("invalid observer overlap policy")
    path = Path(path)
    state = load_json(path) if path.exists() else {"schema_version": VERSION, "accounts": {}}
    if state.get("schema_version") != VERSION or not isinstance(state.get("accounts"), dict):
        raise ResearchError("observer cursor schema changed")
    address(account_id)
    previous = state["accounts"].get(account_id)
    if previous and utc(previous["covered_through_utc"]) >= utc(end):
        return False
    state["accounts"][account_id] = {"covered_through_utc": end, "bundle_id": bundle_id,
        "overlap_seconds": overlap_seconds, "financial_complete": False}
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".pending")
    if temporary.exists() or path.is_symlink():
        raise ResearchError("cursor pending/symlink requires investigation")
    with temporary.open("xb") as stream:
        stream.write(canonical(state) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="UTC whole-second start inclusive")
    parser.add_argument("--end", required=True, help="UTC whole-second end exclusive")
    parser.add_argument("--out", required=True, help="archive directory outside Git/runtime state")
    parser.add_argument("--registry")
    parser.add_argument("--account", action="append", help="registered account name; default both public accounts")
    parser.add_argument("--state", help="optional coverage cursor file, outside immutable bundles")
    parser.add_argument("--overlap-seconds", type=int, default=600)
    parser.add_argument("--max-bytes", type=int, default=16_000_000)
    parser.add_argument("--max-requests", type=int, default=240)
    parser.add_argument("--max-seconds", type=int, default=120)
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--source-revision", default="unknown")
    args = parser.parse_args()
    window(args.start, args.end)
    if not 0 <= args.overlap_seconds <= 86400:
        raise ResearchError("overlap outside bounded one-day policy")
    start = args.start
    if args.state and Path(args.state).exists():
        state = load_json(args.state)
        cursors = [utc(item["covered_through_utc"]) for item in state.get("accounts", {}).values()]
        if cursors:
            lookback = int(min(cursors).timestamp()) - args.overlap_seconds
            start = epoch_utc(min(int(utc(start).timestamp()), max(0, lookback)))
    registry = load_registry(args.registry)
    accounts = [a for a in registry["accounts"] if not args.account or a["name"] in args.account]
    if not accounts or (args.account and set(args.account) - {a["name"] for a in accounts}):
        raise ResearchError("unknown account selection")
    client = PublicHTTP(args.max_bytes, args.max_requests, args.max_seconds)
    outputs = []
    for account in accounts:
        first_receipt = len(client.receipts)
        observed = observe_account(client, account, start, args.end, max_rows=args.max_rows)
        files = {"events.jsonl": observed["events"], "closing.json": observed["snapshot"],
                 "coverage.json": observed["coverage"], "source-activity.jsonl": observed["activity_rows"],
                 "source-positions.jsonl": observed["position_rows"], "requests.jsonl": client.receipts[first_receipt:],
                 "registry.json": {"schema_version": VERSION, "accounts": [account]}}
        published = publish_bundle(args.out, files, scope={"account_id": account["account_id"], "lane": "public-benchmark",
            "kind": "one-public-account", "chain_id": 137}, utc_range=window(start, args.end), quality=observed["quality"],
            source_revision=args.source_revision, config_hash=digest(registry))
        if args.state:
            update_watermark(args.state, account["account_id"], args.end, published.name,
                             observed["coverage"]["activity_complete"], args.overlap_seconds)
        outputs.append({"account": account["name"], "bundle": str(published), "quality": observed["quality"],
                        "coverage": observed["coverage"]})
    print(canonical({"bundles": outputs, "requests": client.request_count, "bytes": client.bytes_read}).decode())


if __name__ == "__main__":
    main()
