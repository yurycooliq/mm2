"""Bounded public-only Data API V2 observer; no offset or V1 fallback.

Official contracts checked 2026-10-08:
https://docs.polymarket.com/api-reference/feeds/list-account-activity
https://docs.polymarket.com/api-reference/wallet/list-positions-for-a-user-or-market
https://docs.polymarket.com/migrate/data-api-v1-to-v2

Activity size/current_size are human shares; usdc_size/current_value are human
USDC. Entry basis is report-only and fee-exclusive. Public fees/PnL are never
promoted to authenticated ledger facts. Schema errors abort publication.
"""
from collections import Counter
import hashlib
import http.client
import time
import urllib.error
import urllib.parse
import urllib.request

from .observe_mo_money import (BASE, NoRedirect, _exact_public, display_title,
                             normalize_activity, position_snapshot)
from .schema import (MICRO, PICO, ResearchError, address, canonical, condition,
                     digest, epoch_utc, parse_json, quality, units, utc, utc_now, validate_event, window)

MAX_BYTES, MAX_REQUESTS, MAX_SECONDS, MAX_ROWS = 16_000_000, 120, 90, 10_000
ACTIVITY_MAPPING = {"proxy_wallet": "proxyWallet", "timestamp": "timestamp", "condition_id": "conditionId",
    "type": "type", "size": "size", "usdc_size": "usdcSize", "transaction_hash": "transactionHash",
    "price": "price", "token_id": "asset", "side": "side", "outcome_index": "outcomeIndex",
    "outcome": "outcome", "is_combo": "isCombo"}
POSITION_MAPPING = {"proxy_wallet": "proxyWallet", "token_id": "asset", "condition_id": "conditionId",
    "current_size": "size", "avg_price": "avgPrice", "entry_cost_usdc": "initialValue",
    "current_value": "currentValue", "current_price": "curPrice", "outcome": "outcome",
    "outcome_index": "outcomeIndex", "redeemable": "redeemable", "mergeable": "mergeable",
    "negative_risk": "negativeRisk", "end_date": "endDate"}


def envelope(value):
    if (not isinstance(value, dict) or set(value) != {"data", "pagination"}
            or not isinstance(value["data"], list) or len(value["data"]) > 1000
            or any(not isinstance(row, dict) for row in value["data"])):
        raise ResearchError("V2 public envelope schema changed")
    paging = value["pagination"]
    if (not isinstance(paging, dict) or type(paging.get("has_more")) is not bool
            or type(paging.get("limit")) is not int or not 1 <= paging["limit"] <= 1000
            or type(paging.get("offset")) is not int or paging["offset"] < 0
            or "next_cursor" not in paging):
        raise ResearchError("V2 public pagination schema changed")
    cursor = paging["next_cursor"]
    if cursor is not None and (not isinstance(cursor, str) or not 0 < len(cursor) <= 4096 or any(ord(c) < 33 or ord(c) > 126 for c in cursor)):
        raise ResearchError("V2 public cursor malformed")
    if paging["has_more"] != (cursor is not None):
        raise ResearchError("V2 public cursor completeness conflict")
    if len(value["data"]) > paging["limit"]:
        raise ResearchError("V2 public page exceeds limit")
    return value


class PublicHTTPV2:
    def __init__(self, max_bytes=MAX_BYTES, max_requests=MAX_REQUESTS, max_seconds=MAX_SECONDS, opener=None):
        for value, maximum in ((max_bytes, MAX_BYTES), (max_requests, MAX_REQUESTS), (max_seconds, MAX_SECONDS)):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ResearchError("V2 capture budget outside hard limit")
        self.max_bytes, self.max_requests = max_bytes, max_requests
        self.deadline = time.monotonic() + max_seconds
        self.opener = opener or urllib.request.build_opener(NoRedirect()).open
        self.bytes_read, self.request_count, self.receipts = 0, 0, []

    def _body(self, response, limit):
        # read1 performs one underlying buffered read, so a trickling response
        # cannot hide indefinitely inside read(n). Tighten the native urllib
        # socket timeout to the remaining budget before each bounded chunk.
        reader = getattr(response, "read1", response.read)
        native_socket = getattr(getattr(getattr(response, "fp", None), "raw", None), "_sock", None)
        pieces, length = [], 0
        while True:
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise ResearchError("V2 public capture time budget exhausted")
            if native_socket is not None:
                native_socket.settimeout(min(15, remaining))
            chunk = reader(min(65_536, limit + 1 - length))
            self.bytes_read += len(chunk)
            length += len(chunk)
            if length > limit:
                raise ResearchError("V2 public response byte limit exceeded")
            if time.monotonic() > self.deadline:
                raise ResearchError("V2 public capture time budget exhausted")
            if not chunk:
                return b"".join(pieces)
            pieces.append(chunk)

    def get(self, endpoint, parameters):
        if endpoint not in {"activity", "positions"}:
            raise ResearchError("V2 public GET endpoint not allowed")
        allowed = ({"user", "start", "end", "limit", "cursor", "sort_by", "sort_direction", "exclude_deposits_withdrawals"}
            if endpoint == "activity" else {"user", "status", "limit", "cursor", "sort_by", "sort_direction", "filter_type", "filter_amount", "include_archived"})
        if not isinstance(parameters, dict) or set(parameters) - allowed or len(canonical(parameters)) > 6144:
            raise ResearchError("V2 public query outside allowlist")
        address(parameters.get("user"))
        api_path = "/v2/" + endpoint
        url = BASE + api_path + "?" + urllib.parse.urlencode(parameters)
        for attempt in range(3):
            remaining, budget = self.deadline - time.monotonic(), self.max_bytes - self.bytes_read
            if self.request_count >= self.max_requests or remaining <= 0 or budget <= 0:
                raise ResearchError("V2 public capture budget exhausted")
            self.request_count += 1
            limit = min(budget, 4_000_000)
            request = urllib.request.Request(url, headers={"Accept": "application/json", "Accept-Encoding": "identity", "User-Agent": "mm2-public-research/2"})
            try:
                with self.opener(request, timeout=min(15, remaining)) as response:
                    if getattr(response, "status", 200) != 200:
                        raise ResearchError("V2 public API non-success status")
                    data = self._body(response, limit)
                    value = envelope(parse_json(data, api=True))
                    receipt = {"endpoint": endpoint, "api_version": 2, "api_path": api_path, "source_url": BASE + api_path,
                        "parameters": parameters, "bytes": len(data), "response_sha256": hashlib.sha256(data).hexdigest(),
                        "observed_at_utc": utc_now(), "rows": len(value["data"]),
                        "pagination": {key: value["pagination"][key] for key in ("has_more", "next_cursor", "limit", "offset")}}
                    self.receipts.append(receipt)
                    return value, receipt
            except urllib.error.HTTPError as error:
                if error.code not in {429, 500, 502, 503, 504} or attempt == 2:
                    raise ResearchError("V2 public API HTTP failure") from None
                delay = min(2**attempt, max(0, self.deadline - time.monotonic()))
                if delay:
                    time.sleep(delay)
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException):
                raise ResearchError("V2 public transport unavailable") from None


def project_activity(raw, account_id):
    required = set(ACTIVITY_MAPPING) - {"is_combo"}
    if not isinstance(raw, dict) or not required.issubset(raw):
        raise ResearchError("V2 public activity financial schema changed")
    if address(raw["proxy_wallet"]) != account_id or type(raw["timestamp"]) is not int:
        raise ResearchError("V2 public activity identity or clock mismatch")
    epoch_utc(raw["timestamp"])
    if not isinstance(raw["type"], str) or not raw["type"] or len(raw["type"]) > 32:
        raise ResearchError("V2 public activity type malformed")
    if type(raw["outcome_index"]) is not int or not 0 <= raw["outcome_index"] <= 999:
        raise ResearchError("V2 public activity outcome index malformed")
    if "is_combo" in raw and type(raw["is_combo"]) is not bool:
        raise ResearchError("V2 public activity combo flag malformed")
    for name, scale in (("size", MICRO), ("usdc_size", PICO)):
        units(raw[name], scale, name)
    # Preserve reported average price precision without inventing a micro tick.
    units(raw["price"], PICO, "V2 reported price")
    for name in ("token_id", "transaction_hash", "side", "outcome", "condition_id"):
        if not isinstance(raw[name], str) or len(raw[name]) > 256:
            raise ResearchError("V2 public activity identity field malformed")
    if raw["condition_id"]:
        condition(raw["condition_id"])
    original = _exact_public({key: raw[key] for key in ACTIVITY_MAPPING if key in raw})
    projected = {destination: original[source] for source, destination in ACTIVITY_MAPPING.items() if source in original}
    return projected, original


def project_position(raw, account_id):
    if not isinstance(raw, dict) or not set(POSITION_MAPPING).issubset(raw):
        raise ResearchError("V2 public position financial schema changed")
    if address(raw["proxy_wallet"]) != account_id:
        raise ResearchError("V2 public position identity mismatch")
    condition(raw["condition_id"])
    if not isinstance(raw["token_id"], str) or not raw["token_id"] or len(raw["token_id"]) > 256:
        raise ResearchError("V2 public position token malformed")
    for name, scale in (("current_size", MICRO), ("current_value", PICO), ("entry_cost_usdc", PICO), ("avg_price", PICO), ("current_price", PICO)):
        units(raw[name], scale, name)
    for name in ("redeemable", "mergeable", "negative_risk"):
        if type(raw[name]) is not bool:
            raise ResearchError("V2 public position flag malformed")
    if raw.get("status") not in {"OPEN", "REDEEMABLE", "MERGEABLE"}:
        raise ResearchError("V2 public position outside requested OPEN spine")
    if (not isinstance(raw["outcome"], str) or len(raw["outcome"]) > 128
            or type(raw["outcome_index"]) is not int or not 0 <= raw["outcome_index"] <= 999
            or not isinstance(raw["end_date"], str) or len(raw["end_date"]) > 64):
        raise ResearchError("V2 public position outcome/date malformed")
    original = _exact_public({key: raw[key] for key in POSITION_MAPPING})
    projected = {destination: original[source] for source, destination in POSITION_MAPPING.items()}
    for name in ("entry_fees_usdc", "total_cost_usdc", "total_size", "realized_pnl", "unrealized_pnl", "total_pnl"):
        if name in raw:
            units(raw[name], MICRO if name == "total_size" else PICO, name, signed=name.endswith("pnl"))
            original[name] = _exact_public(raw[name])
    original["status"] = raw["status"]
    if "archived" in raw:
        if type(raw["archived"]) is not bool:
            raise ResearchError("V2 public archived flag malformed")
        original["archived"] = raw["archived"]
    return projected, original


def _walk(client, endpoint, parameters, projector, *, max_rows, interval=None):
    rows, cursors, seen_positions, reasons = [], set(), set(), []
    cursor, last_time, complete = None, None, False
    for _ in range(MAX_REQUESTS):
        query = dict(parameters)
        if cursor is not None:
            query["cursor"] = cursor
        page, receipt = client.get(endpoint, query)
        page = envelope(page)
        for raw in page["data"]:
            if len(rows) >= max_rows:
                reasons.append("V2_CAPTURE_ROW_BUDGET")
                break
            projected, original = projector(raw, parameters["user"])
            if interval:
                at = projected["timestamp"]
                if not interval[0] <= at < interval[1] or (last_time is not None and at < last_time):
                    raise ResearchError("V2 public activity interval or ordering mismatch")
                last_time = at
            else:
                identity = projected["asset"]
                if identity in seen_positions:
                    reasons.append("V2_POSITIONS_SHIFTED_OR_DUPLICATE")
                    break
                seen_positions.add(identity)
            rows.append({"row": projected, "source_v2_row": original, "receipt_hash": digest(receipt),
                         "observed_at_utc": receipt["observed_at_utc"], "display_title": display_title(raw)})
        if reasons:
            break
        next_cursor = page["pagination"]["next_cursor"]
        if next_cursor is None:
            complete = True
            break
        if not page["data"] or next_cursor in cursors:
            reasons.append("V2_CURSOR_NO_PROGRESS")
            break
        cursors.add(next_cursor)
        cursor = next_cursor
    else:
        reasons.append("V2_CAPTURE_REQUEST_BUDGET")
    return {"rows": rows, "complete": complete, "reasons": reasons}


def observe_account(client, account, start, end, *, page_size=500, max_rows=MAX_ROWS):
    account_id = address(account.get("account_id"))
    if type(page_size) is not int or not 1 <= page_size <= 1000 or type(max_rows) is not int or not 1 <= max_rows <= MAX_ROWS:
        raise ResearchError("V2 public row/page quota invalid")
    start_dt, end_dt = utc(start), utc(end)
    window(start, end)
    if start_dt.microsecond or end_dt.microsecond or (end_dt-start_dt).total_seconds() > 31*86400:
        raise ResearchError("V2 activity window requires whole seconds and at most 31 days")
    lo, hi = int(start_dt.timestamp()), int(end_dt.timestamp())
    activity = _walk(client, "activity", {"user": account_id, "start": lo, "end": hi-1,
        "limit": page_size, "sort_by": "TIMESTAMP", "sort_direction": "ASC", "exclude_deposits_withdrawals": "false"},
        project_activity, max_rows=max_rows, interval=(lo, hi))
    remaining = max_rows - len(activity["rows"])
    if remaining:
        positions = _walk(client, "positions", {"user": account_id, "status": "OPEN", "limit": page_size,
            "sort_by": "TOKENS", "sort_direction": "ASC", "filter_type": "TOKENS", "filter_amount": "0", "include_archived": "true"},
            project_position, max_rows=min(4096, remaining))
    else:
        positions = {"rows": [], "complete": False, "reasons": ["V2_CAPTURE_TOTAL_ROW_BUDGET"]}
    events, errors, occurrences = [], [], Counter()
    for item in activity["rows"]:
        fingerprint = digest(item["row"])
        occurrence = occurrences[fingerprint]
        occurrences[fingerprint] += 1
        event = normalize_activity(item, account_id, occurrence)
        event["source"]["kind"] = "polymarket-activity-v2"
        event["source"]["original_record_hash"] = digest(item["source_v2_row"])
        events.append(validate_event(event))
    reasons = ["STARTING_CAPITAL_UNKNOWN", "PUBLIC_CASH_UNKNOWN", "PUBLIC_ACTIVITY_RECONCILIATION_REQUIRED",
               "V2_DEFAULT_FEED_EXCLUDES_OPT_IN_TIPS", "V2_OPEN_POSITIONS_NOT_COMPLETE_WALLET_LIFECYCLE",
               "PUBLIC_POSITION_ACCOUNTING_UNAUTHENTICATED"]
    if not activity["complete"]:
        reasons.extend(["ACTIVITY_INCOMPLETE", *activity["reasons"]])
    if not positions["complete"]:
        reasons.extend(["POSITIONS_INCOMPLETE", *positions["reasons"]])
    if account.get("identity_verified_at") is None:
        reasons.append("PROFILE_ATTRIBUTION_REVERIFY_REQUIRED")
    if account.get("purpose") == "owner-account-reconciliation":
        reasons.append("PUBLIC_OWNER_ACTIVITY_MM2_ATTRIBUTION_UNPROVEN")
    snapshot = position_snapshot(positions, account_id)
    snapshot["source_api_version"] = 2
    coverage = {"activity_complete": activity["complete"], "positions_page_complete": positions["complete"],
        "gaps": [{"start": start, "end": end, "reason": reason} for reason in activity["reasons"]], "invalid_rows": errors,
        "late_events_possible": True, "snapshot_atomic": False, "financial_complete": False,
        "purpose": account.get("purpose"), "mm2_order_attribution": False, "api_version": 2,
        "position_spine": "OPEN", "tips_requested": False}
    return {"events": events, "snapshot": snapshot, "coverage": coverage, "quality": quality("PARTIAL", reasons),
            "activity_rows": activity["rows"], "position_rows": positions["rows"]}
