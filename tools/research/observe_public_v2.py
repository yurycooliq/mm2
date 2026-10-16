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
from decimal import Decimal, DecimalException, Inexact, localcontext
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
PNL_ENDPOINT = BASE + "/v2/user-pnl"
PNL_KEYS = {"economic_pnl_usd": "economic_pnl", "realized_pnl_usd": "realized_pnl",
            "unrealized_pnl_usd": "unrealized_pnl", "wallet_income_usd": "wallet_income"}
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
        self.pnl_evidence = None

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
        if endpoint not in {"activity", "positions", "user-pnl"}:
            raise ResearchError("V2 public GET endpoint not allowed")
        allowed = ({"user", "interval", "fidelity"} if endpoint == "user-pnl" else
            {"user", "start", "end", "limit", "cursor", "sort_by", "sort_direction", "exclude_deposits_withdrawals"}
            if endpoint == "activity" else {"user", "status", "limit", "cursor", "sort_by", "sort_direction", "filter_type", "filter_amount", "include_archived"})
        if not isinstance(parameters, dict) or set(parameters) - allowed or len(canonical(parameters)) > 6144:
            raise ResearchError("V2 public query outside allowlist")
        address(parameters.get("user"))
        if endpoint == "user-pnl" and parameters != {"user": parameters["user"], "interval": "1d", "fidelity": "1h"}:
            raise ResearchError("V2 PnL query outside fixed documented window")
        api_path = "/v2/" + endpoint
        url = BASE + api_path + "?" + urllib.parse.urlencode(parameters)
        attempts = 1 if endpoint == "user-pnl" else 3
        for attempt in range(attempts):
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
                    if endpoint == "user-pnl":
                        receipt = {"endpoint": endpoint, "api_version": 2, "api_path": api_path,
                            "source_url": PNL_ENDPOINT, "parameters": parameters, "bytes": len(data),
                            "response_sha256": hashlib.sha256(data).hexdigest(), "observed_at_utc": utc_now()}
                        self.receipts.append(receipt)
                        self.pnl_evidence = {"raw_body_utf8": None, "receipt": receipt}
                        if len(data) > 200_000:
                            raise ResearchError("V2 PnL evidence record bound exceeded")
                        try:
                            self.pnl_evidence["raw_body_utf8"] = data.decode("utf-8")
                        except UnicodeDecodeError:
                            raise ResearchError("V2 PnL response not UTF8") from None
                        return parse_json(data, api=True), receipt
                    value = envelope(parse_json(data, api=True))
                    receipt = {"endpoint": endpoint, "api_version": 2, "api_path": api_path, "source_url": BASE + api_path,
                        "parameters": parameters, "bytes": len(data), "response_sha256": hashlib.sha256(data).hexdigest(),
                        "observed_at_utc": utc_now(), "rows": len(value["data"]),
                        "pagination": {key: value["pagination"][key] for key in ("has_more", "next_cursor", "limit", "offset")}}
                    self.receipts.append(receipt)
                    return value, receipt
            except urllib.error.HTTPError as error:
                if error.code not in {429, 500, 502, 503, 504} or attempt == attempts - 1:
                    raise ResearchError("V2 public API HTTP failure") from None
                delay = min(2**attempt, max(0, self.deadline - time.monotonic()))
                if delay:
                    time.sleep(delay)
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException):
                raise ResearchError("V2 public transport unavailable") from None


def empty_reported_pnl(account_id, status="UNAVAILABLE", reason="PNL_SOURCE_UNAVAILABLE"):
    return {"status": status, "evidence_class": "venue-reported-wallet-estimate", "wallet": account_id,
        "endpoint": PNL_ENDPOINT, "interval": "1d", "fidelity": "1h", "source_fidelity": None,
        "source_at_ms": None, "response_sha256": None, "latest_at_ms": None,
        "latest": {key: None for key in PNL_KEYS}, "period": {"start_ms": None, "end_ms": None},
        "change_economic_pnl_usd": None, "series": [], "warnings": [reason]}


def _pnl_amount(value):
    if value is None:
        return None
    # OpenAPI says JSON numeric USDC; strings or binary floats are schema changes.
    if type(value) is not int and not isinstance(value, Decimal):
        raise ResearchError("V2 PnL numeric schema changed")
    # An extreme exponent must not underflow to a fabricated zero inside the
    # shared fixed-point converter's Decimal context.
    with localcontext() as context:
        context.traps[Inexact] = True
        amount = int(units(value, PICO, "reported PnL USDC", signed=True))
    sign, (whole, fraction) = ("-" if amount < 0 else ""), divmod(abs(amount), PICO)
    return sign + str(whole) + (("." + str(fraction).zfill(12).rstrip("0")) if fraction else "")


def project_reported_pnl(evidence, account_id):
    """Verify original bounded response/receipt; never establish ledger completeness."""
    result = empty_reported_pnl(account_id)
    if not isinstance(evidence, dict):
        return result
    if evidence.get("status") != "AVAILABLE":
        return empty_reported_pnl(account_id, "INVALID" if evidence.get("status") == "INVALID" else "UNAVAILABLE",
                                  "PNL_SOURCE_INVALID" if evidence.get("status") == "INVALID" else "PNL_SOURCE_UNAVAILABLE")
    try:
        raw, receipt = evidence.get("raw_body_utf8"), evidence.get("receipt")
        if not isinstance(raw, str) or len(raw.encode("utf-8")) > 200_000 or not isinstance(receipt, dict):
            raise ResearchError("V2 PnL original evidence missing")
        body = raw.encode("utf-8")
        if (receipt.get("endpoint") != "user-pnl" or receipt.get("api_version") != 2
                or receipt.get("api_path") != "/v2/user-pnl" or receipt.get("source_url") != PNL_ENDPOINT
                or receipt.get("parameters") != {"user": account_id, "interval": "1d", "fidelity": "1h"}
                or receipt.get("bytes") != len(body) or receipt.get("response_sha256") != hashlib.sha256(body).hexdigest()):
            raise ResearchError("V2 PnL source receipt mismatch")
        observed = utc(receipt["observed_at_utc"])
        observed_ms = int(observed.timestamp() * 1000)
        value = parse_json(body, api=True)
        if not isinstance(value, dict) or set(value) != {"data"} or not isinstance(value["data"], dict):
            raise ResearchError("V2 PnL envelope changed")
        data = value["data"]
        if (set(data) != {"proxy_wallet", "interval", "fidelity", "source_fidelity", "points"}
                or data.get("proxy_wallet") != account_id or data.get("interval") != "1d"
                or data.get("fidelity") != "1h" or data.get("source_fidelity") != "1d"
                or not isinstance(data.get("points"), list) or not 2 <= len(data["points"]) <= 64):
            raise ResearchError("V2 PnL identity/window changed")
        required = {"timestamp", "source_block", "realized_market_pnl", "realized_lp_pnl", "realized_combo_pnl",
                    "realized_pnl", "volume", "volume_usdc", "trade_count"}
        numeric = {"cashflow_net", "deposits", "economic_pnl", "fees", "fees_paid", "fees_refunded", "maker_rebate",
                   "position_pnl", "referral_income", "reward_income", "settled_pnl", "sponsored_income",
                   "taker_rebate", "trade_pnl", "unrealized_pnl", "wallet_income", "withdrawals", "yield_income",
                   "realized_market_pnl", "realized_lp_pnl", "realized_combo_pnl", "realized_pnl", "volume", "volume_usdc"}
        previous = None
        for point in data["points"]:
            if not isinstance(point, dict) or not required <= point.keys() or point.keys() - (numeric | {"timestamp", "source_block", "trade_count"}):
                raise ResearchError("V2 PnL point schema changed")
            if any(type(point.get(key)) is not int or point[key] < 0 for key in ("timestamp", "source_block", "trade_count")):
                raise ResearchError("V2 PnL source clock/count changed")
            at = int(utc(epoch_utc(point["timestamp"])).timestamp() * 1000)
            if not observed_ms - 86_400_000 <= at <= observed_ms + 5000:
                raise ResearchError("V2 PnL point outside requested rolling window")
            if previous is not None and (point["timestamp"] - previous["timestamp"] != 3600
                    or point["source_block"] < previous["source_block"] or point["trade_count"] < previous["trade_count"]):
                raise ResearchError("V2 PnL point clock/order discontinuity")
            for key in numeric & point.keys():
                _pnl_amount(point[key])
            if any(point[key] is None for key in required - {"timestamp", "source_block", "trade_count"}):
                raise ResearchError("V2 PnL required source value missing")
            for target, terms in (("realized_pnl", ("realized_market_pnl", "realized_lp_pnl", "realized_combo_pnl")),
                                  ("position_pnl", ("realized_pnl", "unrealized_pnl")),
                                  ("economic_pnl", ("position_pnl", "wallet_income")),
                                  ("settled_pnl", ("realized_pnl", "wallet_income"))):
                if point.get(target) is not None and all(point.get(key) is not None for key in terms):
                    if units(point[target], PICO, signed=True) != str(sum(int(units(point[key], PICO, signed=True)) for key in terms)):
                        raise ResearchError("V2 PnL reported composition inconsistent")
            result["series"].append({"at_ms": at, **{key: _pnl_amount(point.get(source)) for key, source in PNL_KEYS.items()}})
            previous = point
        if observed_ms - result["series"][-1]["at_ms"] > 3_600_000:
            raise ResearchError("V2 PnL current baseline unavailable")
        result.update(status="AVAILABLE", source_fidelity="1d", source_at_ms=observed_ms,
                      response_sha256=receipt["response_sha256"], latest_at_ms=result["series"][-1]["at_ms"],
                      latest={key: result["series"][-1][key] for key in PNL_KEYS},
                      period={"start_ms": result["series"][0]["at_ms"], "end_ms": result["series"][-1]["at_ms"]},
                      warnings=["PNL_ESTIMATE_NOT_AUTHENTICATED_BOT_RESULT", "PNL_DAILY_SOURCE_ON_HOURLY_GRID"])
        if any(point.get(key) is None for point in data["points"] for key in ("deposits", "withdrawals", "cashflow_net")):
            result["warnings"].append("PNL_CASHFLOWS_UNKNOWN")
        return result
    except (ResearchError, DecimalException, ValueError, TypeError, KeyError, OverflowError):
        return empty_reported_pnl(account_id, "INVALID", "PNL_SOURCE_INVALID")


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


def observe_account(client, account, start, end, *, page_size=500, max_rows=MAX_ROWS, include_reported_pnl=False):
    account_id = address(account.get("account_id"))
    if type(page_size) is not int or not 1 <= page_size <= 1000 or type(max_rows) is not int or not 1 <= max_rows <= MAX_ROWS:
        raise ResearchError("V2 public row/page quota invalid")
    if type(include_reported_pnl) is not bool:
        raise ResearchError("V2 optional PnL flag must be boolean")
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
    result = {"events": events, "snapshot": snapshot, "coverage": coverage, "quality": quality("PARTIAL", reasons),
              "activity_rows": activity["rows"], "position_rows": positions["rows"]}
    if include_reported_pnl:
        evidence = {"status": "UNAVAILABLE", "reason": "PNL_SOURCE_UNAVAILABLE", "raw_body_utf8": None, "receipt": None}
        try:
            client.get("user-pnl", {"user": account_id, "interval": "1d", "fidelity": "1h"})
            captured = getattr(client, "pnl_evidence", None)
            if captured:
                evidence.update(captured, status="AVAILABLE", reason=None)
                projected = project_reported_pnl(evidence, account_id)
                evidence["status"] = projected["status"]
                evidence["reason"] = "PNL_SOURCE_INVALID" if projected["status"] == "INVALID" else None
        except (ResearchError, DecimalException, OSError, ValueError, TypeError, KeyError):
            captured = getattr(client, "pnl_evidence", None)
            if captured:
                evidence.update(captured, status="INVALID", reason="PNL_SOURCE_INVALID")
        result["reported_pnl_evidence"] = evidence
    return result
