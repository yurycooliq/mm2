"""Bounded, read-only private-journal projection. No primary locks, writes or cleanup.

Only a writer's post-sync watermark or a verified sealed boundary permits export.
Checkpoint projection is not a complete trade/cashflow ledger. Errors omit raw input.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile


class ExportError(ValueError):
    pass


@dataclass(frozen=True)
class ExportLimits:
    max_input_bytes: int = 64 * 1024 * 1024
    max_output_bytes: int = 64 * 1024 * 1024
    max_records: int = 100_000
    max_record_bytes: int = 1_048_576


ZERO_HASH = "0" * 64
HASH = re.compile(r"[0-9a-f]{64}\Z")
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
LIVE_REASONS = {
    "startup_paused_recovery", "relayer_dispatch_before_send_once", "fault", "owner_control",
    "signed_submission_intent", "post_result", "cancel_response_requires_query",
    "authoritative_account_reconciliation", "foreign_orders_pause",
    "signed_chain_intent_before_broadcast", "relayer_request_acknowledged",
    "finalized_chain_receipt_capital_reconciliation", "expired_relayer_batch_without_economic_credit",
    "cancel_intent", "shutdown_pause", "owner_handle_dropped_pause", "private_disconnect_pause",
}
LEDGER_FIELDS = {
    "phase", "orders", "positions", "position_floor", "receipts", "signed_intents",
    "collateral_micros", "allowance_micros", "initial_collateral_micros", "session_spent_pico",
    "daily_loss_micros", "loss_day", "foreign_order_ids", "fault", "owned_positions",
    "closeout_markets", "chain_pending", "chain_receipts", "chain_expiries",
    "returned_collateral_micros", "realized_cost_pico", "session_gas_cost_wei",
}
OP_FIELDS = {
    "market": {"condition_id", "start_ms", "end_ms"},
    "advance": {"now_ms"}, "quotes": {"now_ms", "quotes"}, "cancel_all": {"now_ms"},
    "book": {"now_ms", "outcome", "bid", "ask", "ahead_micros"},
    "trade": {"now_ms", "outcome", "worst_price_micros", "quantity_micros"},
    "observed_trade": {"now_ms", "outcome", "price_micros", "quantity_micros", "fingerprint"},
    "venue_rules": {"now_ms", "tick_micros", "min_order_micros"},
    "demo_resolution": {"condition_id", "winning_outcome", "at_ms"},
    "limits": {"now_ms", "limits"},
}
LIMIT_FIELDS = {"initial_collateral_usd", "max_imbalance_shares", "lot_shares", "half_width", "max_feed_age_ms"}


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def sha(value):
    return hashlib.sha256(value).hexdigest()


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ExportError("duplicate JSON field")
        value[key] = item
    return value


def parse(raw):
    try:
        return json.loads(raw, object_pairs_hook=_unique,
                          parse_constant=lambda _: (_ for _ in ()).throw(ExportError("nonfinite JSON")))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ExportError("invalid evidence JSON") from error


def integer(value, maximum=2**128 - 1):
    if type(value) is not int or not 0 <= value <= maximum:
        raise ExportError("invalid exact unsigned integer")
    return value


def uint_string(value):
    if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]{0,77}", value):
        raise ExportError("invalid exact integer string")
    integer(int(value), 2**256 - 1)
    return value


def identity(value):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ExportError("invalid public identity")
    return value


def digest(value):
    if not isinstance(value, str) or not HASH.fullmatch(value):
        raise ExportError("invalid evidence digest")
    return value


def fields(value, allowed, required=()):
    if not isinstance(value, dict) or not set(value) <= allowed or not set(required) <= set(value):
        raise ExportError("unsupported evidence schema")
    return value


def enum(value, allowed):
    if not isinstance(value, str) or value not in allowed:
        raise ExportError("unsupported evidence enum")
    return value


def _safe_scalar(key, value):
    if value is None:
        return None
    if key in {"outcome", "winning_outcome"}:
        return enum(value, {"up", "down"})
    if key in {"condition_id", "slot", "fingerprint", "market_slug", "order_id"}:
        return identity(value)
    if key in {"token_id", "cost_pico"}:
        return uint_string(value)
    if key in {"redeemable", "mergeable", "neg_risk"}:
        if type(value) is not bool:
            raise ExportError("invalid boolean")
        return value
    return integer(value)


def _limits(value):
    fields(value, LIMIT_FIELDS, LIMIT_FIELDS)
    result = {}
    for key, item in value.items():
        if key == "max_feed_age_ms":
            result[key] = integer(item)
        elif not isinstance(item, str) or not re.fullmatch(r"[0-9]{1,15}(?:\.[0-9]{1,12})?", item):
            raise ExportError("invalid exact decimal")
        else:
            result[key] = item
    return result


def _operation(value):
    if not isinstance(value, dict) or value.get("kind") not in OP_FIELDS:
        raise ExportError("unsupported paper operation")
    kind = value["kind"]
    required = {"kind", "condition_id"} if kind == "market" else OP_FIELDS[kind] | {"kind"}
    fields(value, OP_FIELDS[kind] | {"kind"}, required)
    result = {"kind": kind}
    for key, item in value.items():
        if key == "kind":
            continue
        if key == "limits":
            result[key] = _limits(item)
        elif key == "quotes":
            if not isinstance(item, list) or len(item) > 16:
                raise ExportError("quote projection exceeds bound")
            quote_fields = {"slot", "outcome", "price_micros", "quantity_micros"}
            result[key] = [{k: _safe_scalar(k, v) for k, v in fields(q, quote_fields, quote_fields).items()} for q in item]
        else:
            result[key] = _safe_scalar(key, item)
    return result


def _engine_events(entry):
    if not isinstance(entry, dict):
        raise ExportError("unsupported engine entry")
    kind = entry.get("kind")
    if kind == "bootstrap":
        fields(entry, {"kind", "limits"}, {"kind", "limits"})
        return [("engine_bootstrap_config", {"limits": _limits(entry["limits"])}, None)]
    if kind not in {"execution", "command"}:
        raise ExportError("unsupported engine entry")
    allowed = {"kind", "operations"} | ({"command", "receipt"} if kind == "command" else set())
    fields(entry, allowed, allowed)
    result = []
    if kind == "command":
        command = entry["command"]
        if not isinstance(command, dict):
            raise ExportError("unsupported owner command")
        command_kind = enum(command.get("kind"), {"control", "limits"})
        cf = {"kind", "id", "expected_version", "action" if command_kind == "control" else "limits"}
        fields(command, cf, cf)
        safe = {"kind": command_kind, "id": identity(command["id"]), "expected_version": integer(command["expected_version"])}
        if command_kind == "control":
            safe["action"] = enum(command["action"], {"pause", "resume", "kill", "reset"})
        else:
            safe["limits"] = _limits(command["limits"])
        receipt = fields(entry["receipt"], {"command_id", "applied_version", "mode_at_ack"}, {"command_id", "applied_version", "mode_at_ack"})
        safe_receipt = {"command_id": identity(receipt["command_id"]), "applied_version": integer(receipt["applied_version"]),
                        "mode_at_ack": enum(receipt["mode_at_ack"], {"paused", "shadow", "killed", "simulation", "trading", "both", "live"})}
        result.append(("owner_receipt", {"command": safe, "receipt": safe_receipt}, None))
    operations = entry["operations"]
    if not isinstance(operations, list) or len(operations) > 32:
        raise ExportError("operation projection exceeds bound")
    for op in operations:
        safe = _operation(op)
        result.append(("paper_replay_input", safe, safe.get("now_ms", safe.get("at_ms"))))
    return result


def _live_projection(record):
    reason = enum(record["reason"], LIVE_REASONS)
    ledger = fields(record["ledger"], LEDGER_FIELDS, {"phase", "orders", "positions", "collateral_micros", "session_spent_pico"})
    orders, positions = ledger["orders"], ledger["positions"]
    if not isinstance(orders, dict) or not isinstance(positions, dict) or len(orders) > 256 or len(positions) > 4096:
        raise ExportError("live projection collection exceeds bound")
    safe = {"phase": enum(ledger["phase"], {"paused", "running", "killed", "faulted"}), "orders": [], "positions": []}
    for key in ("collateral_micros", "allowance_micros", "initial_collateral_micros", "returned_collateral_micros", "daily_loss_micros", "loss_day"):
        if key in ledger:
            safe[key] = None if ledger[key] is None else integer(ledger[key])
    for key in ("session_spent_pico", "realized_cost_pico", "session_gas_cost_wei"):
        if key in ledger:
            safe[key] = "0" if ledger[key] == "" else uint_string(ledger[key])
    reserve = 0
    order_fields = {"order_id", "quote", "phase", "cumulative_filled_micros", "created_at_ms", "last_operation_ms", "retry_at_ms"}
    quote_fields = {"slot", "condition_id", "market_slug", "token_id", "outcome", "price_micros", "quantity_micros", "window_start_ms", "window_end_ms", "tick_micros", "min_order_micros", "fee_bps", "protocol", "neg_risk"}
    for order in orders.values():
        fields(order, order_fields, order_fields)
        quote = fields(order["quote"], quote_fields, quote_fields)
        q = {k: _safe_scalar(k, v) for k, v in quote.items()}
        phase = enum(order["phase"], {"prepared", "posting", "live", "cancelling", "unknown_post", "unknown_cancel", "closed"})
        filled = integer(order["cumulative_filled_micros"])
        if filled > q["quantity_micros"] or q["fee_bps"] > 10_000:
            raise ExportError("invalid live commitment")
        if phase != "closed":
            base = (q["quantity_micros"] - filled) * q["price_micros"]
            reserve += base + (base * q["fee_bps"] + 9999) // 10_000
        safe["orders"].append({"order_id": identity(order["order_id"]), "quote": q, "phase": phase,
                               "cumulative_filled_micros": filled, "created_at_ms": integer(order["created_at_ms"]),
                               "last_operation_ms": integer(order["last_operation_ms"])})
    position_fields = {"condition_id", "token_id", "outcome", "quantity_micros", "cost_pico", "redeemable", "mergeable"}
    for position in positions.values():
        safe["positions"].append({k: _safe_scalar(k, v) for k, v in fields(position, position_fields, position_fields).items()})
    # Private subtrees, free-form fault strings and signed wire material are never traversed/copied.
    return {"reason": reason, "state": safe, "reserved_upper_bound_pico": str(reserve),
            "classifications": {"state": "derived", "reserved_upper_bound_pico": "bound",
                                "session_spent_pico": "bound", "position_cost_pico": "bound",
                                "native_trade_ids": "unavailable", "fill_timestamps": "unavailable",
                                "cashflow_completeness": "unavailable"}}


def validate_watermark(value, limits):
    fields(value, {"schema_version", "source_kind", "ledger_id", "segment", "byte_offset", "next_sequence", "last_hash", "manifest_bytes", "account_identity", "sealed"},
           {"schema_version", "source_kind", "ledger_id", "segment", "byte_offset", "next_sequence", "last_hash"})
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ExportError("unsupported committed prefix version")
    kind = enum(value["source_kind"], {"engine", "live"})
    digest(value["ledger_id"])
    digest(value["last_hash"])
    integer(value["byte_offset"], limits.max_input_bytes)
    integer(value["next_sequence"])
    segment = fields(value["segment"], {"name", "index", "start_sequence", "previous_hash"}, {"name", "start_sequence", "previous_hash"})
    integer(segment["start_sequence"])
    digest(segment["previous_hash"])
    if kind == "engine":
        if segment["name"] != "commands.jsonl":
            raise ExportError("invalid engine segment name")
        integer(segment.get("index"), 4096)
    elif segment["name"] != f"live-{segment['start_sequence']:016}.jsonl":
        raise ExportError("invalid live segment name")
    if value["next_sequence"] < segment["start_sequence"]:
        raise ExportError("invalid committed sequence range")
    if "sealed" in value and type(value["sealed"]) is not bool:
        raise ExportError("invalid seal flag")
    if "manifest_bytes" in value:
        integer(value["manifest_bytes"], 2 * 1024 * 1024)
    if "account_identity" in value:
        digest(value["account_identity"])
    return value


def _read_file(path):
    if path.is_symlink():
        raise ExportError("linked journal evidence refused")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise ExportError("nonregular journal evidence refused")
    return os.fdopen(fd, "rb")


def _records(source, watermark, limits):
    segment = watermark["segment"]
    if watermark["source_kind"] == "engine":
        archive = source / f"commands-{segment['index']:08}.jsonl"
        path = archive if archive.exists() else source / "commands.jsonl"
        if watermark.get("sealed") and not archive.exists():
            raise ExportError("sealed archive unavailable; pending rotation is not exportable")
    else:
        path = source / segment["name"]
    expected, previous = segment["start_sequence"], segment["previous_hash"]
    remaining, count = watermark["byte_offset"], 0
    with _read_file(path) as stream:
        while remaining:
            line = stream.readline(min(remaining, limits.max_record_bytes) + 1)
            if not line or len(line) > min(remaining, limits.max_record_bytes) or not line.endswith(b"\n"):
                raise ExportError("partial or oversized committed record")
            remaining -= len(line)
            record = parse(line)
            core = {"sequence", "previous_hash", "hash"}
            body_fields = {"entry"} if watermark["source_kind"] == "engine" else {"reason", "ledger"}
            fields(record, core | body_fields, core | body_fields)
            integer(record["sequence"])
            digest(record["hash"])
            digest(record["previous_hash"])
            body = [record["entry"]] if watermark["source_kind"] == "engine" else [record["reason"], record["ledger"]]
            if record["sequence"] != expected or record["previous_hash"] != previous or record["hash"] != sha(encoded([expected, previous, *body])):
                raise ExportError("committed journal hash or sequence mismatch")
            expected += 1
            previous = record["hash"]
            count += 1
            if count > limits.max_records:
                raise ExportError("record budget exceeded")
            yield record
    if expected != watermark["next_sequence"] or previous != watermark["last_hash"]:
        raise ExportError("committed prefix boundary mismatch")


def _sync_dir(path):
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def discover_sealed(source, source_kind, ledger_id, limits=ExportLimits()):
    """Return conservative immutable boundaries; live has no explicit seal manifest.

    The final live segment is always excluded. A nonempty hash-linked successor proves
    that the writer moved on; a last empty successor is conservatively left unexported.
    Source permissions/producer identity remain an independent authenticity gate.
    """
    digest(ledger_id)
    source = Path(source).resolve(strict=True)
    result = []
    if source_kind == "engine":
        previous, start, index, consumed = ZERO_HASH, 0, 0, 0
        with _read_file(source / "journal-manifest.jsonl") as stream:
            while line := stream.readline(1025):
                consumed += len(line)
                if consumed > 2 * 1024 * 1024 or len(line) > 1024 or not line.endswith(b"\n") or index >= 4096:
                    raise ExportError("seal manifest bound exceeded")
                seal = fields(parse(line), {"segment", "bytes", "next_sequence", "last_hash", "hash"}, {"segment", "bytes", "next_sequence", "last_hash", "hash"})
                if seal["segment"] != index or seal["next_sequence"] <= start or seal["hash"] != sha(encoded([index, seal["bytes"], seal["next_sequence"], seal["last_hash"]])):
                    raise ExportError("invalid sealed segment contract")
                digest(seal["last_hash"])
                integer(seal["bytes"], limits.max_input_bytes)
                archive = source / f"commands-{index:08}.jsonl"
                if not archive.exists():
                    # A synced final intent may precede the rename; only writer recovery completes it.
                    if stream.read(1):
                        raise ExportError("historical sealed archive is missing")
                    break
                if archive.is_symlink() or archive.stat().st_size != seal["bytes"]:
                    raise ExportError("archive does not match seal")
                result.append({"schema_version": 1, "source_kind": "engine", "ledger_id": ledger_id,
                               "segment": {"name": "commands.jsonl", "index": index, "start_sequence": start, "previous_hash": previous},
                               "byte_offset": seal["bytes"], "next_sequence": seal["next_sequence"], "last_hash": seal["last_hash"], "sealed": True})
                previous, start, index = seal["last_hash"], seal["next_sequence"], index + 1
    elif source_kind == "live":
        names = []
        for item in source.iterdir():
            if item.name.startswith("live-") and item.name.endswith(".jsonl"):
                if not re.fullmatch(r"live-[0-9]{16}\.jsonl", item.name):
                    raise ExportError("noncanonical live segment")
                names.append(item.name)
                if len(names) > 256:
                    raise ExportError("live segment count exceeded")
        names.sort()
        previous = ZERO_HASH
        for index, name in enumerate(names[:-1]):
            start = int(name[5:-6])
            successor = source / names[index + 1]
            with _read_file(successor) as stream:
                first = stream.readline(limits.max_record_bytes + 1)
            if not first:
                break
            if len(first) > limits.max_record_bytes or not first.endswith(b"\n"):
                raise ExportError("invalid live successor")
            first_record = parse(first)
            next_sequence = int(names[index + 1][5:-6])
            if not isinstance(first_record, dict) or first_record.get("sequence") != next_sequence:
                raise ExportError("live successor sequence mismatch")
            fields(first_record, {"sequence", "previous_hash", "hash", "reason", "ledger"},
                   {"sequence", "previous_hash", "hash", "reason", "ledger"})
            if first_record["hash"] != sha(encoded([next_sequence, first_record["previous_hash"], first_record["reason"], first_record["ledger"]])):
                raise ExportError("live successor hash mismatch")
            last_hash = digest(first_record.get("previous_hash"))
            size = (source / name).stat().st_size
            prefix = {"schema_version": 1, "source_kind": "live", "ledger_id": ledger_id,
                      "segment": {"name": name, "start_sequence": start, "previous_hash": previous},
                      "byte_offset": size, "next_sequence": next_sequence, "last_hash": last_hash, "sealed": True}
            validate_watermark(prefix, limits)
            # Verify private predecessor records now, without publishing/copying their contents.
            for _ in _records(source, prefix, limits):
                pass
            result.append(prefix)
            previous = last_hash
    else:
        raise ExportError("unsupported journal source")
    return result


def export_prefix(source, destination, watermark, source_id, limits=ExportLimits()):
    """Create one atomic sanitized bundle; failure leaves only an incomplete staging directory."""
    identity(source_id)
    validate_watermark(watermark, limits)
    if Path(source).is_symlink():
        raise ExportError("linked journal directory refused")
    source = Path(source).resolve(strict=True)
    destination = Path(destination)
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    if source == destination or source in destination.parents or destination in source.parents or destination.is_symlink():
        raise ExportError("export destination must be outside primary state")
    publication = {"source_id": source_id, "watermark": watermark, "projection_version": 1}
    publication_id = sha(encoded(publication))
    if destination.exists():
        manifest = verify_bundle(destination, limits)
        if manifest["publication_id"] != publication_id:
            raise ExportError("destination belongs to another publication")
        return manifest
    staging = Path(tempfile.mkdtemp(prefix=".incomplete-export-", dir=parent))
    os.chmod(staging, 0o700)
    (staging / "INCOMPLETE").write_text("uncommitted analytical export\n", encoding="utf-8")
    output_bytes, events, records = 0, 0, 0
    output_hash = hashlib.sha256()
    with (staging / "events.jsonl").open("xb") as output:
        for record in _records(source, watermark, limits):
            records += 1
            if watermark["source_kind"] == "engine":
                projections = _engine_events(record["entry"])
            else:
                projections = [("ledger_checkpoint_projection", _live_projection(record), None)]
            for index, (kind, data, event_time) in enumerate(projections):
                event_id = sha(encoded([source_id, watermark["ledger_id"], record["sequence"], index, 1]))
                event = {"schema_version": 1, "event_id": event_id, "source_id": source_id,
                         "ledger_id": watermark["ledger_id"], "source_kind": watermark["source_kind"],
                         "source_seq": record["sequence"], "operation_index": index,
                         "source_record_hash": record["hash"], "kind": kind, "evidence_class": "derived",
                         "classifications": {"durable_record": "observed", "projection": "derived", "native_fill_identity": "unavailable"},
                         "event_time_ms": event_time, "event_time_provenance": "paper_input_clock" if event_time is not None else "unavailable",
                         "native_fill_identity": None, "data": data}
                line = encoded(event) + b"\n"
                output_bytes += len(line)
                if output_bytes > limits.max_output_bytes:
                    raise ExportError("analytical output byte budget exceeded")
                output.write(line)
                output_hash.update(line)
                events += 1
        output.flush()
        os.fsync(output.fileno())
    manifest = {"schema_version": 1, "publication_id": publication_id, **publication,
                "source_records": records, "event_count": events,
                "objects": [{"name": "events.jsonl", "bytes": output_bytes, "sha256": output_hash.hexdigest()}],
                "projection_complete": True, "financial_effects_complete": False,
                "quality": "PARTIAL", "quality_reasons": ["projection_is_not_a_complete_actual_fill_cashflow_ledger"],
                "authenticity": "requires_trusted_local_watermark_and_producer_access_policy"}
    with (staging / "manifest.json").open("xb") as output:
        output.write(encoded(manifest) + b"\n")
        output.flush()
        os.fsync(output.fileno())
    (staging / "INCOMPLETE").unlink()
    _sync_dir(staging)
    # A racing writer must never silently replace an existing nonempty bundle.
    try:
        os.rename(staging, destination)
    except OSError:
        (staging / "INCOMPLETE").write_text("uncommitted analytical export\n", encoding="utf-8")
        raise
    _sync_dir(parent)
    return manifest


def verify_bundle(path, limits=ExportLimits()):
    path = Path(path)
    if path.is_symlink() or (path / "INCOMPLETE").exists():
        raise ExportError("incomplete or linked analytical bundle")
    with _read_file(path / "manifest.json") as stream:
        raw = stream.read(65_537)
    if len(raw) > 65_536:
        raise ExportError("manifest budget exceeded")
    manifest = parse(raw)
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1 or not isinstance(manifest.get("objects"), list) or len(manifest["objects"]) != 1:
        raise ExportError("unsupported analytical manifest")
    digest(manifest.get("publication_id"))
    identity(manifest.get("source_id"))
    validate_watermark(manifest.get("watermark"), limits)
    if manifest.get("projection_version") != 1 or manifest["publication_id"] != sha(encoded({
        "source_id": manifest["source_id"], "watermark": manifest["watermark"], "projection_version": 1
    })):
        raise ExportError("publication identity hash mismatch")
    if {item.name for item in path.iterdir()} != {"manifest.json", "events.jsonl"}:
        raise ExportError("unexpected analytical bundle members")
    item = fields(manifest["objects"][0], {"name", "bytes", "sha256"}, {"name", "bytes", "sha256"})
    if item["name"] != "events.jsonl":
        raise ExportError("unsupported analytical object")
    integer(item["bytes"], limits.max_output_bytes)
    digest(item["sha256"])
    h, size = hashlib.sha256(), 0
    with _read_file(path / item["name"]) as stream:
        while chunk := stream.read(1_048_576):
            size += len(chunk)
            if size > limits.max_output_bytes:
                raise ExportError("analytical object budget exceeded")
            h.update(chunk)
    if size != item["bytes"] or h.hexdigest() != item["sha256"]:
        raise ExportError("analytical object hash mismatch")
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--watermark", required=True, help="Trusted safe post-sync metadata JSON, never credentials")
    parser.add_argument("--source-id", required=True, help="Registry-pinned globally unique source identity")
    args = parser.parse_args(argv)
    with _read_file(Path(args.watermark)) as stream:
        raw = stream.read(65_537)
    if len(raw) > 65_536:
        raise ExportError("watermark budget exceeded")
    manifest = export_prefix(args.source, args.destination, parse(raw), args.source_id)
    print(json.dumps({"publication_id": manifest["publication_id"], "event_count": manifest["event_count"], "quality": manifest["quality"]}))


if __name__ == "__main__":
    try:
        main()
    except (ExportError, OSError):
        raise SystemExit("Journal export failed; preserve incomplete staging evidence and inspect bounded health diagnostics.")
