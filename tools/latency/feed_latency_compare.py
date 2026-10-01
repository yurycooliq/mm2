#!/usr/bin/env python3
"""Compare identical canonical public WS events across two bounded captures."""

import argparse
import collections
import json
import math
import pathlib
import re

import feed_latency_common as common


def capture(path, max_bytes):
    path = pathlib.Path(path)
    if path.stat().st_size > max_bytes:
        raise ValueError("input byte budget exceeded")
    config = summary = None
    events = {}
    identities = collections.defaultdict(set)
    duplicates = collections.Counter()
    ignored = collections.Counter()
    ambiguous = set()
    with path.open("rb") as source:
        for index in range(200001):
            line = source.readline(16385)
            if not line:
                break
            if len(line) > 16384:
                raise ValueError("input line budget exceeded")
            try:
                row = json.loads(line)
            except (ValueError, UnicodeError, RecursionError):
                raise ValueError("invalid capture JSONL") from None
            if not isinstance(row, dict):
                raise ValueError("invalid capture row")
            kind = row.get("type")
            if summary is not None:
                raise ValueError("rows after terminal summary")
            if kind == "config":
                if config is not None or index != 0:
                    raise ValueError("duplicate or misplaced capture config")
                config = row
            elif kind == "summary":
                summary = row
            elif kind == "event":
                feed = row.get("feed")
                event_hash = row.get("event_hash")
                identity = row.get("identity")
                if (feed not in ("clob", "binance") or not isinstance(event_hash, str)
                        or not re.fullmatch("[0-9a-f]{64}", event_hash)
                        or not isinstance(identity, str) or len(identity) > 350):
                    raise ValueError("invalid canonical event identity")
                common.integer(row.get("receive_utc_ns"))
                common.integer(row.get("receive_mono_ns"))
                uncertainty = row.get("clock_sample_uncertainty_ms")
                if (isinstance(uncertainty, bool) or not isinstance(uncertainty, (float, int))
                        or not math.isfinite(uncertainty) or not 0 <= uncertainty <= 1000):
                    raise ValueError("invalid clock sample uncertainty")
                eligible = row.get("comparison_eligible") is True and (
                    (feed == "clob" and row.get("event_type") == "price_change")
                    or (feed == "binance" and row.get("event_type") == "aggTrade"))
                if not eligible:
                    ignored[feed] += 1
                    continue
                identities[identity].add(event_hash)
                if event_hash in events:
                    duplicates[feed] += 1
                    if feed == "clob":
                        ambiguous.add(event_hash)
                else:
                    events[event_hash] = row
            elif kind not in ("status", "geoblock"):
                raise ValueError("unknown capture row")
        else:
            raise ValueError("input row budget exceeded")
    if not isinstance(config, dict) or not isinstance(summary, dict):
        raise ValueError("capture config or terminal summary missing")
    if config.get("schema_version") != common.SCHEMA_VERSION or summary.get("schema_version") != common.SCHEMA_VERSION:
        raise ValueError("capture schema version mismatch")
    if config.get("canonical_version") != common.CANONICAL_VERSION:
        raise ValueError("canonical version mismatch")
    if (not isinstance(config.get("canonical_source_sha256"), str)
            or not re.fullmatch("[0-9a-f]{64}", config["canonical_source_sha256"])):
        raise ValueError("canonical source fingerprint missing")
    common.label(config.get("label"))
    if not isinstance(config.get("tokens"), list) or not isinstance(config.get("symbols"), list):
        raise ValueError("subscription config missing")
    if not 1 <= len(config["tokens"]) <= 4 or not 1 <= len(config["symbols"]) <= 2:
        raise ValueError("invalid subscription count")
    for value in config["tokens"]:
        common.token(value)
    for value in config["symbols"]:
        if not isinstance(value, str) or not re.fullmatch("[a-z0-9]{3,20}", value):
            raise ValueError("invalid symbol")
    return {"config": config, "summary": summary, "events": events,
            "identities": identities, "duplicates": duplicates, "ignored": ignored,
            "ambiguous": ambiguous}


def clock_status(value):
    clock = value["config"].get("clock")
    if not isinstance(clock, dict):
        return "clock_metadata_missing"
    uncertainty = clock.get("uncertainty_ms")
    offset = clock.get("offset_ms")
    if uncertainty is None or clock.get("source") in (None, "unknown"):
        return "unknown_clock_bound"
    if (any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
            for v in (uncertainty, offset)) or not 0 <= uncertainty <= 60000 or abs(offset) > 60000):
        return "invalid_clock_bound"
    if value["summary"].get("clock_step_detected") is not False:
        return "clock_step_detected_or_missing"
    residual = value["summary"].get("max_wall_mono_residual_ms")
    if (isinstance(residual, bool) or not isinstance(residual, (int, float))
            or not math.isfinite(residual) or not 0 <= residual <= uncertainty):
        return "clock_drift_exceeds_declared_bound"
    return "operator_bounded"


def diagnostics(value):
    permitted = ("messages", "events", "recorded_events", "duplicates", "failures",
                 "connections", "connection_attempts", "reconnects", "silence_intervals",
                 "aggregate_id_gaps", "aggregate_ids_skipped", "invalid_messages",
                 "identity_conflicts", "artifact_events_dropped", "out_of_order")
    feeds = value["summary"].get("feeds", {})
    clean = {}
    for feed in ("clob", "binance"):
        counts = feeds.get(feed, {}) if isinstance(feeds, dict) else {}
        clean[feed] = {name: counts[name] for name in permitted
                       if isinstance(counts, dict) and name in counts
                       and isinstance(counts[name], int) and not isinstance(counts[name], bool)
                       and 0 <= counts[name] <= 1000000000}
    reason = value["summary"].get("stop_reason")
    if reason not in ("completed", "deadline", "event_budget", "message_budget",
                      "incoming_byte_budget", "output_byte_budget", "feed_unavailable",
                      "geoblock_unavailable", "geoblock_blocked"):
        reason = "unknown_capture_status"
    return {"stop_reason": reason, "feeds": clean,
            "duplicates_in_comparison_events": dict(value["duplicates"]),
            "ignored_non_identifiable_events": dict(value["ignored"]),
            "ambiguous_recurring_clob_payloads": len(value["ambiguous"])}


def compare(left, right):
    if left["config"]["label"] == right["config"]["label"]:
        raise ValueError("captures require distinct host/session labels")
    for field in ("canonical_version", "canonical_source_sha256", "tokens", "symbols"):
        if left["config"].get(field) != right["config"].get(field):
            raise ValueError("capture normalization or subscriptions differ")
    statuses = {"a": clock_status(left), "b": clock_status(right)}
    timing_allowed = all(status == "operator_bounded" for status in statuses.values())
    output = {"type": "comparison", "a": left["config"]["label"], "b": right["config"]["label"],
              "clock_status": statuses, "timing_status": "operator_bounded" if timing_allowed else "unavailable",
              "interpretation": "positive B-minus-A means A observed the same event earlier; not one-way delay",
              "captures": {name: diagnostics(value)
                           for name, value in (("a", left), ("b", right))}, "feeds": {}}
    for feed in ("clob", "binance"):
        a = {h for h, r in left["events"].items() if r["feed"] == feed}
        b = {h for h, r in right["events"].items() if r["feed"] == feed}
        common_hashes = a & b
        ambiguous_hashes = left["ambiguous"] | right["ambiguous"]
        conflict_hashes = set()
        identities = left["identities"].keys() | right["identities"].keys()
        for identity in identities:
            hashes = left["identities"].get(identity, set()) | right["identities"].get(identity, set())
            if len(hashes) > 1:
                conflict_hashes.update(hashes)
        matches = common_hashes - (ambiguous_hashes | conflict_hashes)
        deltas, uncertainties = [], []
        earlier = collections.Counter()
        for event_hash in matches:
            ar, br = left["events"][event_hash], right["events"][event_hash]
            if timing_allowed:
                ac, bc = left["config"]["clock"], right["config"]["clock"]
                delta = ((br["receive_utc_ns"] - round(bc["offset_ms"] * 1e6))
                         - (ar["receive_utc_ns"] - round(ac["offset_ms"] * 1e6))) / 1e6
                bound = (ac["uncertainty_ms"] + bc["uncertainty_ms"]
                         + ar["clock_sample_uncertainty_ms"] + br["clock_sample_uncertainty_ms"])
                deltas.append(delta)
                uncertainties.append(bound)
                earlier["a_definitely_earlier" if delta > bound else "b_definitely_earlier"
                        if delta < -bound else "within_clock_uncertainty"] += 1
        conflicts = sum(1 for identity in left["identities"].keys() & right["identities"].keys()
                        if left["identities"][identity] != right["identities"][identity]
                        and identity.startswith("aggTrade:" if feed == "binance" else "price_change:"))
        output["feeds"][feed] = {
            "unique_a": len(a), "unique_b": len(b), "matched_unique": len(matches),
            "matched_payload_hashes": len(common_hashes),
            "ambiguous_matched_payload_hashes": len(common_hashes & ambiguous_hashes),
            "excluded_conflicting_identity_hashes": len(common_hashes & conflict_hashes),
            "excluded_ambiguous_a": len(a & left["ambiguous"]),
            "excluded_ambiguous_b": len(b & right["ambiguous"]),
            "unmatched_a": len(a - b), "unmatched_b": len(b - a),
            "identity_payload_conflicts": conflicts,
            "arrival_delta_b_minus_a": common.HTTP.distribution(deltas),
            "max_combined_clock_uncertainty_ms": max(uncertainties) if uncertainties else None,
            "arrival_order_counts": dict(earlier),
        }
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("a")
    parser.add_argument("b")
    parser.add_argument("--max-input-bytes", type=int, default=64 * 1024**2)
    parser.add_argument("--jsonl", help="opt-in new comparison under ignored output/")
    args = parser.parse_args(argv)
    if not 65536 <= args.max_input_bytes <= 64 * 1024**2:
        parser.error("input budget must be 64KiB..64MiB per capture")
    try:
        result = compare(capture(args.a, args.max_input_bytes), capture(args.b, args.max_input_bytes))
        if args.jsonl:
            output = common.HTTP.output_path(args.jsonl)
            output.parent.mkdir(parents=True, exist_ok=True)
            with output.open("x", encoding="utf-8") as target:
                target.write(json.dumps(result, sort_keys=True) + "\n")
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, "comparison failed: " + type(error).__name__ + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
