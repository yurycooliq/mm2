#!/usr/bin/env python3
"""Bounded concurrent public CLOB/Binance capture; no auth or order routes."""

import argparse
import asyncio
import collections
import contextlib
import hashlib
import json
import logging
import math
import pathlib
import re
import ssl
import time
import urllib.parse

import feed_latency_common as common


CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_BASE = "wss://stream.binance.com:9443/stream?streams="
SUMMARY_RESERVE = 8192


class Recorder:
    def __init__(self, args, artifact, clock):
        self.args, self.artifact, self.clock = args, artifact, clock
        self.deadline = time.perf_counter() + args.deadline
        self.events = self.messages = self.incoming_bytes = self.output_bytes = 0
        self.stop_reason = None
        self.seen = set()
        self.identities = {}
        self.last_aggregate = {}
        self.last_receive = {}
        self.stats = {feed: collections.Counter() for feed in ("clob", "binance")}
        self.interarrival = {feed: [] for feed in self.stats}
        self.processing = {feed: [] for feed in self.stats}

    def emit(self, row, final=False):
        text = json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
        size = len(text.encode("utf-8"))
        limit = self.args.max_bytes if final else self.args.max_bytes - SUMMARY_RESERVE
        if self.output_bytes + size > limit:
            self.stop_reason = self.stop_reason or "output_byte_budget"
            return False
        self.output_bytes += size
        if self.artifact is not None:
            self.artifact.write(text)
        return True

    def status(self, feed, kind, **fields):
        return self.emit({"type": "status", "feed": feed, "kind": kind,
                          **self.clock.stamp(), **fields})

    def stopped(self):
        if self.stop_reason is None and time.perf_counter() >= self.deadline:
            self.stop_reason = "deadline"
        return self.stop_reason is not None

    def receive(self, feed, message, generation):
        stamp = self.clock.stamp()
        started = time.perf_counter_ns()
        self.messages += 1
        self.incoming_bytes += len(message.encode("utf-8") if isinstance(message, str) else message)
        self.stats[feed]["messages"] += 1
        if self.messages > self.args.max_messages or self.incoming_bytes > self.args.max_bytes:
            self.stop_reason = "message_budget" if self.messages > self.args.max_messages else "incoming_byte_budget"
            return
        try:
            normalized = common.normalize_message(feed, message, set(self.args.tokens), set(self.args.symbols))
        except (ValueError, KeyError, TypeError, UnicodeError, RecursionError):
            self.stats[feed]["invalid_messages"] += 1
            self.status(feed, "invalid_message")
            return
        if not normalized:
            self.stats[feed]["non_event_messages"] += 1
        for value in normalized:
            if self.events >= self.args.max_events:
                self.stop_reason = "event_budget"
                break
            self.events += 1
            self.stats[feed]["events"] += 1
            duplicate = value["event_hash"] in self.seen
            self.stats[feed]["duplicates"] += int(duplicate)
            self.seen.add(value["event_hash"])
            old_hash = self.identities.setdefault(value["identity"], value["event_hash"])
            if old_hash != value["event_hash"]:
                self.stats[feed]["identity_conflicts"] += 1
                self.status(feed, "identity_conflict", identity=value["identity"])
            aggregate = value["aggregate_id"]
            if aggregate is not None and not duplicate:
                key = value["instrument"]
                previous = self.last_aggregate.get(key)
                if previous is not None and aggregate > previous + 1:
                    self.stats[feed]["aggregate_id_gaps"] += 1
                    self.stats[feed]["aggregate_ids_skipped"] += aggregate - previous - 1
                    self.status(feed, "aggregate_id_gap", instrument=key,
                                previous=previous, current=aggregate,
                                potential_missing=aggregate - previous - 1)
                elif previous is not None and aggregate < previous:
                    self.stats[feed]["out_of_order"] += 1
                    self.status(feed, "aggregate_id_out_of_order", instrument=key,
                                previous=previous, current=aggregate)
                self.last_aggregate[key] = max(aggregate, previous or 0)
            prior = self.last_receive.get(feed)
            if prior is not None:
                self.interarrival[feed].append((stamp["receive_mono_ns"] - prior) / 1e6)
            self.last_receive[feed] = stamp["receive_mono_ns"]
            written = self.emit({"type": "event", **value, **stamp, "generation": generation,
                                 "duplicate": duplicate})
            self.stats[feed]["recorded_events"] += int(written)
            self.stats[feed]["artifact_events_dropped"] += int(not written)
            if self.stopped():
                break
        self.processing[feed].append((time.perf_counter_ns() - started) / 1e6)
        if self.events >= self.args.max_events:
            self.stop_reason = self.stop_reason or "event_budget"

    def summary(self, geo):
        self.clock.stamp()
        feeds = {}
        for feed in self.stats:
            feeds[feed] = {**dict(self.stats[feed]),
                           "interarrival_ms": common.HTTP.distribution(self.interarrival[feed]),
                           "processing_ms": common.HTTP.distribution(self.processing[feed]),
                           "gap_detection": "aggregate_id_discontinuities" if feed == "binance"
                           else "no_documented_sequence_number"}
        return {"type": "summary", "schema_version": common.SCHEMA_VERSION,
                "label": self.args.label, "geoblock": geo,
                "stop_reason": self.stop_reason or "completed", "events": self.events,
                "messages": self.messages, "incoming_bytes": self.incoming_bytes,
                "artifact_bytes_before_summary": self.output_bytes,
                "elapsed_ms": self.clock.stamp()["elapsed_ms"], "feeds": feeds,
                "clock_step_detected": self.clock.step_detected,
                "max_wall_mono_residual_ms": round(self.clock.max_residual_ms, 6),
                "measurement": "userspace message receipt; no one-way delay or trading latency"}


async def open_public_socket(url, deadline, timeout, max_message_bytes):
    from websockets.asyncio.client import connect
    parsed = urllib.parse.urlsplit(url)
    attempt_deadline = min(deadline, time.perf_counter() + timeout)
    sock = await asyncio.to_thread(common.HTTP.bounded_connect,
                                   (parsed.hostname, parsed.port or 443), attempt_deadline)
    try:
        remaining = common.HTTP.remaining(attempt_deadline)
        return await connect(url, sock=sock, proxy=None, compression=None,
                             open_timeout=remaining, close_timeout=.5,
                             ping_interval=None, max_size=max_message_bytes,
                             max_queue=4, ssl=ssl.create_default_context())
    except BaseException:
        sock.close()
        raise


async def capture_feed(feed, url, recorder, connector=open_public_socket):
    from websockets.exceptions import WebSocketException
    args = recorder.args
    disconnected_at = None
    for generation in range(args.max_reconnects + 1):
        if recorder.stopped():
            return
        websocket = None
        connect_started = time.perf_counter_ns()
        try:
            recorder.stats[feed]["connection_attempts"] += 1
            websocket = await asyncio.wait_for(
                connector(url, recorder.deadline, args.timeout, args.max_message_bytes),
                timeout=min(args.timeout, common.HTTP.remaining(recorder.deadline)),
            )
            recorder.stats[feed]["connections"] += 1
            recorder.status(feed, "connected", generation=generation,
                            connect_ms=(time.perf_counter_ns() - connect_started) / 1e6,
                            observed_disconnect_ms=(time.perf_counter_ns() - disconnected_at) / 1e6
                            if disconnected_at is not None else None)
            disconnected_at = None
            if feed == "clob":
                await asyncio.wait_for(websocket.send(json.dumps({"assets_ids": args.tokens, "type": "market"})),
                                       timeout=min(args.timeout, common.HTTP.remaining(recorder.deadline)))
            next_ping = time.perf_counter() + 10
            silence_since = time.perf_counter()
            gap_reported = False
            while not recorder.stopped():
                now = time.perf_counter()
                if feed == "clob" and now >= next_ping:
                    await asyncio.wait_for(websocket.send("PING"),
                                           timeout=min(args.timeout, common.HTTP.remaining(recorder.deadline)))
                    recorder.stats[feed]["heartbeats_sent"] += 1
                    next_ping = now + 10
                try:
                    message = await asyncio.wait_for(websocket.recv(),
                                                    timeout=min(1, common.HTTP.remaining(recorder.deadline)))
                except TimeoutError:
                    if not gap_reported and (time.perf_counter() - silence_since) * 1000 >= args.gap_ms:
                        recorder.stats[feed]["silence_intervals"] += 1
                        recorder.status(feed, "observation_silence_not_proven_loss",
                                        silence_ms=(time.perf_counter() - silence_since) * 1000)
                        gap_reported = True
                    continue
                recorder.receive(feed, message, generation)
                silence_since, gap_reported = time.perf_counter(), False
        except (OSError, WebSocketException) as error:
            recorder.stats[feed]["failures"] += 1
            recorder.stats[feed]["error_" + type(error).__name__] += 1
            if disconnected_at is None:
                disconnected_at = time.perf_counter_ns()
            recorder.status(feed, "connection_failure", generation=generation,
                            error=type(error).__name__)
        finally:
            if websocket is not None:
                with contextlib.suppress(OSError, WebSocketException, TimeoutError):
                    await asyncio.wait_for(websocket.close(), timeout=.6)
        if recorder.stopped():
            return
        if generation < args.max_reconnects:
            recorder.stats[feed]["reconnects"] += 1
            await asyncio.sleep(min(.5 * (generation + 1), max(0, recorder.deadline - time.perf_counter())))
    recorder.stop_reason = recorder.stop_reason or "feed_unavailable"


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--tokens", required=True, help="comma-separated explicit CLOB token IDs, at most four")
    parser.add_argument("--symbols", default="btcusdt", help="at most two lowercase Binance symbols")
    parser.add_argument("--deadline", type=float, default=60, help="whole run seconds, maximum 600")
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--max-events", type=int, default=10000)
    parser.add_argument("--max-messages", type=int, default=10000)
    parser.add_argument("--max-bytes", type=int, default=8 * 1024**2,
                        help="separate ceilings for incoming message bytes and artifact bytes, maximum 64MiB")
    parser.add_argument("--max-message-bytes", type=int, default=256 * 1024)
    parser.add_argument("--max-reconnects", type=int, default=2)
    parser.add_argument("--gap-ms", type=float, default=3000)
    parser.add_argument("--clock-offset-ms", type=float, default=0,
                        help="operator measured local clock minus UTC; comparison subtracts it")
    parser.add_argument("--clock-uncertainty-ms", type=float,
                        help="operator conservative UTC error bound for the entire run; unknown by default")
    parser.add_argument("--clock-source", default="unknown")
    parser.add_argument("--clock-step-ms", type=float, default=10)
    parser.add_argument("--jsonl", help="opt-in new capture under this checkout's ignored output/")
    args = parser.parse_args(argv)
    try:
        args.label = common.label(args.label)
        args.clock_source = common.label(args.clock_source)
        args.tokens = list(dict.fromkeys(common.token(t) for t in args.tokens.split(",")))
        args.symbols = list(dict.fromkeys(args.symbols.split(",")))
        if not 1 <= len(args.tokens) <= 4 or not 1 <= len(args.symbols) <= 2:
            raise ValueError("at most four tokens and two symbols")
        if any(not re.fullmatch(r"[a-z0-9]{3,20}", s) for s in args.symbols):
            raise ValueError("invalid symbol")
        if args.jsonl:
            args.jsonl = common.HTTP.output_path(args.jsonl)
        values = [args.deadline, args.timeout, args.gap_ms, args.clock_offset_ms, args.clock_step_ms]
        if args.clock_uncertainty_ms is not None:
            values.append(args.clock_uncertainty_ms)
        if (not all(math.isfinite(v) for v in values) or not 0 < args.deadline <= 600
                or not 0 < args.timeout <= 30 or not 100 <= args.gap_ms <= 60000
                or not 0 < args.clock_step_ms <= 1000 or abs(args.clock_offset_ms) > 60000
                or not 1 <= args.max_events <= 100000 or not 1 <= args.max_messages <= 100000
                or not 65536 <= args.max_bytes <= 64 * 1024**2
                or not 1024 <= args.max_message_bytes <= min(args.max_bytes, 1024**2)
                or not 0 <= args.max_reconnects <= 3):
            raise ValueError("invalid finite capture budget")
        if args.clock_uncertainty_ms is not None and (
                not 0 <= args.clock_uncertainty_ms <= 60000 or args.clock_source == "unknown"):
            raise ValueError("clock bound requires a named operator clock source")
    except ValueError as error:
        parser.error(str(error))
    return args


async def run(args, artifact=None, connector=open_public_socket, geo_fetch=None):
    import websockets
    clock = common.Clock(args.clock_offset_ms, args.clock_uncertainty_ms,
                         args.clock_source, args.clock_step_ms)
    recorder = Recorder(args, artifact, clock)
    source_hash = hashlib.sha256(pathlib.Path(common.__file__).read_bytes()).hexdigest()
    recorder.emit({"type": "config", "schema_version": common.SCHEMA_VERSION,
                   "canonical_version": common.CANONICAL_VERSION, "canonical_source_sha256": source_hash,
                   "websockets_version": websockets.__version__, "label": args.label,
                   "tokens": sorted(args.tokens), "symbols": sorted(args.symbols), "clock": clock.metadata(),
                   "deadline_s": args.deadline, "max_events": args.max_events,
                   "max_messages": args.max_messages, "max_bytes": args.max_bytes,
                   "max_reconnects": args.max_reconnects, "started": clock.anchor})

    def fetch_geoblock():
        connection = common.HTTP.ProbeConnection("polymarket.com", ssl.create_default_context())
        try:
            row, body = common.HTTP.sample(connection, "/api/geoblock", "geoblock", args.timeout, recorder.deadline)
            return common.HTTP.geoblock_result(row, body), row["error"]
        finally:
            connection.close()

    geo, geo_error = await asyncio.to_thread(geo_fetch or fetch_geoblock)
    recorder.emit({"type": "geoblock", **geo, "error": geo_error})
    if geo_error or geo["blocked"] is not False:
        recorder.stop_reason = "geoblock_unavailable" if geo_error else "geoblock_blocked"
    else:
        binance_url = BINANCE_BASE + "/".join(symbol + "@aggTrade" for symbol in args.symbols)
        await asyncio.gather(capture_feed("clob", CLOB_URL, recorder, connector),
                             capture_feed("binance", binance_url, recorder, connector))
    summary = recorder.summary(geo)
    recorder.emit(summary, final=True)
    if artifact is not None:
        artifact.flush()
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all(recorder.stats[f]["events"] for f in recorder.stats) and recorder.stop_reason != "feed_unavailable" else 1


def main(argv=None):
    args = arguments(argv)
    try:
        import websockets  # noqa: F401
    except ImportError:
        raise SystemExit("websockets is unavailable; use the prepared measurement environment") from None
    logging.getLogger("websockets").disabled = True
    logging.getLogger("websockets.client").disabled = True
    if args.jsonl:
        args.jsonl.parent.mkdir(parents=True, exist_ok=True)
        with args.jsonl.open("x", encoding="utf-8") as artifact:
            return asyncio.run(run(args, artifact))
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
