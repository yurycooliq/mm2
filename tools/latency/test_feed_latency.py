"""Offline protocol and matched-event checks; no public network access."""

import asyncio
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import tempfile
import time
import unittest

import feed_latency_common as common
import feed_latency_compare as comparison
import feed_latency_probe as probe


def clob(token="1", public_hash="abcd", stamp=1780000000000):
    return {"event_type": "price_change", "market": "0x" + "a" * 64,
            "timestamp": str(stamp), "price_changes": [
                {"asset_id": token, "price": "0.50", "size": "10.0", "side": "BUY",
                 "hash": public_hash, "best_bid": "0.50", "best_ask": "0.51"}]}


def binance(aggregate=7):
    return {"e": "aggTrade", "E": 1780000000000, "T": 1779999999999, "s": "BTCUSDT",
            "a": aggregate, "f": aggregate * 2, "l": aggregate * 2 + 1,
            "p": "100.00", "q": "2.0", "m": True}


def configuration(name="a", bound=1, offset=0):
    return {"type": "config", "schema_version": 1, "canonical_version": common.CANONICAL_VERSION,
            "canonical_source_sha256": hashlib.sha256(pathlib.Path(common.__file__).read_bytes()).hexdigest(),
            "label": name, "tokens": ["1", "2"], "symbols": ["btcusdt"],
            "clock": {"offset_ms": offset, "uncertainty_ms": bound, "source": "fixture"}}


def event_row(offset_ns=0, value=None):
    value = value or common.normalize_binance(binance(), {"btcusdt"})[0]
    return {"type": "event", **value, "receive_utc_ns": 1780000000000000000 + offset_ns,
            "receive_mono_ns": 1000000000, "clock_sample_uncertainty_ms": .01,
            "duplicate": False}


def summary(step=False):
    return {"type": "summary", "schema_version": 1, "clock_step_detected": step,
            "max_wall_mono_residual_ms": .1, "stop_reason": "deadline",
            "feeds": {"clob": {"events": 3}, "binance": {"events": 3}}}


def loaded(config, rows, terminal=None):
    with tempfile.TemporaryDirectory() as directory:
        path = pathlib.Path(directory) / "capture.jsonl"
        path.write_text("\n".join(json.dumps(r) for r in [config, *rows, terminal or summary()]) + "\n")
        return comparison.capture(path, 65536)


class CanonicalTests(unittest.TestCase):
    def test_clob_hash_independent_of_batch_and_decimal_format(self):
        raw = clob()
        raw["price_changes"].extend(clob("2", "dcba")["price_changes"])
        batched = common.normalize_clob(raw, {"1", "2"})
        single = clob()
        single["price_changes"][0].update(price="0.5", size="10")
        normalized = common.normalize_clob(single, {"1"})
        self.assertEqual(batched[0]["event_hash"], normalized[0]["event_hash"])
        self.assertTrue(batched[0]["comparison_eligible"])

    def test_binance_aggregate_identity_and_two_source_clocks_retained(self):
        raw = binance()
        raw["authorization"] = "private-marker"
        value = common.normalize_binance({"stream": "btcusdt@aggTrade", "data": raw}, {"btcusdt"})[0]
        self.assertEqual(value["identity"], "aggTrade:btcusdt:7")
        self.assertEqual(value["exchange_event_ms"], 1780000000000)
        self.assertEqual(value["exchange_trade_ms"], 1779999999999)
        self.assertNotIn("private-marker", json.dumps(value))

    def test_different_price_levels_sharing_timestamp_and_book_hash_remain_distinct(self):
        raw = clob()
        second = {**raw["price_changes"][0], "price": "0.49", "size": "12"}
        raw["price_changes"].append(second)
        values = common.normalize_clob(raw, {"1"})
        self.assertEqual(len(values), 2)
        self.assertNotEqual(values[0]["identity"], values[1]["identity"])
        self.assertEqual(values[0]["delta"]["hash"], values[1]["delta"]["hash"])

    def test_missing_delta_hash_and_connection_snapshots_not_compared(self):
        raw = clob()
        raw["price_changes"][0].pop("hash")
        self.assertFalse(common.normalize_clob(raw, {"1"})[0]["comparison_eligible"])
        book = {"event_type": "book", "asset_id": "1", "market": "0xabc",
                "timestamp": "123", "hash": "abcd", "bids": [], "asks": []}
        self.assertFalse(common.normalize_clob(book, {"1"})[0]["comparison_eligible"])

    def test_unsubscribed_instruments_and_control_frames_ignored(self):
        self.assertEqual(common.normalize_clob(clob("3"), {"1"}), [])
        self.assertEqual(common.normalize_message("clob", "PONG", {"1"}, {"btcusdt"}), [])

    def test_malformed_timestamps_and_exponent_expansion_rejected(self):
        for value in ("1e-999999999", "NaN", "Infinity", "-1"):
            with self.assertRaises(ValueError):
                common.numeric(value)
        raw = clob()
        raw["timestamp"] = True
        with self.assertRaises(ValueError):
            common.normalize_clob(raw, {"1"})


class ComparisonTests(unittest.TestCase):
    def test_unknown_clock_has_matches_but_no_numeric_arrival_claim(self):
        a = loaded(configuration(bound=None), [event_row()])
        b = loaded(configuration("b"), [event_row(10000000)])
        result = comparison.compare(a, b)
        self.assertEqual(result["feeds"]["binance"]["matched_unique"], 1)
        self.assertEqual(result["timing_status"], "unavailable")
        self.assertIsNone(result["feeds"]["binance"]["arrival_delta_b_minus_a"]["p50_ms"])

    def test_offset_subtracted_and_uncertainty_preserved(self):
        a = loaded(configuration(), [event_row()])
        b = loaded(configuration("b", offset=20), [event_row(30000000)])
        result = comparison.compare(a, b)["feeds"]["binance"]
        self.assertEqual(result["arrival_delta_b_minus_a"]["p50_ms"], 10)
        self.assertEqual(result["arrival_order_counts"]["a_definitely_earlier"], 1)
        self.assertAlmostEqual(result["max_combined_clock_uncertainty_ms"], 2.02)
        self.assertIsNone(result["arrival_delta_b_minus_a"]["p99_ms"])

    def test_clock_step_and_declared_bound_smaller_than_drift_refuse_timing(self):
        a = loaded(configuration(), [event_row()], summary(step=True))
        b = loaded(configuration("b", bound=.01), [event_row()])
        result = comparison.compare(a, b)
        self.assertEqual(result["timing_status"], "unavailable")
        self.assertEqual(result["clock_status"]["b"], "clock_drift_exceeds_declared_bound")

    def test_duplicate_first_arrival_and_unmatched_counts_visible(self):
        other = common.normalize_binance(binance(8), {"btcusdt"})[0]
        a = loaded(configuration(), [event_row(), event_row(10000000), event_row(value=other)])
        b = loaded(configuration("b"), [event_row(30000000)])
        result = comparison.compare(a, b)
        self.assertEqual(result["feeds"]["binance"]["unmatched_a"], 1)
        self.assertEqual(result["feeds"]["binance"]["arrival_delta_b_minus_a"]["p50_ms"], 30)
        self.assertEqual(result["captures"]["a"]["duplicates_in_comparison_events"]["binance"], 1)

    def test_recurring_clob_payload_excluded_as_ambiguous_not_assumed_transport_duplicate(self):
        value = common.normalize_clob(clob(), {"1"})[0]
        a = loaded(configuration(), [event_row(value=value), event_row(1000000, value=value)])
        b = loaded(configuration("b"), [event_row(10000000, value=value)])
        result = comparison.compare(a, b)["feeds"]["clob"]
        self.assertEqual(result["matched_payload_hashes"], 1)
        self.assertEqual(result["ambiguous_matched_payload_hashes"], 1)
        self.assertEqual(result["matched_unique"], 0)
        self.assertEqual(result["arrival_delta_b_minus_a"]["samples"], 0)

    def test_conflicting_binance_id_not_admitted_to_timing_subset(self):
        raw = binance()
        raw["p"] = "101"
        changed = common.normalize_binance(raw, {"btcusdt"})[0]
        a = loaded(configuration(), [event_row(), event_row(value=changed)])
        b = loaded(configuration("b"), [event_row(10000000)])
        result = comparison.compare(a, b)["feeds"]["binance"]
        self.assertEqual(result["excluded_conflicting_identity_hashes"], 1)
        self.assertEqual(result["matched_unique"], 0)

    def test_same_capture_label_refused(self):
        a = loaded(configuration(), [event_row()])
        with self.assertRaises(ValueError):
            comparison.compare(a, a)

    def test_summary_diagnostics_do_not_echo_unknown_fields(self):
        terminal = summary()
        terminal["feeds"]["binance"]["authorization"] = "private-marker"
        terminal["stop_reason"] = "private-marker"
        a = loaded(configuration(), [event_row()], terminal)
        result = comparison.compare(a, loaded(configuration("b"), [event_row()]))
        self.assertNotIn("private-marker", json.dumps(result))

    def test_different_subscriptions_refused(self):
        config = configuration("b")
        config["tokens"] = ["2"]
        with self.assertRaises(ValueError):
            comparison.compare(loaded(configuration(), [event_row()]), loaded(config, [event_row()]))

    def test_missing_terminal_summary_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "incomplete.jsonl"
            path.write_text(json.dumps(configuration()) + "\n")
            with self.assertRaises(ValueError):
                comparison.capture(path, 65536)

    def test_capture_input_byte_and_line_limits(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "oversized.jsonl"
            path.write_bytes(b"x" * 65537)
            with self.assertRaises(ValueError):
                comparison.capture(path, 65536)
            path.write_bytes(b"x" * 16385 + b"\n")
            with self.assertRaises(ValueError):
                comparison.capture(path, 65536)


class RecorderTests(unittest.TestCase):
    def make(self, *extra):
        args = probe.arguments(["--label", "fixture", "--tokens", "1,2", "--max-bytes", "65536",
                                "--max-message-bytes", "65536", *extra])
        return probe.Recorder(args, io.StringIO(), common.Clock())

    def test_aggregate_gaps_duplicates_and_out_of_order_visible(self):
        recorder = self.make()
        for aggregate in (7, 9, 9, 8):
            recorder.receive("binance", json.dumps(binance(aggregate)), 0)
        self.assertEqual(recorder.stats["binance"]["aggregate_ids_skipped"], 1)
        self.assertEqual(recorder.stats["binance"]["duplicates"], 1)
        self.assertEqual(recorder.stats["binance"]["out_of_order"], 1)
        self.assertIn("aggregate_id_gap", recorder.artifact.getvalue())

    def test_observed_clob_coarse_identity_collision_is_not_a_conflict(self):
        recorder = self.make()
        raw = clob()
        raw["price_changes"].append({**raw["price_changes"][0], "price": "0.49", "size": "12"})
        recorder.receive("clob", json.dumps(raw), 0)
        self.assertEqual(recorder.stats["clob"]["recorded_events"], 2)
        self.assertEqual(recorder.stats["clob"]["identity_conflicts"], 0)
        self.assertEqual(recorder.stats["clob"]["duplicates"], 0)

    def test_artifact_event_budget_reserves_terminal_summary(self):
        recorder = self.make("--max-events", "1000")
        for aggregate in range(1000):
            recorder.receive("binance", json.dumps(binance(aggregate)), 0)
            if recorder.stopped():
                break
        self.assertEqual(recorder.stop_reason, "output_byte_budget")
        recorder.emit(recorder.summary({"country": "PT", "blocked": False}), final=True)
        self.assertLessEqual(len(recorder.artifact.getvalue().encode()), 65536)
        self.assertEqual(json.loads(recorder.artifact.getvalue().splitlines()[-1])["type"], "summary")

    def test_invalid_messages_are_counted_without_raw_payload(self):
        recorder = self.make()
        recorder.receive("clob", "private-marker", 0)
        self.assertEqual(recorder.stats["clob"]["invalid_messages"], 1)
        self.assertNotIn("private-marker", recorder.artifact.getvalue())


@unittest.skipUnless(importlib.util.find_spec("websockets"), "optional measurement dependency absent")
class WebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        asyncio.get_running_loop().set_debug(False)

    async def test_real_concurrent_protocol_capture_reconnects_and_clean_shutdown(self):
        from websockets.asyncio.client import connect
        from websockets.asyncio.server import serve

        subscriptions = []
        observed = {"clob": 0, "binance": 0}
        both_reconnected = asyncio.Event()

        async def handler(ws):
            feed = ws.request.path[1:]
            if feed == "clob":
                subscriptions.append(json.loads(await ws.recv()))
            await ws.send(json.dumps(clob() if feed == "clob" else binance()))
            self.assertEqual(await ws.recv(), "fixture-received")
            observed[feed] += 1
            if observed[feed] == 2:
                if all(observed[name] >= 2 for name in ("clob", "binance")):
                    both_reconnected.set()
                await asyncio.wait_for(both_reconnected.wait(), 30)
            await ws.close()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]

            async def connector(url, deadline, timeout, max_message_bytes):
                path = "clob" if "polymarket" in url else "binance"
                socket = await connect(f"ws://127.0.0.1:{port}/{path}", proxy=None, ping_interval=None)
                native_recv = socket.recv
                async def observed_recv(*args, **kwargs):
                    message = await native_recv(*args, **kwargs)
                    await socket.send("fixture-received")
                    return message
                socket.recv = observed_recv
                return socket

            args = probe.arguments(["--label", "fixture", "--tokens", "1,2", "--deadline", "30",
                                    "--max-reconnects", "1"])
            artifact = io.StringIO()
            with contextlib.redirect_stdout(io.StringIO()):
                result = await probe.run(args, artifact, connector, lambda: ({"country": "PT", "blocked": False}, None))
        terminal = json.loads(artifact.getvalue().splitlines()[-1])
        self.assertEqual(result, 1)
        self.assertEqual(terminal["stop_reason"], "feed_unavailable")
        self.assertEqual(subscriptions[0], {"assets_ids": ["1", "2"], "type": "market"})
        for feed in ("clob", "binance"):
            self.assertEqual(terminal["feeds"][feed]["connections"], 2)
            self.assertEqual(terminal["feeds"][feed]["duplicates"], 1)
        self.assertNotIn("one_way", terminal["feeds"])

    async def test_connect_timeout_obeys_whole_capture_deadline(self):
        cancelled = asyncio.Event()
        async def slow_connector(*args):
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        args = probe.arguments(["--label", "fixture", "--tokens", "1", "--deadline", ".05"])
        with contextlib.redirect_stdout(io.StringIO()):
            result = await probe.run(args, io.StringIO(), slow_connector,
                                     lambda: ({"country": "PT", "blocked": False}, None))
        self.assertEqual(result, 1)
        self.assertTrue(cancelled.is_set())

    async def test_blocked_geoblock_does_not_open_websockets(self):
        async def forbidden_connector(*args):
            self.fail("blocked capture attempted a socket")

        args = probe.arguments(["--label", "fixture", "--tokens", "1"])
        artifact = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()):
            result = await probe.run(args, artifact, forbidden_connector,
                                     lambda: ({"country": "GB", "blocked": True}, None))
        self.assertEqual(result, 1)
        self.assertEqual(json.loads(artifact.getvalue().splitlines()[-1])["stop_reason"], "geoblock_blocked")


if __name__ == "__main__":
    unittest.main()
