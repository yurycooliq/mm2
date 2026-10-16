"""Online acceptance cannot pass on process health alone; fixtures use no Docker or network."""
from contextlib import redirect_stdout
from copy import deepcopy
from io import StringIO
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import smoke_public as smoke


OWNER = "a" * 32
WALL_MS = 300_050


def snapshot():
    book = {"initialized": True, "tick": "0.01", "best_bid": "0.4", "best_ask": "0.6",
            "exchange_ms": 300_025, "observed_mono_ms": 100, "exchange_age_ms": 25, "receive_age_ms": 20}
    return {"mode": "paused", "execution_mode": "shadow", "feed_source": "public", "command_version": 0,
            "reset_epoch": 0, "boot_id": "00000000-0000-0000-0000-000000000001", "published_at_ms": WALL_MS,
            "api_ready": True, "feed_ready": True, "shadow_ready": False, "awaiting_source_refresh": False,
            "live_eligible": False, "live": None, "simulation": None, "parallel_simulation": None,
            "shadow_intents": [], "fault": None, "last_input_error": None, "simulation_error": None,
            "shutting_down": False, "committing": False, "source_status": {"phase": "streaming", "message": "not exported"},
            "limits": {"max_feed_age_ms": 1000}, "books": {"up": deepcopy(book), "down": deepcopy(book)},
            "market": {"slug": "btc-updown-5m-300", "condition_id": "0x" + "1" * 64,
                       "up_token": "1", "down_token": "2", "window_start_ms": 300_000, "window_end_ms": 600_000},
            "binance_connected": True, "reference": {"source": "binance_btcusdt", "price": "90000",
                                                     "exchange_ms": 300_025, "observed_mono_ms": 100, "oracle": False}}


class PublicSmokeTests(unittest.TestCase):
    def run_fixture(self, value=None, *, state_failure=None, running=True, foreign_owner=False, next_state=None):
        value = snapshot() if value is None else value
        calls, requests = [], []
        clock = [0.0]

        def docker(*args):
            calls.append(args)
            return "fixture-container" if args[0] == "run" else "fixture-volume"

        def inspect(kind, name):
            labels = {"mm2.public-check": "foreign" if foreign_owner else OWNER}
            if kind == "volume":
                return {"Labels": labels}
            return {"Config": {"Labels": labels}, "State": {"Running": running, "Status": "running" if running else "exited"},
                    "NetworkSettings": {"Ports": {"8088/tcp": [{"HostPort": "12345"}]}}}

        def request(base, path, token):
            requests.append(path)
            if state_failure is not None:
                raise state_failure
            return (next_state if next_state is not None and len(requests) > 1 else value), {}

        with patch.object(smoke, "docker", side_effect=docker), patch.object(smoke, "inspect", side_effect=inspect), \
                patch.object(smoke, "request", side_effect=request), patch.object(smoke.uuid, "uuid4", return_value=SimpleNamespace(hex=OWNER)), \
                patch.object(smoke.time, "monotonic", side_effect=lambda: clock[0]), \
                patch.object(smoke.time, "sleep", side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds)), \
                patch.object(smoke.time, "time", return_value=WALL_MS / 1000):
            output = StringIO()
            with redirect_stdout(output):
                code = smoke.main(["--seconds", "5", "--engine-image", "sha256:" + "b" * 64])
        return code, json.loads(output.getvalue()), calls, requests, output.getvalue()

    def test_paused_without_intents_passes_with_both_fresh_books_and_reference(self):
        code, result, calls, requests, output = self.run_fixture()
        self.assertEqual(code, 0)
        for key in ("passed", "feed_ready_seen", "market_seen", "up_book_fresh_seen", "down_book_fresh_seen",
                    "reference_seen", "binance_connected_seen", "complete_feed_seen", "paused_verified", "cleanup_ok"):
            self.assertTrue(result[key], key)
        self.assertFalse(result["shadow_ready_seen"])
        self.assertFalse(result["shadow_ready_required"])
        self.assertEqual(result["live_orders"], 0)
        self.assertEqual(set(requests), {"/v1/state"})
        run = next(call for call in calls if call[0] == "run")
        self.assertEqual(run[-1], "sha256:" + "b" * 64)
        self.assertIn("--read-only", run)
        self.assertIn("127.0.0.1::8088", run)
        self.assertNotIn(OWNER * 2, output)
        self.assertNotIn("not exported", output)
        self.assertTrue(any(call[:3] == ("container", "rm", "--force") for call in calls))
        self.assertTrue(any(call[:2] == ("volume", "rm") for call in calls))

    def test_healthy_api_without_required_feed_observation_fails(self):
        value = snapshot()
        value.update(feed_ready=False, market=None, books={"up": None, "down": None}, reference=None, binance_connected=False)
        code, result, _, requests, _ = self.run_fixture(value)
        self.assertEqual(code, 1)
        self.assertFalse(result["passed"])
        self.assertTrue(result["api_ready_seen"])
        self.assertFalse(result["feed_ready_seen"])
        self.assertEqual(result["failure"], "required_public_feeds_not_observed")
        self.assertTrue(requests)

    def test_claimed_feed_ready_is_insufficient_for_missing_stale_or_wrong_identity_evidence(self):
        invalid = []
        for outcome in ("up", "down"):
            for field, bad in (("initialized", False), ("exchange_age_ms", 1001), ("receive_age_ms", None),
                               ("exchange_ms", 0), ("best_bid", "NaN"), ("best_ask", None)):
                value = snapshot()
                value["books"][outcome][field] = bad
                invalid.append(value)
        for field, bad in (("slug", "demo-300"), ("down_token", "1"), ("window_end_ms", 300_000),
                           ("condition_id", "missing")):
            value = snapshot()
            value["market"][field] = bad
            invalid.append(value)
        for value in invalid:
            with self.subTest(value=value):
                observed = smoke.observe(value, WALL_MS)
                self.assertFalse(observed["feed_ready"])

    def test_clob_books_alone_cannot_establish_complete_source_acceptance(self):
        for reference, connected in ((None, True), (snapshot()["reference"], False),
                                     ({**snapshot()["reference"], "exchange_ms": 0}, True)):
            value = snapshot()
            value.update(reference=reference, binance_connected=connected)
            code, result, _, _, _ = self.run_fixture(value)
            self.assertEqual(code, 1)
            self.assertTrue(result["feed_ready_seen"])
            self.assertFalse(result["reference_seen"])

    def test_snapshot_publication_lag_counts_against_book_and_reference_freshness(self):
        value = snapshot()
        now = WALL_MS + 4000
        observed = smoke.observe(value, now)
        self.assertTrue(observed["api_ready"])
        self.assertFalse(observed["feed_ready"])
        self.assertFalse(observed["up_book_fresh"])
        self.assertFalse(observed["down_book_fresh"])
        value["reference"]["exchange_ms"] = WALL_MS - 2000
        self.assertFalse(smoke.observe(value, now)["reference_ready"])
        # Exchange timestamps alone cannot establish receive freshness after cached publication lag.
        for outcome in ("up", "down"):
            value["books"][outcome]["exchange_ms"] = now - 10
            value["books"][outcome]["exchange_age_ms"] = 10
        self.assertFalse(smoke.observe(value, now)["feed_ready"])

    def test_current_market_window_is_checked_at_observer_wall_time(self):
        value = snapshot()
        value["published_at_ms"] = 599_999
        self.assertFalse(smoke.observe(value, 600_000)["market_ready"])

    def test_engine_exit_api_loss_fault_control_change_and_orders_fail_with_cleanup(self):
        code, result, calls, requests, _ = self.run_fixture(running=False)
        self.assertEqual(code, 1)
        self.assertEqual(result["failure"], "engine_exited_or_oom")
        self.assertEqual(requests, [])
        self.assertTrue(any(call[:2] == ("container", "rm") for call in calls))
        code, result, _, _, output = self.run_fixture(state_failure=OSError("private-token-url-must-not-appear"))
        self.assertEqual(code, 1)
        self.assertNotIn("private-token-url-must-not-appear", output)
        for field, bad in (("fault", "private fault message"), ("last_input_error", "source failed"),
                           ("mode", "shadow"), ("command_version", 1), ("reset_epoch", 1),
                           ("live_eligible", True), ("live", {"orders": [{}]}), ("actual_orders", [{}]),
                           ("simulation", {}), ("shadow_intents", [{}])):
            value = snapshot()
            value[field] = bad
            with self.subTest(field=field):
                code, result, _, _, output = self.run_fixture(next_state=value)
                self.assertEqual(code, 1)
                self.assertFalse(result["passed"])
                self.assertTrue(result["cleanup_ok"])
                self.assertNotIn("private fault message", output)

    def test_restart_stale_snapshot_and_foreign_resource_labels_never_pass(self):
        value = snapshot()
        value["boot_id"] = "00000000-0000-0000-0000-000000000002"
        code, result, _, _, _ = self.run_fixture(next_state=value)
        self.assertEqual(code, 1)
        self.assertEqual(result["failure"], "engine_restarted")
        value = snapshot()
        value["published_at_ms"] = 1
        with self.assertRaises(smoke.SmokeFailure):
            smoke.observe(value, WALL_MS)
        code, result, calls, _, _ = self.run_fixture(foreign_owner=True)
        self.assertEqual(code, 1)
        self.assertFalse(result["cleanup_ok"])
        self.assertFalse(any(call[0] in {"container", "volume"} and call[1] == "rm" for call in calls))


if __name__ == "__main__":
    unittest.main()
