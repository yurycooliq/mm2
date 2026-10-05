"""Read-only monitor must fail on stale/capacity faults and never dispatch controls."""
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from soak import loopback_url, main, observations


def options():
    return SimpleNamespace(expect_mode="both", require_feed_ready=True,
                           max_snapshot_age_ms=5000, min_free_bytes=100,
                           min_free_percent=5, warn_capacity_percent=80)


def state():
    return {"execution_mode": "both", "mode": "paused", "api_ready": True,
            "feed_ready": True, "live_eligible": False, "published_at_ms": 100_000,
            "journal": {"total_bytes": 10, "total_max_bytes": 100,
                        "archive_segments": 0, "max_segments": 10,
                        "owner_commands": 1, "max_commands": 10,
                        "execution_operations": 2, "max_execution_operations": 10}}


class MonitorTests(unittest.TestCase):
    def test_unready_feed_and_stale_snapshot_are_reported_even_with_healthy_api(self):
        value = state()
        value["feed_ready"] = False
        result = observations(value, 1000, 50, options(), 110_000)
        self.assertIn("feed_not_ready", result["alerts"])
        self.assertIn("state_snapshot_stale_or_clock_skew", result["alerts"])

    def test_all_retention_budgets_and_disk_are_observed(self):
        value = state()
        for used, bound in (("total_bytes", "total_max_bytes"), ("archive_segments", "max_segments"),
                            ("owner_commands", "max_commands"), ("execution_operations", "max_execution_operations")):
            value["journal"][used] = value["journal"][bound]
        result = observations(value, 1, 1, options(), 100_000)
        self.assertEqual(len(result["alerts"]), 5)
        self.assertIn("disk_capacity_low", result["alerts"])

    def test_companion_paper_and_live_capacity_are_independently_checked(self):
        value = state()
        value["parallel_simulation"] = {"journal": {"total_bytes": 90, "total_max_bytes": 100,
                                                    "execution_operations": 80, "max_execution_operations": 100}}
        value["live"] = {"phase": "paused", "journal_bytes": 95, "journal_max_bytes": 100}
        result = observations(value, 1000, 50, options(), 100_000)
        self.assertEqual(set(result["alerts"]), {"paper_journal_bytes_capacity_threshold",
                                               "paper_execution_history_capacity_threshold",
                                               "live_journal_bytes_capacity_threshold"})
        self.assertEqual(result["paper_journal"]["total_bytes"], 90)
        self.assertEqual(result["live_journal_max_bytes"], 100)

    def test_only_loopback_urls_without_credentials_or_redirect_payloads_are_accepted(self):
        self.assertEqual(loopback_url("http://127.0.0.1:8088/"), "http://127.0.0.1:8088")
        for url in ("http://example.com", "http://127.0.0.1@evil.example", "http://x:y@127.0.0.1",
                    "http://127.0.0.1/controls", "http://127.0.0.1?token=value", "file:///tmp/state"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                loopback_url(url)

    def test_real_loopback_probe_uses_only_get_health_and_state_and_never_logs_token(self):
        requested = []
        token = "test-owner-token-never-print"

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requested.append((self.command, self.path))
                value = {"healthy": True}
                if self.path == "/v1/state":
                    if self.headers.get("Authorization") != "Bearer " + token:
                        self.send_error(401)
                        return
                    value = state()
                    value["published_at_ms"] = int(time.time() * 1000)
                    value["boot_id"] = "fixed-boot"
                elif self.path != "/healthz":
                    self.send_error(405)
                    return
                raw = json.dumps(value).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "observations.jsonl"
                captured = StringIO()
                with patch.dict("os.environ", {"MM2_TEST_TOKEN": token}), redirect_stdout(captured):
                    code = main(["--url", f"http://127.0.0.1:{server.server_port}", "--token-env", "MM2_TEST_TOKEN",
                                 "--seconds", "1", "--interval", "1", "--output", str(output),
                                 "--disk-path", directory, "--min-free-bytes", "0", "--min-free-percent", "0",
                                 "--expect-mode", "both", "--require-feed-ready"])
                self.assertEqual(code, 0)
                self.assertEqual(requested, [("GET", "/healthz"), ("GET", "/v1/state")])
                self.assertNotIn(token, captured.getvalue() + output.read_text())
                self.assertTrue(json.loads(captured.getvalue())["passed"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
