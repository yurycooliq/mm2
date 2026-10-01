"""Offline transport checks for the read-only placement probe."""

import contextlib
import http.client
import http.server
import importlib.util
import io
import json
import pathlib
import threading
import time
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location(
    "clob_probe", pathlib.Path(__file__).with_name("clob-latency-probe.py"),
)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def do_GET(self):
        self.server.paths.append(self.path)
        if self.path == "/slow":
            try:
                self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                self.wfile.flush()
                for _ in range(20):
                    time.sleep(.02)
                    self.wfile.write(b"X-Slow: value\r\n")
                    self.wfile.flush()
            except OSError:
                pass
            return
        if self.path == "/delayed-headers":
            self.wfile.write(b"HTTP/1.1 200 OK\r\n")
            self.wfile.flush()
            time.sleep(.15)
            self.wfile.write(b"Content-Length: 2\r\n\r\n{}")
            self.wfile.flush()
            return
        body = (b'{"blocked":false,"country":"PT","ip":"private-marker"}'
                if self.path == "/api/geoblock" else b"1234567890")
        limited = self.path == "/time" and self.server.rate_limited
        self.send_response(429 if limited else 200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Set-Cookie", "private-marker")
        self.send_header("Authorization", "private-marker")
        self.send_header("Poly-RateLimit-Tier", "Standard")
        if self.path == "/close":
            self.send_header("Connection", "close")
            self.close_connection = True
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()


class LocalConnection(http.client.HTTPConnection):
    def __init__(self, address):
        super().__init__(*address)
        self.deadline = 0
        self.last_response = None
        self._create_connection = self.connect_socket
        self.response_class = self.make_response

    def connect_socket(self, address, timeout=None, source_address=None):
        return probe.bounded_connect(address, self.deadline)

    def make_response(self, sock, *args, **kwargs):
        self.last_response = probe.TimedHTTPResponse(
            sock, *args, deadline=self.deadline, **kwargs,
        )
        return self.last_response


class ProbeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.daemon_threads = True
        cls.server.paths = []
        cls.server.rate_limited = False
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def setUp(self):
        self.server.paths.clear()
        self.server.rate_limited = False
        self.connection = LocalConnection(self.server.server_address)

    def tearDown(self):
        self.connection.close()

    def sample(self, path="/time", timeout=1):
        return probe.sample(self.connection, path, "warm", timeout, time.perf_counter() + 2)

    def test_real_keep_alive_and_first_byte_before_headers_complete(self):
        first, _ = self.sample("/delayed-headers")
        second, _ = self.sample()
        self.assertTrue(first["ok"])
        self.assertFalse(first["connection_reused"])
        self.assertTrue(second["connection_reused"])
        self.assertGreater(first["total_ms"] - first["ttfb_ms"], 100)

    def test_closed_connection_does_not_count_as_reused(self):
        first, _ = self.sample("/close")
        second, _ = self.sample()
        self.assertTrue(first["ok"] and second["ok"])
        self.assertFalse(second["connection_reused"])

    def test_cumulative_read_deadline_stops_slow_header_drip(self):
        row, _ = self.sample("/slow", timeout=.08)
        self.assertFalse(row["ok"])
        self.assertEqual(row["error"], "TimeoutError")
        self.assertLess(row["total_ms"], 400)
        self.assertIsNotNone(row["ttfb_ms"])

    def test_expired_run_deadline_is_a_sanitized_error(self):
        row, _ = probe.sample(self.connection, "/time", "warm", 1, time.perf_counter() - 1)
        self.assertFalse(row["ok"])
        self.assertEqual(row["error"], "TimeoutError")

    def test_dns_wait_is_bounded(self):
        def slow_dns(*args, **kwargs):
            time.sleep(.2)
            return []

        started = time.perf_counter()
        with mock.patch.object(probe.socket, "getaddrinfo", side_effect=slow_dns):
            with self.assertRaises(TimeoutError):
                probe.bounded_connect(("example.invalid", 443), started + .03)
        self.assertLess(time.perf_counter() - started, .15)

    def test_rate_headers_retained_and_credentials_removed(self):
        row, _ = self.sample()
        self.assertEqual(row["headers"]["poly-ratelimit-tier"], "Standard")
        self.assertNotIn("set-cookie", row["headers"])
        self.assertNotIn("authorization", row["headers"])
        self.assertNotIn("private-marker", json.dumps(row))

    def test_geoblock_keeps_only_country_and_blocked(self):
        row, body = self.sample("/api/geoblock")
        result = probe.geoblock_result(row, body)
        self.assertEqual(result, {"country": "PT", "blocked": False})
        self.assertNotIn("private-marker", json.dumps(result))
        probe.geoblock_result(row, b'{"country":"PT","blocked":"false"}')
        self.assertFalse(row["ok"])

    def test_p99_is_withheld_for_small_exact_group(self):
        result = probe.distribution(list(range(9)))
        self.assertEqual(result["samples"], 9)
        self.assertIsNone(result["p99_ms"])
        self.assertEqual(result["p99_status"], "insufficient_samples")
        self.assertEqual(probe.distribution(list(range(1000)))["p99_ms"], 989)

    def test_output_path_cannot_escape_ignored_output(self):
        for name in ("docs/result.jsonl", "output/../docs/result.jsonl", "output/result.txt"):
            with self.assertRaises(ValueError):
                probe.output_path(name)
        self.assertTrue(probe.output_path("output/latency/result.jsonl").is_relative_to(probe.REPO_ROOT))

    def test_full_entry_point_stops_at_429_without_retry_and_sanitizes_jsonl(self):
        self.server.rate_limited = True
        args = probe.arguments(["--cold", "3", "--warm", "4", "--max-requests", "8",
                                "--interval-ms", "0"])
        artifact = io.StringIO()
        stdout = io.StringIO()
        factory = lambda host, context: LocalConnection(self.server.server_address)
        with mock.patch.object(probe, "ProbeConnection", side_effect=factory):
            with contextlib.redirect_stdout(stdout):
                result = probe.run(args, artifact)
        summary = json.loads(stdout.getvalue())
        self.assertEqual(result, 1)
        self.assertEqual(summary["stop_reason"], "rate_limited")
        self.assertEqual(summary["requests_attempted"], 2)
        self.assertEqual(self.server.paths, ["/api/geoblock", "/time"])
        self.assertNotIn("private-marker", artifact.getvalue())
        self.assertEqual(summary["latency"]["cold"]["successful"], 0)


if __name__ == "__main__":
    unittest.main()
