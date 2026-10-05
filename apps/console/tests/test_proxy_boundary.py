from dataclasses import replace
from email.message import Message
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

from mm2_console.app import App
from mm2_console.auth import RateLimiter
from mm2_console.config import Config
from mm2_console.server import ConsoleServer, Handler

BOT = "offline-proxy-boundary-token"


def client_key(peer, values=(), enabled=True, forwarded_for=None):
    handler = Handler.__new__(Handler)
    handler.server = SimpleNamespace(app=SimpleNamespace(config=SimpleNamespace(trusted_proxy_header=enabled)))
    handler.client_address = (peer, 1234)
    handler.headers = Message()
    for value in values:
        handler.headers["X-MM2-Client-IP"] = value
    if forwarded_for:
        handler.headers["X-Forwarded-For"] = forwarded_for
    return handler._client_key()


class ProxyBoundaryTests(unittest.TestCase):
    def test_proxy_trust_is_explicit_and_disabled_for_direct_public_or_loopback_peers(self):
        self.assertEqual(client_key("172.30.0.2", ["8.8.8.8"], enabled=False), "172.30.0.2")
        for peer in ("8.8.4.4", "127.0.0.1", "::1", "::ffff:127.0.0.1", "not-an-address"):
            with self.subTest(peer=peer):
                self.assertEqual(client_key(peer, ["8.8.8.8"]), peer)

    def test_private_bridge_peers_accept_one_ip_and_normalize_ipv6_only_for_rate_buckets(self):
        self.assertEqual(client_key("172.30.0.2", ["8.8.8.8"]), "8.8.8.8")
        self.assertEqual(client_key("fd00::2", ["2001:4860:4860:0000:0000:0000:0000:8888"]), "2001:4860:4860::8888")
        self.assertEqual(client_key("172.30.0.2", ["::ffff:8.8.8.8"]), "8.8.8.8")

    def test_missing_duplicate_list_scoped_or_invalid_proxy_values_fall_back_to_socket_peer(self):
        for values in ([], ["8.8.8.8", "8.8.4.4"], ["8.8.8.8,8.8.4.4"], ["8.8.8.8:443"],
                       [" 8.8.8.8"], ["8.8.8.8 "], ["fe80::1%eth0"], ["0.0.0.0"], ["::"],
                       ["224.0.0.1"], ["ff00::1"], ["008.008.008.008"], ["8" * 46]):
            with self.subTest(values=values):
                self.assertEqual(client_key("172.30.0.2", values, forwarded_for="1.1.1.1"), "172.30.0.2")

    def test_arbitrary_forwarded_for_never_selects_a_rate_bucket(self):
        self.assertEqual(client_key("172.30.0.2", forwarded_for="8.8.8.8"), "172.30.0.2")
        self.assertEqual(client_key("172.30.0.2", ["1.1.1.1"], forwarded_for="8.8.8.8"), "1.1.1.1")

    def test_environment_defaults_to_untrusted_and_accepts_only_explicit_boolean_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            values = {"MM2_CONSOLE_STATE_DIR": directory, "MM2_ENGINE_API_TOKEN": "e" * 64,
                      "MM2_CONSOLE_TOKEN": "o" * 64}
            with patch.dict(os.environ, values, clear=True):
                self.assertFalse(Config.from_env().trusted_proxy_header)
            for value, expected in (("true", True), ("false", False)):
                with patch.dict(os.environ, {**values, "MM2_TRUSTED_PROXY_HEADER": value}, clear=True):
                    self.assertEqual(Config.from_env().trusted_proxy_header, expected)
            for value in ("", "yes", "1", "TRUE"):
                with patch.dict(os.environ, {**values, "MM2_TRUSTED_PROXY_HEADER": value}, clear=True), self.assertRaises(ValueError):
                    Config.from_env()


class PrivatePeerFixtureServer(ConsoleServer):
    def process_request(self, request, client_address):
        # Wire fixture models the private bridge socket peer without claiming
        # that a public forwarded header is itself a trusted connection.
        return super().process_request(request, ("172.30.0.2", client_address[1]))


class NoEngine:
    def request(self, *_):
        raise AssertionError("Rate-limit/authentication fixtures must not request the engine")


class ProxyHttpTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        config = Config("http://127.0.0.1:1", "e" * 64, "o" * 64, root, root, frozenset(),
                        telegram_token=BOT, telegram_owner=42, telegram_only=True, trusted_proxy_header=True)
        self.app = App(config, NoEngine())
        self.server = PrivatePeerFixtureServer(("127.0.0.1", 0), self.app)
        self.port = self.server.server_address[1]
        self.origin = f"http://127.0.0.1:{self.port}"
        self.app.config = replace(config, allowed_origins=frozenset({self.origin}))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.app.close()
        self.temp.cleanup()

    def request(self, method, path, ip, body=None):
        client = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        try:
            client.request(method, path, body=None if body is None else json.dumps(body),
                           headers={"Origin": self.origin, "Content-Type": "application/json", "X-MM2-Client-IP": ip})
            response = client.getresponse()
            result = response.status, response.read()
            return result
        finally:
            client.close()

    def test_bad_anonymous_client_cannot_exhaust_a_different_owner_client_login_bucket(self):
        self.app.auth_attempts = RateLimiter(capacity=2, refill=0)
        for _ in range(2):
            self.assertEqual(self.request("POST", "/api/auth/telegram", "8.8.8.8", {"init_data": "invalid"})[0], 404)
        self.assertEqual(self.request("POST", "/api/auth/telegram", "8.8.8.8", {"init_data": "invalid"})[0], 429)
        fields = {"auth_date": str(int(time.time())), "user": '{"id":42}', "query_id": "offline-proxy-owner"}
        check = "\n".join(f"{key}={fields[key]}" for key in sorted(fields))
        key = hmac.new(b"WebAppData", BOT.encode(), hashlib.sha256).digest()
        fields["hash"] = hmac.new(key, check.encode(), hashlib.sha256).hexdigest()
        self.assertEqual(self.request("POST", "/api/auth/telegram", "1.1.1.1", {"init_data": urlencode(fields)})[0], 200)

    def test_ingress_buckets_are_separate_but_client_ip_never_grants_owner_identity(self):
        self.app.ingress = RateLimiter(capacity=1, refill=0)
        self.assertEqual(self.request("GET", "/api/state", "8.8.8.8")[0], 404)
        self.assertEqual(self.request("GET", "/api/state", "8.8.8.8")[0], 429)
        self.assertEqual(self.request("GET", "/api/state", "1.1.1.1")[0], 404)


if __name__ == "__main__":
    unittest.main()
