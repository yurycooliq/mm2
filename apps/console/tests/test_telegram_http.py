from dataclasses import replace
import hashlib
import hmac
import http.client
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

from mm2_console.app import App
from mm2_console.auth import Sessions
from mm2_console.config import Config
from mm2_console.server import ConsoleServer

BOT = "offline-http-telegram-fixture-token"
OWNER = 3_666_000_000
LOCAL_TOKEN = "offline-local-token-" + "l" * 40
ENGINE_TOKEN = "offline-engine-token-" + "e" * 40


def signed(user_id=OWNER, auth_date=None, query_id="offline-launch"):
    fields = {"auth_date": str(int(time.time()) if auth_date is None else auth_date),
              "user": json.dumps({"id": user_id, "first_name": "Offline owner"}, separators=(",", ":")),
              "query_id": query_id, "signature": "offline-third-party-proof"}
    check = "\n".join(f"{key}={fields[key]}" for key in sorted(fields))
    key = hmac.new(b"WebAppData", BOT.encode(), hashlib.sha256).digest()
    fields["hash"] = hmac.new(key, check.encode(), hashlib.sha256).hexdigest()
    return urlencode(fields)


class NoEngine:
    def __init__(self):
        self.calls = []

    def request(self, *args):
        self.calls.append(args)
        raise AssertionError("HTTP authentication and cached reads must not request the engine")


class TelegramHttpTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        static = root / "static"
        static.mkdir()
        (static / "index.html").write_text("<html><body>bootstrap</body></html>")
        config = Config("http://127.0.0.1:1", ENGINE_TOKEN, LOCAL_TOKEN, root, static, frozenset(),
                        telegram_token=BOT, telegram_owner=OWNER, telegram_only=True)
        self.engine = NoEngine()
        self.app = App(config, self.engine)
        self.server = ConsoleServer(("127.0.0.1", 0), self.app)
        self.port = self.server.server_address[1]
        self.origin = f"http://127.0.0.1:{self.port}"
        self.app.config = replace(config, allowed_origins=frozenset({self.origin, "https://mm2.vstage.site"}))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.app.close()
        self.temp.cleanup()

    def request(self, method, path, body=None, cookie=None, origin=None, host=None):
        headers = {"Content-Type": "application/json", "Origin": self.origin if origin is None else origin}
        if cookie is not None:
            headers["Cookie"] = cookie
        if host is not None:
            headers["Host"] = host
        client = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        try:
            client.request(method, path, body=None if body is None else json.dumps(body), headers=headers)
            response = client.getresponse()
            raw = response.read()
            value = json.loads(raw) if response.getheader("Content-Type") == "application/json" else raw
            return response.status, dict(response.getheaders()), value
        finally:
            client.close()

    def authenticate(self, init_data=None, cookie=None, **kwargs):
        return self.request("POST", "/api/auth/telegram", {"init_data": init_data or signed()}, cookie, **kwargs)

    def owner_cookie(self, init_data=None):
        status, headers, value = self.authenticate(init_data)
        self.assertEqual(status, 200)
        self.assertEqual(value, {"authenticated": True})
        return headers["Set-Cookie"].split(";", 1)[0]

    def assert_hidden(self, reply):
        status, headers, value = reply
        self.assertEqual(status, 404)
        self.assertEqual(value, {"error": "not_found"})
        self.assertNotIn("Set-Cookie", headers)

    def test_unauthenticated_api_is_hidden_and_static_bootstrap_remains_available(self):
        for path in ("/api/state", "/api/history", "/api/logs", "/api/unknown"):
            with self.subTest(path=path):
                self.assert_hidden(self.request("GET", path))
        self.assert_hidden(self.request("POST", "/api/control", {"id": "unused", "expected_version": 0, "action": "pause"}))
        self.assertEqual(self.request("GET", "/")[0], 200)
        self.assertFalse(self.engine.calls)

    def test_even_the_correct_local_token_is_disabled_in_telegram_only_mode(self):
        for token in (LOCAL_TOKEN, "wrong-token"):
            self.assert_hidden(self.request("POST", "/api/auth/local", {"token": token}))
        self.assertEqual(len(self.app.sessions._sessions), 0)
        self.assertFalse(self.engine.calls)

    def test_signed_owner_gets_a_private_cookie_and_cached_api_without_engine_requests(self):
        status, headers, value = self.authenticate()
        self.assertEqual(status, 200)
        self.assertEqual(value, {"authenticated": True})
        self.assertIn("HttpOnly", headers["Set-Cookie"])
        self.assertIn("SameSite=Strict", headers["Set-Cookie"])
        self.assertEqual(headers["Cache-Control"], "no-store")
        cookie = headers["Set-Cookie"].split(";", 1)[0]
        status, _, state = self.request("GET", "/api/state", cookie=cookie)
        self.assertEqual(status, 200)
        self.assertIsNone(state["state"])
        encoded = json.dumps(state)
        for secret in (BOT, LOCAL_TOKEN, ENGINE_TOKEN):
            self.assertNotIn(secret, encoded)
        self.assertFalse(self.engine.calls)

    def test_additional_operator_authenticates_without_displacing_owner_or_admitting_stranger(self):
        additional = 1401626578
        self.app.config = replace(self.app.config, telegram_allowed_ids=frozenset({additional}))
        owner_cookie = self.owner_cookie(signed(query_id="primary-owner"))
        extra_data = signed(user_id=additional, query_id="additional-operator")
        extra_cookie = self.owner_cookie(extra_data)
        self.assertNotEqual(owner_cookie, extra_cookie)
        for cookie in (owner_cookie, extra_cookie):
            self.assertEqual(self.request("GET", "/api/state", cookie=cookie)[0], 200)
        self.assert_hidden(self.authenticate(signed(user_id=additional + 1)))
        self.assert_hidden(self.authenticate(extra_data))
        self.assertEqual(self.authenticate(extra_data, extra_cookie)[0], 200)
        self.assertEqual(self.request("GET", "/api/state", cookie=owner_cookie)[0], 200)
        self.assertFalse(self.engine.calls)

    def test_https_termination_uses_secure_httponly_cookie_for_telegram_embedding(self):
        status, headers, _ = self.authenticate(origin="https://mm2.vstage.site", host="mm2.vstage.site")
        self.assertEqual(status, 200)
        for flag in ("HttpOnly", "SameSite=None", "; Secure", "Path=/"):
            self.assertIn(flag, headers["Set-Cookie"])

    def test_reload_reuses_only_the_original_bound_cookie_without_extending_server_ttl(self):
        clock = [0]
        self.app.sessions = Sessions(ttl=30, clock=lambda: clock[0])
        data = signed()
        cookie = self.owner_cookie(data)
        token = cookie.split("=", 1)[1]
        original_expiry = self.app.sessions._sessions[token]
        clock[0] = 15
        status, headers, value = self.authenticate(data, cookie)
        self.assertEqual(status, 200)
        self.assertEqual(value, {"authenticated": True})
        self.assertEqual(headers["Set-Cookie"].split(";", 1)[0], cookie)
        self.assertEqual(self.app.sessions._sessions[token], original_expiry)
        self.assertEqual(len(self.app.sessions._sessions), 1)
        clock[0] = 30
        self.assert_hidden(self.request("GET", "/api/state", cookie=cookie))
        self.assert_hidden(self.authenticate(data, cookie))

    def test_reload_after_launch_expiry_keeps_live_session_then_requires_fresh_launch(self):
        clock, wall = [0], [1_800_000_000]
        self.app.sessions = Sessions(clock=lambda: clock[0])
        with patch("mm2_console.auth.time.time", side_effect=lambda: wall[0]):
            data = signed(auth_date=wall[0])
            cookie = self.owner_cookie(data)
            token = cookie.split("=", 1)[1]
            expiry = self.app.sessions._sessions[token]
            for age in (301, 3599):
                clock[0], wall[0] = age, 1_800_000_000 + age
                self.assertEqual(self.authenticate(data, cookie)[0], 200)
                self.assertEqual(self.app.sessions._sessions[token], expiry)
            clock[0], wall[0] = 3600, 1_800_003_600
            self.assert_hidden(self.authenticate(data, cookie))
            self.assertEqual(len(self.app.sessions._sessions), 0)
            self.assertEqual(self.authenticate(signed(auth_date=wall[0], query_id="fresh-reopen"))[0], 200)
        self.assertFalse(self.engine.calls)

    def test_replayed_init_data_cannot_issue_a_session_to_another_cookie_or_client(self):
        data = signed(query_id="original-client")
        original = self.owner_cookie(data)
        second = self.owner_cookie(signed(query_id="second-client"))
        local_session = "mm2_owner=" + self.app.sessions.create()
        for cookie in (None, second, local_session):
            with self.subTest(cookie=cookie):
                self.assert_hidden(self.authenticate(data, cookie))
        self.assertEqual(self.authenticate(data, original)[0], 200)
        self.assertEqual(self.request("GET", "/api/state", cookie=original)[0], 200)
        self.assertFalse(self.engine.calls)

    def test_wrong_owner_expired_and_tampered_data_reject_even_an_existing_owner_cookie(self):
        cookie = self.owner_cookie()
        data = (signed(user_id=OWNER + 1), signed(auth_date=int(time.time()) - 301), signed(auth_date=int(time.time()) + 10),
                signed().replace("Offline+owner", "attacker"))
        for value in data:
            with self.subTest(value=value):
                self.assert_hidden(self.authenticate(value, cookie))
        self.assertEqual(len(self.app.sessions._sessions), 0)
        self.assertFalse(self.engine.calls)

    def test_rejected_owner_or_expired_launch_revokes_an_earlier_owner_cookie(self):
        for index, rejected in enumerate((signed(user_id=OWNER + 1), signed(auth_date=int(time.time()) - 301))):
            cookie = self.owner_cookie(signed(query_id=f"fresh-owner-before-rejection-{index}"))
            self.assertEqual(self.request("GET", "/api/state", cookie=cookie)[0], 200)
            self.assert_hidden(self.authenticate(rejected, cookie))
            self.assert_hidden(self.request("GET", "/api/state", cookie=cookie))
            self.assertFalse(self.app.sessions.valid(cookie.split("=", 1)[1]))

    def test_foreign_origin_cannot_force_owner_cookie_revocation(self):
        cookie = self.owner_cookie()
        status, _, _ = self.authenticate(signed(user_id=OWNER + 1), cookie, origin="https://attacker.invalid")
        self.assertEqual(status, 403)
        self.assertEqual(self.request("GET", "/api/state", cookie=cookie)[0], 200)

    def test_signed_data_does_not_bypass_same_origin_csrf_or_host_validation(self):
        for origin, host in (("https://attacker.invalid", None), (self.origin, "attacker.invalid")):
            with self.subTest(origin=origin, host=host):
                status, _, value = self.authenticate(origin=origin, host=host)
                self.assertEqual(status, 403)
                self.assertEqual(value["error"], "origin_rejected")
        self.assertEqual(len(self.app.sessions._sessions), 0)

    def test_logout_revokes_the_cookie_and_keeps_its_original_init_data_replay_fenced(self):
        data = signed()
        cookie = self.owner_cookie(data)
        self.assertEqual(self.request("POST", "/api/logout", {}, cookie)[0], 200)
        self.assert_hidden(self.request("GET", "/api/state", cookie=cookie))
        self.assert_hidden(self.authenticate(data, cookie))
        self.assertEqual(len(self.app.sessions._sessions), 0)


if __name__ == "__main__":
    unittest.main()
