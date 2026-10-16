from dataclasses import replace
import hashlib
import hmac
from http.cookies import SimpleCookie
import http.client
import io
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from urllib.parse import urlencode

from mm2_console.app import App, command_payload
from mm2_console.auth import RateLimiter, Sessions, validate_telegram
from mm2_console.config import Config
from mm2_console.engine import EngineClient, Reply
from mm2_console.server import ConsoleServer
from mm2_console.store import Store
from mm2_console.telegram import TelegramBot

ENGINE_SECRET = "test-engine-secret-" + "e" * 32
OWNER_SECRET = "test-owner-secret-" + "o" * 32
BOT_SECRET = "not-a-real-telegram-token-for-offline-tests"


class FakeEngine:
    def __init__(self):
        self.calls = []
        self.online = True
        self.command_reply = None

    def request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if method == "GET":
            return Reply(200, {"command_version": 7, "published_at_ms": int(time.time() * 1000),
                              "mode": "paused", "feed_ready": False, "execution_mode": "emulator",
                              "simulation": None, "source_status": {}}) if self.online else Reply(503, {"error": "engine_offline"})
        return self.command_reply or Reply(200, {"receipt": {"command_id": payload["id"], "applied_version": 8, "mode_at_ack": "paused"}, "replayed": False})


def signed_data(user_id=42, auth_date=None, extras=None):
    fields = {"auth_date": str(int(time.time()) if auth_date is None else auth_date),
              "user": json.dumps({"id": user_id, "first_name": "Owner"}, separators=(",", ":")), "query_id": "offline-fixture"}
    fields.update(extras or {})
    check = "\n".join(f"{key}={fields[key]}" for key in sorted(fields))
    secret = hmac.new(b"WebAppData", BOT_SECRET.encode(), hashlib.sha256).digest()
    fields["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return urlencode(fields)


class AuthTests(unittest.TestCase):
    def test_signed_init_data_owner_ttl_and_duplicate_fields(self):
        data = signed_data()
        self.assertEqual(len(validate_telegram(data, BOT_SECRET, 42)), 64)
        with self.assertRaises(ValueError): validate_telegram(data + "&auth_date=1", BOT_SECRET, 42)
        with self.assertRaises(ValueError): validate_telegram(signed_data(user_id=43), BOT_SECRET, 42)
        with self.assertRaises(ValueError): validate_telegram(signed_data(auth_date=int(time.time()) - 301), BOT_SECRET, 42)
        with self.assertRaises(ValueError): validate_telegram(signed_data(auth_date=int(time.time()) + 6), BOT_SECRET, 42)
        with self.assertRaises(ValueError): validate_telegram(data.replace("Owner", "Attacker"), BOT_SECRET, 42)
        with self.assertRaises(ValueError): validate_telegram(data, "", 42)

    def test_sessions_bounded_expiring_and_telegram_replay(self):
        clock = [0]
        sessions = Sessions(ttl=10, maximum=2, clock=lambda: clock[0])
        first, second, third = sessions.create(), sessions.create(), sessions.create()
        self.assertFalse(sessions.valid(first)); self.assertTrue(sessions.valid(second)); self.assertTrue(sessions.valid(third))
        sessions.consume_telegram("hash")
        with self.assertRaises(ValueError): sessions.consume_telegram("hash")
        clock[0] = 11
        self.assertFalse(sessions.valid(second))

    def test_rate_limit_has_bounded_key_count_and_refill(self):
        clock = [0]
        limiter = RateLimiter(capacity=2, refill=1, maximum=1, clock=lambda: clock[0])
        self.assertTrue(limiter.allow("a")); self.assertTrue(limiter.allow("a")); self.assertFalse(limiter.allow("a"))
        self.assertFalse(limiter.allow("b")); clock[0] = 1
        self.assertTrue(limiter.allow("a"))


class ConsoleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        (root / "static").mkdir()
        (root / "static" / "index.html").write_text("<html>owner console</html>")
        config = Config("http://127.0.0.1:1", ENGINE_SECRET, OWNER_SECRET, root, root / "static", frozenset())
        self.engine = FakeEngine()
        self.app = App(config, self.engine)
        self.server = ConsoleServer(("127.0.0.1", 0), self.app)
        self.port = self.server.server_address[1]
        self.origin = f"http://127.0.0.1:{self.port}"
        self.app.config = replace(config, allowed_origins=frozenset({self.origin}))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(2)
        self.app.close(); self.temp.cleanup()

    def request(self, method, path, body=None, cookie=None, origin=True, headers=None):
        values = {"Content-Type": "application/json"}
        if origin: values["Origin"] = self.origin if origin is True else origin
        if cookie: values["Cookie"] = cookie
        values.update(headers or {})
        client = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        client.request(method, path, None if body is None else json.dumps(body), values)
        response = client.getresponse()
        raw = response.read()
        result = (response.status, dict(response.getheaders()), json.loads(raw) if response.getheader("Content-Type") == "application/json" else raw)
        client.close()
        return result

    def login(self):
        status, headers, _ = self.request("POST", "/api/auth/local", {"token": OWNER_SECRET})
        self.assertEqual(status, 200)
        self.assertIn("HttpOnly", headers["Set-Cookie"])
        self.assertIn("SameSite=Strict", headers["Set-Cookie"])
        return headers["Set-Cookie"].split(";", 1)[0]

    def test_owner_auth_csrf_and_revoked_cookie(self):
        self.assertEqual(self.request("GET", "/api/state")[0], 401)
        self.assertEqual(self.request("POST", "/api/auth/local", {"token": OWNER_SECRET}, origin="https://attacker.invalid")[0], 403)
        self.assertEqual(self.request("POST", "/api/auth/local", {"token": "bad"})[0], 401)
        cookie = self.login()
        command = {"id": "owner-command", "expected_version": 7, "action": "pause"}
        self.assertEqual(self.request("POST", "/api/control", command, cookie, origin=False)[0], 403)
        self.assertEqual(self.request("POST", "/api/control", command, cookie, origin="https://attacker.invalid")[0], 403)
        self.assertFalse(self.engine.calls)
        self.assertEqual(self.request("POST", "/api/logout", {}, cookie)[0], 200)
        self.assertEqual(self.request("GET", "/api/state", cookie=cookie)[0], 401)

    def test_proxy_preserves_identity_cas_conflict_and_unknown_outcome(self):
        cookie = self.login()
        command = {"id": "stable-id", "expected_version": 7, "action": "reset"}
        self.engine.command_reply = Reply(503, {"error": "outcome_unknown", "message": "retry same identity"})
        self.assertEqual(self.request("POST", "/api/control", command, cookie)[0], 503)
        self.engine.command_reply = Reply(200, {"receipt": {"command_id": "stable-id", "applied_version": 8}, "replayed": True})
        self.assertTrue(self.request("POST", "/api/control", command, cookie)[2]["replayed"])
        self.assertEqual(self.engine.calls[0][2], self.engine.calls[1][2])
        self.engine.command_reply = Reply(409, {"error": "version_conflict", "message": "CAS rejected"})
        self.assertEqual(self.request("POST", "/api/control", command, cookie)[0], 409)

    def test_browser_reads_never_poll_engine_and_offline_state_is_explicit(self):
        cookie = self.login()
        self.assertIsNone(self.request("GET", "/api/state", cookie=cookie)[2]["state"])
        self.app.sample_once()
        for _ in range(8): self.request("GET", "/api/state", cookie=cookie)
        self.assertEqual(len(self.engine.calls), 1)
        self.engine.online = False; self.app.sample_once()
        state = self.request("GET", "/api/state", cookie=cookie)[2]
        self.assertFalse(state["connected"]); self.assertIsNotNone(state["state"])
        self.assertIn("last successful", state["error"])

    def test_comparison_requires_owner_and_reads_never_send_commands_or_poll_engine(self):
        self.assertEqual(self.request("GET", "/api/comparison")[0], 401)
        cookie = self.login()
        self.app.sample_once()
        before = list(self.engine.calls)
        for period in ("1h", "6h", "24h", "all"):
            status, _, value = self.request("GET", "/api/comparison?period=" + period, cookie=cookie)
            self.assertEqual(status, 200)
            self.assertEqual(value["requested_period"], period)
            self.assertEqual(value["reference"]["status"], "UNAVAILABLE")
        self.assertEqual(self.engine.calls, before)
        self.assertEqual(self.request("GET", "/api/comparison?period=7d", cookie=cookie)[0], 400)
        self.assertEqual(self.request("GET", "/api/comparison?period=1h&period=24h", cookie=cookie)[0], 400)
        self.assertEqual(self.request("POST", "/api/comparison", {}, cookie)[0], 404)
        self.assertEqual(self.engine.calls, before)

    def test_sampler_start_is_single_and_reports_bounded_persistence(self):
        self.app.start(); first = self.app._sampler; self.app.start()
        self.assertIs(first, self.app._sampler)
        time.sleep(1.15)
        self.assertGreaterEqual(len(self.engine.calls), 2)
        self.assertLessEqual(len(self.engine.calls), 3)
        self.assertEqual(self.app.state()["telemetry"]["sampling_interval_ms"], 1000)
        self.assertEqual(len(self.app.store.history(120)), len(self.engine.calls))

    def test_cached_200_engine_publication_stale_invalid_or_future_is_not_connected(self):
        cookie = self.login()
        now = int(time.time() * 1000)
        for published, reason in ((now - 4000, "older than 3 seconds"), (now + 6000, "clock is invalid"), (None, "clock is invalid"), (True, "clock is invalid")):
            self.engine.request = lambda *_, stamp=published: Reply(200, {"command_version": 7, "published_at_ms": stamp, "feed_ready": True, "source_status": {}})
            self.app.sample_once()
            envelope = self.request("GET", "/api/state", cookie=cookie)[2]
            self.assertFalse(envelope["connected"])
            self.assertIn(reason, envelope["error"])
            self.assertEqual(envelope["state"]["command_version"], 7)
        self.engine.request = lambda *_: Reply(200, {"command_version": 7, "published_at_ms": int(time.time() * 1000), "feed_ready": True, "source_status": {}})
        self.app.sample_once(); self.assertTrue(self.app.state()["connected"])

    def test_state_history_logs_and_proxy_never_expose_configured_secrets(self):
        cookie = self.login()
        self.engine.request = lambda *_: Reply(200, {"command_version": 1, "source_status": {}, "authorization": ENGINE_SECRET,
                                                    "message": OWNER_SECRET + BOT_SECRET, "nested": {"api_token": ENGINE_SECRET}})
        self.app.config = replace(self.app.config, telegram_token=BOT_SECRET, telegram_owner=42)
        self.app.sample_once(); self.app.log("INFO", ENGINE_SECRET + OWNER_SECRET + BOT_SECRET)
        for path in ("/api/state", "/api/history", "/api/logs"):
            text = json.dumps(self.request("GET", path, cookie=cookie)[2])
            for secret in (ENGINE_SECRET, OWNER_SECRET, BOT_SECRET): self.assertNotIn(secret, text)

    def test_oversized_body_and_static_traversal_are_rejected(self):
        cookie = self.login()
        self.assertEqual(self.request("POST", "/api/control", {"oversized": "x" * 17000}, cookie)[0], 400)
        self.assertEqual(self.request("GET", "/%2e%2e/secondary.sqlite3")[0], 404)
        self.assertEqual(self.request("GET", "/")[0], 200)

    def test_telegram_owner_only_and_unknown_outcome_reuses_payload_with_mock_transport(self):
        self.app.config = replace(self.app.config, telegram_token=BOT_SECRET, telegram_owner=42, miniapp_url="https://console.example/")
        self.app.sample_once()
        bot = TelegramBot(self.app, transport=object())
        message = {"update_id": 10, "message": {"from": {"id": 42}, "chat": {"id": 42, "type": "private"}, "text": "/pause", "date": int(time.time())}}
        stranger = json.loads(json.dumps(message)); stranger["message"]["from"]["id"] = 99
        self.assertIsNone(bot.handle_update(stranger))
        self.engine.command_reply = Reply(503, {"error": "outcome_unknown"})
        self.assertIn("SAME ID", bot.handle_update(message)["text"])
        first = self.engine.calls[-1][2]
        self.engine.command_reply = Reply(200, {"receipt": {"command_id": first["id"], "applied_version": 8, "mode_at_ack": "paused"}})
        message["update_id"] = 11
        self.assertIn("Durable acknowledgement", bot.handle_update(message)["text"])
        self.assertEqual(self.engine.calls[-1][2], first)
        message["message"]["text"] = "/start"
        self.assertEqual(bot.handle_update(message)["reply_markup"]["inline_keyboard"][0][0]["web_app"]["url"], "https://console.example/")
        disabled = TelegramBot(App(replace(self.app.config, telegram_token="", telegram_owner=None), FakeEngine(), self.app.store), transport=object())
        disabled.start(); self.assertIsNone(disabled._thread)

    def test_telegram_ambiguous_200_retains_pending_until_matching_durable_receipt(self):
        self.app.config = replace(self.app.config, telegram_token=BOT_SECRET, telegram_owner=42)
        self.app.sample_once(); bot = TelegramBot(self.app, transport=object())
        update = {"update_id": 20, "message": {"from": {"id": 42}, "chat": {"id": 42, "type": "private"}, "text": "/pause", "date": int(time.time())}}
        for payload in ({}, {"receipt": {"command_id": "foreign", "applied_version": 8, "mode_at_ack": "paused"}}, {"receipt": {"command_id": "tg-42-20", "applied_version": True, "mode_at_ack": "paused"}}, {"receipt": {"command_id": "tg-42-20", "applied_version": -1, "mode_at_ack": "paused"}}):
            self.engine.command_reply = Reply(200, payload)
            self.assertIn("Outcome unknown", bot.handle_update(update)["text"])
            saved = json.loads(self.app.store.get_meta("telegram_command")); self.assertTrue(saved["pending"])
            self.assertEqual(saved["payload"], {"id": "tg-42-20", "expected_version": 7, "action": "pause"})
        self.engine.command_reply = Reply(409, {"error": "version_conflict", "message": "CAS rejected"})
        self.assertIn("HTTP 409", bot.handle_update(update)["text"])
        self.assertFalse(json.loads(self.app.store.get_meta("telegram_command"))["pending"])

    def test_telegram_auth_or_rate_rejection_does_not_settle_original_unknown(self):
        self.app.config = replace(self.app.config, telegram_token=BOT_SECRET, telegram_owner=42)
        self.app.sample_once(); bot = TelegramBot(self.app, transport=object())
        update = {"update_id": 30, "message": {"from": {"id": 42}, "chat": {"id": 42, "type": "private"}, "text": "/resume", "date": int(time.time())}}
        for status in (503, 401, 403, 429):
            self.engine.command_reply = Reply(status, {"error": "outcome_unknown" if status == 503 else "rejected"})
            self.assertIn("Outcome unknown", bot.handle_update(update)["text"])
            self.assertTrue(json.loads(self.app.store.get_meta("telegram_command"))["pending"])
            self.assertEqual(self.engine.calls[-1][2]["id"], "tg-42-30")


class PersistenceTests(unittest.TestCase):
    def test_retention_capacity_restart_and_oversized_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "secondary.sqlite3"
            store = Store(path, max_samples=2, max_logs=2, retention_ms=100)
            for stamp in (1, 2, 3): store.sample(stamp, {"command_version": stamp}); store.log(stamp, "INFO", "secondary")
            self.assertEqual([row["observed_at_ms"] for row in store.history(120)], [2, 3])
            self.assertEqual(len(store.logs(100)), 2)
            self.assertFalse(store.sample(4, {"large": "x" * 70000}))
            store.set_meta("telegram_offset", 17); store.close()
            store = Store(path, max_samples=2, max_logs=2, retention_ms=100)
            self.assertEqual(store.get_meta("telegram_offset"), "17")
            store.sample(200, {"command_version": 9})
            self.assertEqual(len(store.history(120)), 1); store.close()

    def test_command_validation_rejects_bool_cas_and_rounding(self):
        with self.assertRaises(ValueError): command_payload({"id": "x", "expected_version": True, "action": "pause"})
        value = {"id": "x", "expected_version": 1, "limits": {"initial_collateral_usd": "1000", "max_imbalance_shares": "25",
                 "lot_shares": "5", "half_width": "0.0000001", "max_feed_age_ms": 1000}}
        with self.assertRaises(ValueError): command_payload(value, limits=True)

    def test_internal_http_auth_header_and_response_secret_redaction(self):
        class Response(io.BytesIO):
            status = 200
        class Opener:
            def open(self, request, timeout):
                self.request = request
                return Response(json.dumps({"api_token": ENGINE_SECRET, "message": ENGINE_SECRET, "command_version": 1}).encode())
        opener = Opener(); client = EngineClient("http://private-engine", ENGINE_SECRET, opener=opener)
        reply = client.request("GET", "/v1/state")
        self.assertEqual(opener.request.get_header("Authorization"), "Bearer " + ENGINE_SECRET)
        self.assertNotIn(ENGINE_SECRET, json.dumps(reply.payload))
        with self.assertRaises(ValueError): client.request("GET", "/arbitrary")


if __name__ == "__main__": unittest.main()
