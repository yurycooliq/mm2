"""Additional operators keep signed identity and private reply/command ownership."""
import hashlib
import hmac
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

from mm2_console.app import App
from mm2_console.auth import Sessions, telegram_session, validate_telegram
from mm2_console.config import Config
from mm2_console.engine import Reply
from mm2_console.telegram import TelegramBot

OWNER, OPERATOR, STRANGER = 42, 1401626578, 99
BOT = "offline-additional-operator-bot-token"
NOW = 1_800_000_000
ACCESS = frozenset({OPERATOR})


def signed(user_id, date=NOW):
    fields = {"auth_date": str(date), "user": json.dumps({"id": user_id}), "query_id": "access-fixture"}
    check = "\n".join(f"{key}={fields[key]}" for key in sorted(fields))
    key = hmac.new(b"WebAppData", BOT.encode(), hashlib.sha256).digest()
    fields["hash"] = hmac.new(key, check.encode(), hashlib.sha256).hexdigest()
    return urlencode(fields)


class AccessAuthenticationTests(unittest.TestCase):
    def test_configuration_adds_access_without_replacing_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            env = {"MM2_CONSOLE_STATE_DIR": directory, "MM2_CONSOLE_TOKEN": "o" * 64,
                   "MM2_ENGINE_API_TOKEN": "e" * 64, "MM2_TELEGRAM_BOT_TOKEN": BOT,
                   "MM2_TELEGRAM_OWNER_ID": str(OWNER), "MM2_TELEGRAM_ALLOWED_IDS": str(OPERATOR)}
            with patch.dict(os.environ, env, clear=True):
                config = Config.from_env()
                self.assertEqual(config.telegram_owner, OWNER)
                self.assertEqual(config.telegram_users, {OWNER, OPERATOR})
                self.assertTrue(config.telegram_only)
                for invalid in ("0", "-1", "true", "1.0", "1,", "1, 2", str(2**52), ",".join(["1"] * 17)):
                    with self.subTest(invalid=invalid), patch.dict(os.environ, {"MM2_TELEGRAM_ALLOWED_IDS": invalid}), self.assertRaises(ValueError):
                        Config.from_env()
                with patch.dict(os.environ, {"MM2_TELEGRAM_BOT_TOKEN": "", "MM2_TELEGRAM_OWNER_ID": ""}), self.assertRaises(ValueError):
                    Config.from_env()

    def test_only_exact_signed_allowlisted_integer_users_authenticate(self):
        for user_id in (OWNER, OPERATOR):
            self.assertEqual(len(validate_telegram(signed(user_id), BOT, OWNER, NOW, allowed_ids=ACCESS)), 64)
        for user_id in (STRANGER, str(OPERATOR), True):
            with self.subTest(user_id=user_id), self.assertRaises(ValueError):
                validate_telegram(signed(user_id), BOT, OWNER, NOW, allowed_ids=ACCESS)
        for data in (signed(OPERATOR).replace(str(OPERATOR), str(OWNER)), signed(OPERATOR, NOW - 301)):
            with self.assertRaises(ValueError):
                validate_telegram(data, BOT, OWNER, NOW, allowed_ids=ACCESS)
        for invalid in (frozenset({True}), frozenset({0}), frozenset({2**52}), [OPERATOR]):
            with self.assertRaises(ValueError):
                validate_telegram(signed(OPERATOR), BOT, OWNER, NOW, allowed_ids=invalid)

    def test_replay_and_old_launch_binding_stay_separate_between_operators(self):
        clock = [0]
        sessions = Sessions(clock=lambda: clock[0])
        owner = telegram_session(sessions, "", signed(OWNER), BOT, OWNER, NOW, allowed_ids=ACCESS)
        operator = telegram_session(sessions, "", signed(OPERATOR), BOT, OWNER, NOW, allowed_ids=ACCESS)
        self.assertNotEqual(owner, operator)
        with self.assertRaises(ValueError):
            telegram_session(sessions, owner, signed(OPERATOR), BOT, OWNER, NOW, allowed_ids=ACCESS)
        clock[0] = 301
        self.assertEqual(telegram_session(sessions, operator, signed(OPERATOR), BOT, OWNER, NOW + 301, allowed_ids=ACCESS), operator)
        with self.assertRaises(ValueError):
            telegram_session(sessions, owner, signed(OPERATOR), BOT, OWNER, NOW + 301, allowed_ids=ACCESS)
        with self.assertRaises(ValueError):
            telegram_session(sessions, operator, signed(OPERATOR), BOT, OWNER, NOW + 301)


class AccessBotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        config = Config("http://engine:8088", "e" * 64, "o" * 64, root, root, frozenset(),
                        telegram_token=BOT, telegram_owner=OWNER, telegram_allowed_ids=ACCESS,
                        miniapp_url="https://console.example/")
        self.calls, self.deliveries = [], []
        fixture = self

        class Engine:
            def request(self, method, path, payload=None):
                fixture.calls.append((method, path, payload))
                return Reply(200, {"receipt": {"command_id": payload["id"], "applied_version": 2, "mode_at_ack": "paused"}})

        class Transport:
            def call(self, method, payload):
                fixture.deliveries.append((method, payload.copy()))

        self.app = App(config, Engine())
        self.app._snapshot = {"connected": True, "observed_at_ms": int(time.time() * 1000),
                              "state": {"published_at_ms": int(time.time() * 1000), "command_version": 1}, "error": None}
        self.bot = TelegramBot(self.app, Transport())

    def tearDown(self):
        self.bot.close()
        self.app.close()
        self.temp.cleanup()

    def update(self, user_id, action="/start", update_id=20):
        return {"update_id": update_id, "message": {"from": {"id": user_id},
                "chat": {"id": user_id, "type": "private"}, "text": action, "date": int(time.time())}}

    def test_both_users_get_private_replies_and_photo_goes_to_requester(self):
        for user_id in (OWNER, OPERATOR):
            reply = self.bot.handle_update(self.update(user_id))
            self.assertEqual(reply["chat_id"], user_id)
            self.assertIn("reply_markup", reply)
        reply = self.bot.handle_update(self.update(OPERATOR, "/stats"))
        self.bot.send_reply(reply)
        self.assertEqual([(method, payload["chat_id"]) for method, payload in self.deliveries],
                         [("sendMessage", OPERATOR), ("sendPhoto", OPERATOR)])
        self.assertEqual(self.calls, [])

    def test_strangers_bots_groups_and_foreign_chats_have_no_effect(self):
        updates = [self.update(STRANGER), self.update(str(OPERATOR)), self.update(True)]
        foreign = self.update(OPERATOR); foreign["message"]["chat"]["id"] = OWNER; updates.append(foreign)
        group = self.update(OPERATOR); group["message"]["chat"]["type"] = "group"; updates.append(group)
        bot = self.update(OPERATOR); bot["message"]["from"]["is_bot"] = True; updates.append(bot)
        for update in updates:
            self.assertIsNone(self.bot.handle_update(update))
        with self.assertRaises(ValueError):
            self.bot.send_reply({"chat_id": STRANGER, "text": "private"})
        self.assertEqual(self.calls, [])
        self.assertEqual(self.deliveries, [])

    def test_new_operator_command_has_its_own_durable_identity(self):
        self.assertIn("Durable acknowledgement", self.bot.handle_update(self.update(OPERATOR, "/pause"))["text"])
        self.assertEqual(self.calls[0][2], {"id": f"tg-{OPERATOR}-20", "expected_version": 1, "action": "pause"})
        previous = json.loads(self.app.store.get_meta("telegram_command"))
        self.assertEqual(previous["sender_id"], OPERATOR)
        self.assertFalse(previous["pending"])
        self.assertIn("Durable acknowledgement", self.bot.handle_update(self.update(OWNER, "/pause", 21))["text"])
        self.assertEqual(self.calls[1][2]["id"], "tg-42-21")

    def test_unknown_command_cannot_be_taken_over_including_legacy_owner_record(self):
        for legacy in (False, True):
            original = {"update_id": 10, "pending": True, "payload": {"id": "tg-42-10", "expected_version": 1, "action": "pause"}}
            if not legacy:
                original["sender_id"] = OWNER
            self.app.store.set_meta("telegram_command", json.dumps(original))
            self.assertIn("Another operator", self.bot.handle_update(self.update(OPERATOR, "/pause"))["text"])
            self.assertEqual(json.loads(self.app.store.get_meta("telegram_command")), original)
        self.assertEqual(self.calls, [])
        self.assertIn("Durable acknowledgement", self.bot.handle_update(self.update(OWNER, "/pause", 22))["text"])
        self.assertEqual(self.calls[0][2]["id"], "tg-42-10")


if __name__ == "__main__":
    unittest.main()
