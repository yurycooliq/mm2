import hashlib
import hmac
import json
import unittest
from urllib.parse import urlencode

from mm2_console.auth import Sessions, validate_telegram

BOT = "offline-telegram-auth-fixture-token"
NOW = 1_800_000_000
OWNER = 3_666_000_000


def signed(fields=None, user=None, auth_date=NOW):
    values = {"auth_date": str(auth_date), "user": json.dumps(user or {"id": OWNER, "first_name": "Owner"}),
              "query_id": "offline-auth-proof", "signature": "fixture-third-party-signature"}
    values.update(fields or {})
    check = "\n".join(f"{key}={values[key]}" for key in sorted(values))
    key = hmac.new(b"WebAppData", BOT.encode(), hashlib.sha256).digest()
    values["hash"] = hmac.new(key, check.encode(), hashlib.sha256).hexdigest()
    return urlencode(values)


class TelegramAuthenticationTests(unittest.TestCase):
    def test_only_the_exact_signed_integer_owner_is_accepted(self):
        self.assertEqual(len(validate_telegram(signed(), BOT, OWNER, now=NOW)), 64)
        for user in ({"id": OWNER + 1}, {"id": str(OWNER)}, {"id": True}, {"id": OWNER, "is_bot": True}, {}):
            with self.subTest(user=user), self.assertRaises(ValueError):
                validate_telegram(signed(fields={"user": json.dumps(user)}), BOT, OWNER, now=NOW)
        for configured_owner in (True, "3666000000", 0, -1, 2**52, None):
            with self.subTest(configured_owner=configured_owner), self.assertRaises(ValueError):
                validate_telegram(signed(), BOT, configured_owner, now=NOW)

    def test_telegram_signature_covers_all_fields_including_the_new_signature_field(self):
        data = signed()
        for mutation in (data.replace("fixture-third-party-signature", "other"), data.replace("Owner", "Attacker"),
                         data.replace(str(OWNER), str(OWNER + 1)), data + "&auth_date=1", data + "&hash=" + "0" * 64):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_telegram(mutation, BOT, OWNER, now=NOW)
        with self.assertRaises(ValueError):
            validate_telegram(data, BOT + "wrong", OWNER, now=NOW)

    def test_timestamp_acceptance_has_explicit_expiry_and_future_boundaries(self):
        for timestamp in (NOW - 300, NOW + 5):
            self.assertEqual(len(validate_telegram(signed(auth_date=timestamp), BOT, OWNER, now=NOW)), 64)
        for timestamp in (NOW - 301, NOW + 6):
            with self.assertRaises(ValueError):
                validate_telegram(signed(auth_date=timestamp), BOT, OWNER, now=NOW)

    def test_duplicate_user_fields_nonfinite_json_and_bad_percent_encoding_are_rejected(self):
        for value in ('{"id":1,"id":3666000000}', '{"id":3666000000,"first_name":NaN}'):
            with self.assertRaises(ValueError):
                validate_telegram(signed(fields={"user": value}), BOT, OWNER, now=NOW)
        for raw in (signed() + "&extra=%xx", signed() + "&extra=%", "x" * 8193):
            with self.assertRaises(ValueError):
                validate_telegram(raw, BOT, OWNER, now=NOW)

    def test_reload_needs_the_original_live_session_and_matching_validated_digest(self):
        clock = [0]
        sessions = Sessions(ttl=20, maximum=2, clock=lambda: clock[0])
        digest = validate_telegram(signed(), BOT, OWNER, now=NOW)
        sessions.consume_telegram(digest)
        original = sessions.create(telegram_digest=digest)
        local = sessions.create()
        self.assertTrue(sessions.valid(original, telegram_digest=digest))
        self.assertFalse(sessions.valid(original, telegram_digest="0" * 64))
        self.assertFalse(sessions.valid(local, telegram_digest=digest))
        self.assertFalse(sessions.valid("another-client-cookie", telegram_digest=digest))
        with self.assertRaises(ValueError):
            sessions.consume_telegram(digest)
        clock[0] = 20
        self.assertFalse(sessions.valid(original, telegram_digest=digest))
        self.assertEqual(sessions._telegram_bindings, {})

    def test_replay_fence_covers_future_skew_and_session_eviction_revocation(self):
        clock = [0]
        sessions = Sessions(ttl=3600, maximum=1, clock=lambda: clock[0])
        sessions.consume_telegram("digest")
        original = sessions.create(telegram_digest="digest")
        clock[0] = 305
        with self.assertRaises(ValueError):
            sessions.consume_telegram("digest")
        replacement = sessions.create(telegram_digest="second")
        self.assertFalse(sessions.valid(original))
        self.assertEqual(len(sessions._telegram_bindings), 1)
        sessions.revoke(replacement)
        self.assertEqual(sessions._telegram_bindings, {})


if __name__ == "__main__":
    unittest.main()
