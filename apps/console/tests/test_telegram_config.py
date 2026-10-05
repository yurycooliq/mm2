"""Production Telegram policy cannot fall back to owner-token login."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mm2_console.config import Config


class TelegramConfigTests(unittest.TestCase):
    def configured(self, **extra):
        with tempfile.TemporaryDirectory() as directory:
            environment = {"MM2_CONSOLE_STATE_DIR": directory,
                           "MM2_ENGINE_API_TOKEN": "e" * 64,
                           "MM2_CONSOLE_TOKEN": "o" * 64, **extra}
            with patch.dict(os.environ, environment, clear=True):
                return Config.from_env()

    def test_explicit_production_policy_stays_closed_until_credentials_exist(self):
        config = self.configured(MM2_TELEGRAM_ONLY="true")
        self.assertTrue(config.telegram_only)
        self.assertEqual(config.telegram_token, "")
        self.assertIsNone(config.telegram_owner)
        compose = (Path(__file__).resolve().parents[3] / "infra/aws/compose.yaml").read_text()
        self.assertIn('MM2_TELEGRAM_ONLY: "true"', compose)

    def test_configured_bot_cannot_be_downgraded_to_token_login(self):
        for policy in ("true", "false"):
            config = self.configured(MM2_TELEGRAM_ONLY=policy,
                                     MM2_TELEGRAM_BOT_TOKEN="123456789:" + "x" * 35,
                                     MM2_TELEGRAM_OWNER_ID="366612345")
            self.assertTrue(config.telegram_only)

    def test_local_development_compatibility_and_invalid_policy(self):
        self.assertFalse(self.configured().telegram_only)
        with self.assertRaises(ValueError):
            self.configured(MM2_TELEGRAM_ONLY="yes")


if __name__ == "__main__":
    unittest.main()
