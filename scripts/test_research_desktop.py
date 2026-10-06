"""Local desktop CLI encoding regression; no server or external operations."""
import io
import json
import unittest
from unittest.mock import patch

from scripts import research_desktop
from tools.research.schema import canonical


class DesktopEncodingTest(unittest.TestCase):
    def test_cli_emits_utf8_json_with_russian_and_emoji_on_cp1252_stdout(self):
        result = {"title": "Исправление — проверка", "message": "Доходность неизвестна 📈"}
        output = io.BytesIO()
        text_stdout = io.TextIOWrapper(output, encoding="cp1252", errors="strict")
        try:
            with patch.object(research_desktop.sys, "argv", ["research_desktop.py", "status"]), \
                    patch.object(research_desktop.sys, "stdout", text_stdout), \
                    patch.object(research_desktop, "bridge", return_value=result) as bridge:
                exit_code = research_desktop.main()
            text_stdout.flush()
            actual = output.getvalue()
            self.assertEqual(exit_code, 0)
            self.assertEqual(actual, canonical(result) + b"\n")
            self.assertEqual(json.loads(actual.decode("utf-8")), result)
            bridge.assert_called_once_with({"command": "status"})
        finally:
            text_stdout.detach()


if __name__ == "__main__":
    unittest.main()
