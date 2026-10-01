import json
import ssl
import threading
import time
from urllib.request import build_opener, HTTPSHandler, Request

from .engine import NoRedirect, redact


class TelegramTransport:
    def __init__(self, token):
        self._base = "https://api.telegram.org/bot" + token + "/"
        self._opener = build_opener(NoRedirect(), HTTPSHandler(context=ssl.create_default_context()))

    def call(self, method, payload):
        if method not in {"getUpdates", "sendMessage"}:
            raise ValueError("unsupported Telegram method")
        request = Request(self._base + method, data=json.dumps(payload).encode(),
                          headers={"Content-Type": "application/json"}, method="POST")
        try:
            with self._opener.open(request, timeout=7) as response:
                raw = response.read(262_145)
            if len(raw) > 262_144:
                raise ValueError("Telegram response exceeds limit")
            value = json.loads(raw)
            if not isinstance(value, dict) or value.get("ok") is not True:
                raise ValueError("Telegram response unavailable")
            return value["result"]
        except Exception:
            # Provider URL contains the token. Never propagate raw transport exceptions.
            raise RuntimeError("Telegram transport unavailable") from None


class TelegramBot:
    def __init__(self, app, transport=None):
        self.app = app
        self.enabled = bool(app.config.telegram_token and app.config.telegram_owner)
        self.transport = transport or (TelegramTransport(app.config.telegram_token) if self.enabled else None)
        self.stop = threading.Event()
        self._thread = None
        self._last_failure_log = 0

    def start(self):
        if self.enabled and self._thread is None:
            self._thread = threading.Thread(target=self._loop, name="owner-telegram", daemon=True)
            self._thread.start()

    def _status(self):
        envelope = self.app.state()
        state = envelope["state"]
        if not state:
            return "Engine unavailable; no state sampled. Live execution disabled."
        return f"Engine {state.get('mode', 'unknown')} | {state.get('execution_mode', 'unknown')}\nConnected: {envelope['connected']} | feeds ready: {state.get('feed_ready', False)}\nVersion: {state.get('command_version')} | live disabled\nBalances are simulated in emulator; real account funds are unknown."

    def handle_update(self, update):
        if not self.enabled:
            return None
        message = update.get("message") if isinstance(update, dict) else None
        if not isinstance(message, dict):
            return None
        owner = self.app.config.telegram_owner
        sender, chat = message.get("from", {}), message.get("chat", {})
        if not isinstance(sender, dict) or not isinstance(chat, dict) or type(sender.get("id")) is not int or type(chat.get("id")) is not int:
            return None
        if sender.get("id") != owner or chat.get("id") != owner or chat.get("type") != "private" or sender.get("is_bot"):
            return None
        text = message.get("text", "")
        if not isinstance(text, str) or len(text) > 256:
            return None
        command = text.split(maxsplit=1)[0].split("@", 1)[0] if text else ""
        result = {"chat_id": owner, "text": "Commands: /start /status /pause /resume /kill /reset /logs"}
        if command in {"/start", "/status"}:
            result["text"] = self._status()
            if command == "/start" and self.app.config.miniapp_url:
                result["reply_markup"] = {"inline_keyboard": [[{"text": "Open operator console", "web_app": {"url": self.app.config.miniapp_url}}]]}
        elif command == "/logs":
            lines = self.app.store.logs(10)
            result["text"] = "Secondary logs (not the trading ledger):\n" + ("\n".join(f"{row['level']}: {row['message']}" for row in reversed(lines)) or "No logs sampled.")
        elif command in {"/pause", "/resume", "/kill", "/reset"}:
            message_date = message.get("date")
            if type(message_date) is not int or not -5 <= int(time.time()) - message_date <= 300:
                result["text"] = "Command is expired or has no valid date; request was not sent."
                return result
            if not self.app.command_rate.allow("telegram-owner"):
                result["text"] = "Command rate limited; request was not sent."
                return result
            envelope = self.app.state()
            state = envelope["state"]
            if not envelope["connected"] or not state:
                result["text"] = "Engine state is unavailable/stale; command was not sent."
                return result
            update_id = update.get("update_id")
            if type(update_id) is not int or not 0 <= update_id < 2**63:
                return None
            previous = json.loads(self.app.store.get_meta("telegram_command", "null"))
            action = command[1:]
            if previous and (previous.get("update_id") == update_id or previous.get("pending")):
                if previous["payload"]["action"] != action:
                    result["text"] = "A prior command has an unknown outcome. Repeat that same command to reconcile its original ID and payload."
                    return result
                payload = previous["payload"]
            else:
                payload = {"id": f"tg-{owner}-{update_id}", "expected_version": state["command_version"], "action": action}
                previous = {"update_id": update_id, "payload": payload, "pending": True}
                # Persist identity before a possibly outcome-unknown request.
                self.app.store.set_meta("telegram_command", json.dumps(previous))
            reply = self.app.proxy(payload)
            receipt = reply.payload.get("receipt")
            acknowledged = (reply.status == 200 and isinstance(receipt, dict)
                            and receipt.get("command_id") == payload["id"]
                            and type(receipt.get("applied_version")) is int
                            and 0 <= receipt["applied_version"] <= 2**53 - 1
                            and receipt.get("mode_at_ack") in {"paused", "shadow", "killed"})
            rejected = 400 <= reply.status < 500 and reply.status not in {401, 403, 408, 429} and reply.payload.get("error") != "outcome_unknown"
            unknown = not acknowledged and not rejected
            previous["pending"] = unknown
            self.app.store.set_meta("telegram_command", json.dumps(previous))
            if acknowledged:
                result["text"] = f"Durable acknowledgement: {receipt.get('command_id')}\nVersion {receipt.get('applied_version')} | mode {receipt.get('mode_at_ack')}"
            elif unknown:
                result["text"] = f"Outcome unknown for {payload['id']}. Repeat {command} to retry the SAME ID and payload."
            else:
                result["text"] = f"Command HTTP {reply.status}: {reply.payload.get('message', 'no durable acknowledgement')}"
        result["text"] = redact(result["text"][:3500], self.app.config.secret_values)
        return result

    def _loop(self):
        try:
            offset = int(self.app.store.get_meta("telegram_offset", "0"))
        except (ValueError, OSError):
            self.app.log("ERROR", "Telegram offset recovery failed; polling disabled")
            return
        while not self.stop.is_set():
            try:
                updates = self.transport.call("getUpdates", {"offset": offset, "timeout": 5, "limit": 20, "allowed_updates": ["message"]})
                if not isinstance(updates, list) or len(updates) > 20:
                    raise ValueError("invalid Telegram update batch")
                for update in updates:
                    if self.stop.is_set():
                        break
                    update_id = update.get("update_id") if isinstance(update, dict) else None
                    if type(update_id) is not int or update_id < offset:
                        continue
                    reply = self.handle_update(update)
                    if reply:
                        self.transport.call("sendMessage", reply)
                    offset = update_id + 1
                    self.app.store.set_meta("telegram_offset", offset)
            except Exception:
                if time.monotonic() - self._last_failure_log > 60:
                    self.app.log("WARN", "Telegram polling unavailable; no raw provider data logged")
                    self._last_failure_log = time.monotonic()
                self.stop.wait(5)

    def close(self):
        self.stop.set()
        if self._thread:
            self._thread.join(timeout=8)
