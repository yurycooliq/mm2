import json
import secrets
import ssl
import threading
import time
from urllib.request import build_opener, HTTPSHandler, Request

from .engine import NoRedirect, redact
from .observability import execution_summary, money
from .telegram_reports import active_text, deposit_chart, notification_delta, stats_text


class TelegramTransport:
    def __init__(self, token):
        self._base = "https://api.telegram.org/bot" + token + "/"
        self._opener = build_opener(NoRedirect(), HTTPSHandler(context=ssl.create_default_context()))

    def call(self, method, payload):
        if method not in {"getUpdates", "sendMessage", "sendPhoto", "setChatMenuButton", "setMyCommands", "getMe"}:
            raise ValueError("unsupported Telegram method")
        if method == "sendPhoto":
            photo = payload.get("photo")
            if not isinstance(photo, bytes) or not 0 < len(photo) <= 1_048_576 or not photo.startswith(b"\x89PNG\r\n\x1a\n"):
                raise ValueError("Telegram photo must be a bounded generated PNG")
            fields = {key: value for key, value in payload.items() if key != "photo"}
            if set(fields) - {"chat_id", "caption", "disable_notification", "protect_content"}:
                raise ValueError("unsupported Telegram photo fields")
            boundary = "mm2-" + secrets.token_hex(16)
            parts = []
            for key, value in fields.items():
                value = json.dumps(value) if not isinstance(value, str) else value
                parts.append((f"--{boundary}\r\nContent-Disposition: form-data; name=\"{key}\"\r\n\r\n{value}\r\n").encode())
            parts.extend([(f"--{boundary}\r\nContent-Disposition: form-data; name=\"photo\"; filename=\"deposit.png\"\r\nContent-Type: image/png\r\n\r\n").encode(), photo,
                          (f"\r\n--{boundary}--\r\n").encode()])
            data, content_type = b"".join(parts), "multipart/form-data; boundary=" + boundary
        else:
            data, content_type = json.dumps(payload).encode(), "application/json"
        request = Request(self._base + method, data=data,
                          headers={"Content-Type": content_type}, method="POST")
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
        self._last_notification_sample = None

    def start(self):
        if self.enabled and self._thread is None:
            self._thread = threading.Thread(target=self._loop, name="owner-telegram", daemon=True)
            self._thread.start()

    def _status(self):
        envelope = self.app.state()
        state = envelope["state"]
        if not state:
            return "Engine unavailable; no state sampled. Execution profile and balances are unknown."
        summary = envelope.get("execution") or execution_summary(state, envelope["connected"])
        real, paper, journal = summary["real"], summary["paper"], summary["journal"]
        lines = [f"Engine {state.get('mode', 'unknown')} | profile {summary['profile']}",
                 f"Fresh sample: {envelope['connected']} | feeds ready: {state.get('feed_ready', False)}",
                 f"Version: {state.get('command_version')}"]
        if not envelope["connected"]:
            lines.append("STALE: cached state; current execution and balances are unverified.")
        if real["selected"]:
            lines.append(f"REAL trading: {real['phase'] or 'unavailable'} | reconciled: {real['account_reconciled']} | user stream: {real['user_stream_connected']}")
            if real["balances_known"]:
                lines.append(f"REAL funds USD: collateral {real['collateral_usd']} | available {real['available_usd']} | reserved {real['reserved_usd'] or 'unknown'}")
            else:
                lines.append("REAL funds USD: unknown until fresh account reconciliation.")
            if real["daily_loss_bound_usd"] is not None:
                label = "conservative gross purchase loss bound" if real["daily_loss_metric"] == "conservative_gross_purchase_loss_bound" else "risk bound (metric unverified)"
                lines.append(f"Daily risk USD: {real['daily_loss_bound_usd']} | {label}")
        else:
            lines.append("Real trading is not selected in this startup profile.")
        if paper["selected"]:
            lines.append(f"VIRTUAL simulation: {paper['phase'] or 'unknown'}{' | cached' if paper['last_sample_only'] else ''}")
            lines.append(f"VIRTUAL funds USD: global cash {paper['cash_usd'] or 'unknown'} | current market available {paper['available_usd'] or 'unknown'} | reserved {paper['reserved_usd'] or 'unknown'}")
            gate = state.get("strategy_gate")
            waiting = {"inventory_dust": "остаток меньше минимальной заявки",
                       "inventory_cost": "стоимость позиции или её точность не допускает новую заявку",
                       "loss_budget": "недостаточно бюджета риска с учётом позиций и возможных исполнений",
                       "invalid_book": "нет полной допустимой книги",
                       "stale_book": "устарели рыночные данные",
                       "close_guard": "приближается закрытие рынка"}
            orders = (state.get("simulation") or {}).get("orders")
            working = sum(item.get("phase") == "live" for item in orders if isinstance(item, dict)) if isinstance(orders, list) else None
            if not envelope["connected"]:
                reason = "текущий статус неизвестен: снимок устарел"
            elif state.get("mode") in {"paused", "killed"}:
                reason = "пауза" if state["mode"] == "paused" else "аварийная блокировка"
            elif state.get("awaiting_source_refresh"):
                reason = "восстанавливаются рыночные данные"
            else:
                reason = waiting.get(gate, "есть рабочие заявки" if working else "ожидание допустимых заявок")
            lines.append(f"Виртуальный контур: {reason}; рабочих заявок: {working if working is not None else 'неизвестно'}")
            resolution = state.get("paper_resolution") or {}
            policy = resolution.get("risk_policy") or {}
            portfolio = state.get("paper_portfolio") or {}
            limit, loss = policy.get("max_daily_loss_micros"), portfolio.get("daily_loss_pico")
            remaining = None
            if (envelope["connected"] and resolution.get("execution_costs") == "exact_per_outcome"
                    and type(limit) is int and 0 <= limit < 2**64 and money(loss, 10**12) is not None):
                remaining = money(max(0, limit * 10**6 - int(loss)), 10**12)
            lines.append(f"Остаток дневного бюджета USD до учёта открытого риска: {remaining if remaining is not None else 'неизвестен'}")
        lines.append("Economic PnL: unknown.")
        if journal["used_bytes"] is not None:
            lines.append(f"Engine journal: {journal['used_bytes']} / {journal['limit_bytes'] if journal['limit_bytes'] is not None else 'unknown'} bytes | {journal['stage']}")
        for label, item in (("Live journal", real["journal"]), ("Paper journal", paper["journal"])):
            if item["used_bytes"] is not None:
                lines.append(f"{label}: {item['used_bytes']} / {item['limit_bytes'] if item['limit_bytes'] is not None else 'unknown'} bytes | {item['stage']}")
        if real["prepare_error"]:
            lines.append("Live preparation: " + real["prepare_error"])
        for label, fault in (("Engine error", summary["engine_fault"]), ("Live error", real["fault"]), ("Paper error", paper["fault"])):
            if fault:
                lines.append(f"{label}: {fault}")
        return redact("\n".join(lines)[:3500], self.app.config.secret_values)

    def handle_update(self, update):
        if not self.enabled:
            return None
        message = update.get("message") if isinstance(update, dict) else None
        if not isinstance(message, dict):
            return None
        sender, chat = message.get("from", {}), message.get("chat", {})
        if not isinstance(sender, dict) or not isinstance(chat, dict) or type(sender.get("id")) is not int or type(chat.get("id")) is not int:
            return None
        user_id = sender["id"]
        if user_id not in self.app.config.telegram_users or chat["id"] != user_id or chat.get("type") != "private" or sender.get("is_bot"):
            return None
        text = message.get("text", "")
        if not isinstance(text, str) or len(text) > 256:
            return None
        command = text.split(maxsplit=1)[0].split("@", 1)[0] if text else ""
        result = {"chat_id": user_id, "text": "Команды: /start /status /stats /active /pause /resume /kill /reset /logs"}
        if command in {"/start", "/status"}:
            result["text"] = self._status()
            if command == "/start" and self.app.config.miniapp_url:
                result["reply_markup"] = {"inline_keyboard": [[{"text": "Open operator console", "web_app": {"url": self.app.config.miniapp_url}}]]}
        elif command == "/logs":
            lines = self.app.store.logs(10)
            result["text"] = "Secondary logs (not the trading ledger):\n" + ("\n".join(f"{row['level']}: {row['message']}" for row in reversed(lines)) or "No logs sampled.")
        elif command in {"/stats", "/active"}:
            if not self.app.command_rate.allow("telegram-report"):
                result["text"] = "Слишком много запросов статистики; повтори позже."
                return result
            envelope = self.app.state()
            if command == "/active":
                result["text"] = active_text(envelope)
            else:
                result["text"] = stats_text(envelope)
                result["_photo"] = deposit_chart(self.app.store.equity_history())
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
                # Records from the original owner-only console have no sender_id.
                # Another operator must never take over an uncertain command.
                if previous.get("sender_id", self.app.config.telegram_owner) != user_id:
                    result["text"] = "Another operator's command has an unknown outcome. That operator must reconcile the original command first."
                    return result
                if previous["payload"]["action"] != action:
                    result["text"] = "A prior command has an unknown outcome. Repeat that same command to reconcile its original ID and payload."
                    return result
                payload = previous["payload"]
            else:
                payload = {"id": f"tg-{user_id}-{update_id}", "expected_version": state["command_version"], "action": action}
                previous = {"update_id": update_id, "sender_id": user_id, "payload": payload, "pending": True}
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

    def configure_owner_menu(self):
        """Only the configured owner's private chat gets a Mini App menu."""
        if not self.enabled:
            return
        if self.app.config.miniapp_url:
            self.transport.call("setChatMenuButton", {"chat_id": self.app.config.telegram_owner,
                "menu_button": {"type": "web_app", "text": "mm2", "web_app": {"url": self.app.config.miniapp_url}}})
        self.transport.call("setMyCommands", {"scope": {"type": "chat", "chat_id": self.app.config.telegram_owner},
            "commands": [{"command": command, "description": description} for command, description in (
                ("start", "Открыть Mini App"), ("status", "Состояние движка"),
                ("stats", "Статистика и стоимость депозита"), ("active", "Активные позиции и ордера"),
                ("pause", "Приостановить"), ("kill", "Остановить исполнение"), ("logs", "Вторичные логи"))]})

    def notify_once(self):
        """Secondary sampled notifications; no engine request or unbounded outbox."""
        if not self.enabled:
            return
        now = time.monotonic()
        if self._last_notification_sample is not None and now - self._last_notification_sample < 5:
            return
        # Keep this bound inside the method: even immediate queued updates cannot
        # increase snapshot/cursor/network work. A suppressed call advances nothing.
        self._last_notification_sample = now
        envelope = self.app.state()
        if not envelope.get("connected") or not envelope.get("state"):
            return
        raw = self.app.store.get_meta("telegram_notification_cursor", "null")
        if len(raw) > 196_608:
            raise ValueError("Telegram notification cursor exceeds limit")
        previous = json.loads(raw)
        if previous is not None and (not isinstance(previous, dict) or previous.get("version") != 1):
            raise ValueError("Telegram notification cursor is invalid")
        cursor, messages, warnings = notification_delta(envelope["state"], previous)
        encoded = json.dumps(cursor, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode()) > 196_608:
            raise ValueError("Telegram notification cursor exceeds limit")
        if encoded != raw:
            # Telegram sendMessage has no idempotency key. Persist before sending:
            # a crash or an uncertain response can omit a secondary notification,
            # but never re-send historic fills in an unlimited restart loop.
            self.app.store.set_meta("telegram_notification_cursor", encoded)
        for warning in warnings[:3]:
            self.app.log("WARN", warning)
        if messages and not self.stop.is_set():
            text = redact(("\n\n".join(messages) + "\n\nВторичное наблюдение; журнал движка — источник исполнения.")[:3500], self.app.config.secret_values)
            try:
                self.transport.call("sendMessage", {"chat_id": self.app.config.telegram_owner, "text": text, "protect_content": True})
            except Exception:
                self.app.log("WARN", "Telegram execution notification delivery unknown; cursor retained, no blind resend")

    def send_reply(self, reply):
        if type(reply.get("chat_id")) is not int or reply["chat_id"] not in self.app.config.telegram_users:
            raise ValueError("Telegram reply recipient is not authorized")
        photo = reply.pop("_photo", None)
        self.transport.call("sendMessage", reply)
        if photo is not None and not self.stop.is_set():
            self.transport.call("sendPhoto", {"chat_id": reply["chat_id"], "photo": photo,
                "caption": "Стоимость депозита USD за последние 24 ч: 1 снимок/мин, оценка по best bid без глубины/комиссий. REAL и VIRTUAL разделены. Пробелы — неизвестная стоимость. История начинается с включения сбора; виртуальные сделки синтетические.",
                "protect_content": True})

    def _loop(self):
        try:
            offset = int(self.app.store.get_meta("telegram_offset", "0"))
        except (ValueError, OSError):
            self.app.log("ERROR", "Telegram offset recovery failed; polling disabled")
            return
        try:
            self.configure_owner_menu()
        except Exception:
            self.app.log("WARN", "Telegram owner menu unavailable; private /start link remains available")
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
                    offset = update_id + 1
                    self.app.store.set_meta("telegram_offset", offset)
                    # Advance before the non-idempotent reply. Control commands
                    # already retained their durable identity in handle_update.
                    if reply and not self.stop.is_set():
                        self.send_reply(reply)
                if not self.stop.is_set():
                    self.notify_once()
            except Exception:
                if time.monotonic() - self._last_failure_log > 60:
                    self.app.log("WARN", "Telegram polling unavailable; no raw provider data logged")
                    self._last_failure_log = time.monotonic()
                self.stop.wait(5)

    def close(self):
        self.stop.set()
        if self._thread:
            self._thread.join(timeout=8)
