from dataclasses import replace
import io
import json
from pathlib import Path
import struct
import tempfile
import time
import unittest
import zlib
from unittest.mock import patch

from mm2_console.app import App
from mm2_console.config import Config
from mm2_console.engine import Reply
from mm2_console.store import Store
from mm2_console.telegram import TelegramBot, TelegramTransport
from mm2_console.telegram_reports import (active_text, deposit_chart, deposit_value,
                                         holdings, notification_delta, stats_text)


def fixture():
    now = int(time.time() * 1000)
    return {"command_version": 7, "published_at_ms": now, "mode": "paused", "execution_mode": "both",
        "feed_ready": True, "reset_epoch": 0, "simulation_market_condition": "market-A",
        "market": {"condition_id": "market-A"}, "limits": {"max_feed_age_ms": 2000},
        "books": {outcome: {"initialized": True, "exchange_age_ms": 10, "receive_age_ms": 10,
                            "best_bid": price} for outcome, price in (("up", "0.48"), ("down", "0.52"))},
        "live": {"simulated": False, "account_reconciled": True, "collateral_micros": 90_000_000,
                 "last_reconciliation_ms": now,
                 "available_micros": 85_000_000, "reserved_pico": "5000000000000", "session_spent_pico": "10000000000000",
                 "returned_collateral_micros": 0, "orders": [{"order_id": "live-A", "phase": "live",
                    "quote": {"outcome": "up", "quantity_micros": 10_000_000, "price_micros": 490_000}, "cumulative_filled_micros": 2_000_000, "last_operation_ms": now - 1}],
                 "positions": [{"condition_id": "market-A", "outcome": "up", "quantity_micros": 10_000_000}]},
        "simulation": {"simulated": True, "audit_sequence": 10, "recent_audit": [{"sequence": 10, "kind": "venue_fill", "order_id": 1, "quantity_micros": 1_000_000}],
            "stats": {"fill_updates": 1, "submitted_posts": 2, "confirmed_cancels": 1},
            "venue_stats": {"simulated_filled_micros": 1_000_000},
            "account": {"initial_cash_micros": 100_000_000, "spent_pico": "2000000000000", "reserved_pico": "0", "available_pico": "98000000000000"},
            "orders": [{"id": 1, "phase": "unknown_cancel", "quote": {"outcome": "down", "quantity_micros": 3_000_000, "price_micros": 500_000}, "cumulative_filled_micros": 1_000_000}]},
        "paper_portfolio": {"cash_pico": "98000000000000", "reserved_pico": "0", "available_pico": "98000000000000",
            "position_count": 1, "positions_truncated": 0, "positions": [{"market": {"id": "market-A"},
                "up": {"quantity_micros": 0}, "down": {"quantity_micros": 4_000_000}}]}}


def envelope(state, connected=True):
    return {"state": state, "connected": connected, "observed_at_ms": state["published_at_ms"]}


class ReportTests(unittest.TestCase):
    def test_exact_best_bid_marks_separate_real_paper_and_never_add_reserve_twice(self):
        state = fixture()
        self.assertEqual(deposit_value(state, "real", state["published_at_ms"]), 94_800_000_000_000)
        self.assertEqual(deposit_value(state, "paper", state["published_at_ms"]), 100_080_000_000_000)
        text = stats_text(envelope(state))
        self.assertIn("94.8", text)
        self.assertIn("100.08", text)
        self.assertIn("Экономическая прибыль/PnL: неизвестно", text)
        self.assertIn("не число всех сделок", text)

    def test_unknown_prices_stale_markets_and_truncation_never_fabricate_nav(self):
        for mutation in (
            lambda state: state["live"]["positions"][0].update(condition_id="old-market"),
            lambda state: state["books"]["up"].update(exchange_age_ms=2001),
            lambda state: state["books"]["up"].update(best_bid=None),
            lambda state: state["books"]["up"].update(best_bid="0.1234567"),
            lambda state: state.update(feed_ready=False),
            lambda state: state["live"].update(account_reconciled=False),
            lambda state: state["live"].update(simulated=True),
            lambda state: state["live"].update(fault="receipt uncertainty"),
            lambda state: state["live"].update(foreign_order_ids=["external-order"]),
            lambda state: state["live"].update(last_reconciliation_ms=state["published_at_ms"]-5001),
            lambda state: state["live"]["orders"][0].update(last_operation_ms=state["published_at_ms"]+1),
            lambda state: state["live"]["positions"][0].update(quantity_micros=True),
        ):
            state = fixture(); mutation(state)
            self.assertIsNone(deposit_value(state, "real", state["published_at_ms"]))
        state = fixture()
        self.assertIsNone(deposit_value(state, "real", state["published_at_ms"] + 3001))
        state["paper_portfolio"]["positions_truncated"] = 2
        state["paper_portfolio"]["position_count"] = 3
        self.assertIsNone(deposit_value(state, "paper", state["published_at_ms"]))
        self.assertEqual(len(holdings(state, "paper")[0]), 1)
        self.assertFalse(holdings(state, "paper")[1])

    def test_no_positions_known_cash_is_nav_without_fabricating_prices(self):
        state = fixture()
        state["live"]["positions"] = []
        state["feed_ready"] = False
        state["books"] = {}
        self.assertEqual(deposit_value(state, "real", state["published_at_ms"]), 90_000_000_000_000)
        self.assertIsNone(deposit_value({**state, "live": {**state["live"], "positions": None}}, "real", state["published_at_ms"]))

    def test_active_distinguishes_positions_orders_unknown_cancel_and_cached_state(self):
        state = fixture()
        text = active_text(envelope(state))
        self.assertIn("Позиция market-A: UP 10 акций", text)
        self.assertIn("Позиция market-A: DOWN 4 акций", text)
        self.assertIn("осталось 8", text)
        self.assertIn("unknown_cancel", text)
        state["paper_portfolio"]["positions_truncated"] = 1
        state["paper_portfolio"]["position_count"] = 2
        text = active_text(envelope(state, False))
        self.assertIn("УСТАРЕВШИЙ СНИМОК", text)
        self.assertIn("усечён", text)
        self.assertIn("DOWN 4 акций", text)
        self.assertLessEqual(len(text), 3500)

    def test_png_is_bounded_valid_rgb_and_unknown_history_is_rendered(self):
        state = fixture()
        for history in ([], [{"observed_at_ms": state["published_at_ms"], "state": state}],
                        [{"observed_at_ms": state["published_at_ms"], "state": {}}]):
            image = deposit_chart(history)
            self.assertTrue(image.startswith(b"\x89PNG\r\n\x1a\n"))
            position, compressed = 8, []
            while position < len(image):
                size = struct.unpack("!I", image[position:position+4])[0]
                kind = image[position+4:position+8]
                data = image[position+8:position+8+size]
                crc = struct.unpack("!I", image[position+8+size:position+12+size])[0]
                self.assertEqual(zlib.crc32(kind+data)&0xffffffff, crc)
                if kind == b"IHDR":
                    self.assertEqual(struct.unpack("!IIBBBBB", data), (720,360,8,2,0,0,0))
                if kind == b"IDAT": compressed.append(data)
                position += size+12
            self.assertEqual(len(zlib.decompress(b"".join(compressed))), (720*3+1)*360)
            self.assertLess(len(image), 50_000)

    def test_chart_does_not_connect_unknown_points_mode_resets_or_missing_minutes(self):
        stamp = 9_000_000
        def column_has_line(history):
            image = deposit_chart(history)
            position, compressed = 8, []
            while position < len(image):
                size = struct.unpack("!I", image[position:position+4])[0]
                if image[position+4:position+8] == b"IDAT":
                    compressed.append(image[position+8:position+8+size])
                position += size+12
            raw = zlib.decompress(b"".join(compressed))
            x = 92+(695-92)//2
            return any(raw[y*(720*3+1)+1+x*3:y*(720*3+1)+1+x*3+3] == bytes((55,210,193)) for y in range(76,299))
        first = {"observed_at_ms": stamp, "real_pico": "100000000000000", "paper_pico": None, "context": "trading/0"}
        last = {"observed_at_ms": stamp+60_000, "real_pico": "110000000000000", "paper_pico": None, "context": "trading/0"}
        self.assertTrue(column_has_line([first,last]))
        self.assertFalse(column_has_line([first,{**last,"context":"trading/1"}]))
        self.assertFalse(column_has_line([first,{**last,"context":"both/0"}]))
        self.assertFalse(column_has_line([first,{**last,"observed_at_ms":stamp+120_000}]))
        gap = {"observed_at_ms":stamp+60_000,"real_pico":None,"paper_pico":None,"context":"trading/0"}
        self.assertFalse(column_has_line([first,gap,{**last,"observed_at_ms":stamp+120_000}]))

    def test_aggregate_proves_truncated_flat_markets_and_all_shown_holdings(self):
        state = fixture()
        portfolio = state["paper_portfolio"]
        portfolio.update(position_count=70,positions_truncated=6,held_market_count=0)
        portfolio["positions"] = [{"market":{"id":f"flat-{index}"},"up":{"quantity_micros":0},"down":{"quantity_micros":0}} for index in range(64)]
        state["feed_ready"] = False
        self.assertEqual(holdings(state,"paper"), ([],True))
        self.assertEqual(deposit_value(state,"paper",state["published_at_ms"]), 98_000_000_000_000)
        portfolio.pop("held_market_count")
        self.assertFalse(holdings(state,"paper")[1])
        self.assertIsNone(deposit_value(state,"paper",state["published_at_ms"]))
        portfolio["held_market_count"] = 1
        self.assertFalse(holdings(state,"paper")[1])  # A holding exists among the omitted markets.
        portfolio["positions"][0] = {"market":{"id":"market-A"},"up":{"quantity_micros":1_000_000},"down":{"quantity_micros":2_000_000}}
        state["feed_ready"] = True
        self.assertTrue(holdings(state,"paper")[1])  # Two outcomes in one held market.
        self.assertEqual(deposit_value(state,"paper",state["published_at_ms"]), 99_520_000_000_000)

    def test_aggregate_malformed_counts_duplicate_market_ids_and_bounds_are_rejected(self):
        for held in (True,-1,2,4097,"1",1.0):
            state = fixture()
            state["paper_portfolio"]["held_market_count"] = held
            self.assertFalse(holdings(state,"paper")[1])
            self.assertIsNone(deposit_value(state,"paper",state["published_at_ms"]))
        for field,value in (("position_count",True),("position_count",4097),("positions_truncated",-1),("positions_truncated",1)):
            state = fixture()
            state["paper_portfolio"].update(held_market_count=1)
            state["paper_portfolio"][field] = value
            self.assertFalse(holdings(state,"paper")[1])
        state = fixture()
        portfolio = state["paper_portfolio"]
        portfolio["positions"] *= 2
        portfolio.update(position_count=2,held_market_count=2)
        self.assertEqual(holdings(state,"paper"), ([],False))

    def test_notification_baseline_deltas_and_restart_have_no_historic_flood(self):
        state = fixture()
        cursor, messages, warnings = notification_delta(state, None)
        self.assertEqual(messages, [])
        state["simulation"]["audit_sequence"] = 11
        state["simulation"]["recent_audit"].append({"sequence": 11, "kind": "venue_fill", "order_id": 2, "quantity_micros": 500_000})
        state["live"]["orders"][0]["cumulative_filled_micros"] += 250_000
        next_cursor, messages, _ = notification_delta(state, cursor)
        self.assertEqual(len(messages), 2)
        self.assertIn("0.5", messages[0])
        self.assertIn("0.25", messages[1])
        self.assertEqual(notification_delta(state, json.loads(json.dumps(next_cursor)))[1], [])

    def test_notification_rollover_audit_gaps_and_counter_regression_are_explicit(self):
        state = fixture()
        cursor = notification_delta(state, None)[0]
        state["simulation_market_condition"] = "market-B"
        state["simulation"]["audit_sequence"] = 3
        state["simulation"]["recent_audit"] = [{"sequence": 3, "kind": "venue_fill", "order_id": 1, "quantity_micros": 1_000_000}]
        _, messages, warnings = notification_delta(state, cursor)
        self.assertIn("market-B", messages[0])
        self.assertTrue(any("gap" in warning for warning in warnings))
        state = fixture()
        state["live"]["orders"][0]["cumulative_filled_micros"] = 1
        regressed, messages, warnings = notification_delta(state, cursor)
        self.assertEqual(messages, [])
        self.assertTrue(any("regressed" in warning for warning in warnings))
        self.assertEqual(regressed["live"]["orders"]["live-A"], 2_000_000)
        state["live"]["orders"][0]["cumulative_filled_micros"] = 2_000_000
        self.assertEqual(notification_delta(state, regressed)[1], [])
        state = fixture()
        state["simulation"]["audit_sequence"] = 5
        state["simulation"]["recent_audit"] = [{"sequence":5,"kind":"venue_fill","order_id":1,"quantity_micros":1_000_000}]
        regressed, messages, warnings = notification_delta(state, cursor)
        self.assertEqual(messages, [])
        self.assertEqual(regressed["paper"]["sequence"], 10)
        state["simulation"]["audit_sequence"] = 10
        state["simulation"]["recent_audit"][0]["sequence"] = 10
        self.assertEqual(notification_delta(state, regressed)[1], [])

    def test_unreconciled_live_counters_are_not_announced_or_forgotten(self):
        state = fixture()
        cursor = notification_delta(state, None)[0]
        state["live"]["account_reconciled"] = False
        state["live"]["orders"][0]["cumulative_filled_micros"] += 100
        next_cursor, messages, _ = notification_delta(state, cursor)
        self.assertEqual(messages, [])
        self.assertEqual(next_cursor["live"], cursor["live"])


class BotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.config = Config("http://engine:8088", "engine-"+"e"*32, "owner-"+"o"*32,
                             root, root, frozenset({"https://console.example"}),
                             telegram_token="bot-"+"b"*40, telegram_owner=42, miniapp_url="https://console.example/")
        self.current = fixture()
        owner = self
        class Engine:
            def __init__(self): self.calls = []
            def request(self, method, path, payload=None):
                self.calls.append((method,path,payload))
                return Reply(200, owner.current)
        self.engine = Engine()
        self.app = App(self.config, self.engine, Store(root/"secondary.sqlite3"))
        self.app.sample_once()
        self.calls = []
        class Transport:
            def call(self, method, payload):
                owner.calls.append((method,payload))
                return {"message_id": 1}
        self.bot = TelegramBot(self.app, Transport())
        self.clock = [100.0]
        self.clock_patch = patch("mm2_console.telegram.time.monotonic", side_effect=lambda: self.clock[0])
        self.clock_patch.start()

    def tearDown(self):
        self.bot.close(); self.app.close(); self.temp.cleanup()
        self.clock_patch.stop()

    def update(self, command, owner=42, chat=42, private=True):
        return {"update_id": 10, "message": {"from": {"id": owner},
            "chat": {"id": chat, "type": "private" if private else "group"}, "text": command}}

    def test_reports_use_owner_gate_and_cached_sample_only(self):
        for command in ("/stats", "/active"):
            for update in (self.update(command, owner=43), self.update(command, chat=43), self.update(command, private=False)):
                self.assertIsNone(self.bot.handle_update(update))
            reply = self.bot.handle_update(self.update(command))
            self.assertEqual(reply["chat_id"], 42)
        stats = self.bot.handle_update(self.update("/stats"))
        self.assertTrue(stats["_photo"].startswith(b"\x89PNG"))
        self.bot.send_reply(stats)
        self.assertEqual([method for method, _ in self.calls], ["sendMessage", "sendPhoto"])
        self.assertNotIn("_photo", self.calls[0][1])
        self.assertEqual(len(self.engine.calls), 1)

    def test_menu_and_commands_are_scoped_to_exact_owner(self):
        self.bot.configure_owner_menu()
        self.assertEqual(self.calls[0][0], "setChatMenuButton")
        self.assertEqual(self.calls[0][1]["chat_id"], 42)
        self.assertEqual(self.calls[1][1]["scope"], {"type": "chat", "chat_id": 42})
        names = {command["command"] for command in self.calls[1][1]["commands"]}
        self.assertIn("stats", names); self.assertIn("active", names)

    def test_notification_cursor_persists_before_unknown_send_and_restart_never_retries(self):
        self.bot.notify_once()
        self.assertEqual(self.calls, [])
        self.current["live"]["orders"][0]["cumulative_filled_micros"] += 100_000
        self.app.sample_once()
        owner = self
        class UnknownTransport:
            def call(self, method, payload):
                cursor = json.loads(owner.app.store.get_meta("telegram_notification_cursor"))
                owner.assertEqual(cursor["live"]["orders"]["live-A"], 2_100_000)
                owner.calls.append((method,payload))
                raise RuntimeError("provider URL must not be logged")
        self.bot.transport = UnknownTransport()
        self.clock[0] += 5
        self.bot.notify_once()
        self.assertEqual(len(self.calls), 1)
        restarted = TelegramBot(self.app, UnknownTransport())
        restarted.notify_once()
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(any("delivery unknown" in row["message"] for row in self.app.store.logs(10)))
        self.assertEqual(len(self.engine.calls), 2)

    def test_failed_cursor_write_prevents_external_notification(self):
        self.bot.notify_once()
        self.current["live"]["orders"][0]["cumulative_filled_micros"] += 100_000
        self.app.sample_once()
        def failed(*args): raise OSError("private detail")
        self.app.store.set_meta = failed
        self.clock[0] += 5
        with self.assertRaises(OSError): self.bot.notify_once()
        self.assertEqual(self.calls, [])

    def test_corrupt_cursor_is_fail_closed_and_disabled_bot_has_no_calls(self):
        self.app.store.set_meta("telegram_notification_cursor", '{"version":2}')
        with self.assertRaises(ValueError): self.bot.notify_once()
        self.app.config = replace(self.config, telegram_token="", telegram_owner=None)
        disabled = TelegramBot(self.app, self.bot.transport)
        disabled.notify_once(); disabled.configure_owner_menu()
        self.assertEqual(self.calls, [])

    def test_read_reply_offset_is_committed_before_unknown_delivery(self):
        owner = self
        class UnknownTransport:
            def call(self, method, payload):
                if method == "getUpdates": return [owner.update("/stats")]
                if method in {"setChatMenuButton", "setMyCommands"}: return True
                owner.assertEqual(owner.app.store.get_meta("telegram_offset"), "11")
                owner.bot.stop.set()
                raise RuntimeError("delivery unknown")
        self.bot.transport = UnknownTransport()
        self.bot._loop()
        self.assertEqual(self.app.store.get_meta("telegram_offset"), "11")

    def test_immediate_stranger_updates_cannot_increase_notification_cadence(self):
        owner = self
        samples = []
        original_meta = self.app.store.get_meta
        def observed_meta(key, default=""):
            if key == "telegram_notification_cursor": samples.append(True)
            return original_meta(key, default)
        self.app.store.get_meta = observed_meta
        class BusyTransport:
            polls = 0
            def call(self, method, payload):
                if method in {"setChatMenuButton", "setMyCommands"}: return True
                owner.assertEqual(method, "getUpdates")
                self.polls += 1
                if self.polls == 3: owner.bot.stop.set()
                return [owner.update("/stats", owner=43)]
        self.bot.transport = BusyTransport()
        with patch("mm2_console.telegram.time.monotonic", return_value=100):
            self.bot._loop()
        self.assertEqual(len(samples), 1)
        self.assertEqual(len(self.engine.calls), 1)

    def test_method_cooldown_skips_snapshot_and_cursor_work_without_losing_deltas(self):
        self.bot.notify_once()
        baseline = self.app.store.get_meta("telegram_notification_cursor")
        self.current["live"]["orders"][0]["cumulative_filled_micros"] += 100_000
        self.app.sample_once()
        original_state = self.app.state
        def forbidden_state(): raise AssertionError("suppressed cycle must not copy state")
        self.app.state = forbidden_state
        self.clock[0] = 104.999
        self.bot.notify_once()
        self.assertEqual(self.app.store.get_meta("telegram_notification_cursor"), baseline)
        self.assertEqual(self.calls, [])
        self.app.state = original_state
        self.current["live"]["orders"][0]["cumulative_filled_micros"] += 50_000
        self.app.sample_once()
        self.clock[0] = 105
        self.bot.notify_once()
        self.assertEqual(len(self.calls), 1)
        self.assertIn("+0.15 акций", self.calls[0][1]["text"])
        self.assertEqual(json.loads(self.app.store.get_meta("telegram_notification_cursor"))["live"]["orders"]["live-A"], 2_150_000)

    def test_sampler_records_once_per_minute_from_its_existing_request_and_offline_becomes_gap(self):
        start = 9_000_000
        self.app._last_equity_minute = None
        for offset in (0, 1000, 2000, 60_000):
            self.current["published_at_ms"] = start+offset
            self.current["live"]["last_reconciliation_ms"] = start+offset
            self.current["live"]["orders"][0]["last_operation_ms"] = start+offset-1
            with patch("mm2_console.app.time.time", return_value=(start+offset)/1000):
                self.app.sample_once()
        points = self.app.store.equity_history(now_ms=start+61_000)
        self.assertEqual([row["observed_at_ms"] for row in points], [start,start+60_000])
        self.assertEqual(points[0]["real_pico"], "94800000000000")
        self.assertEqual(points[0]["paper_pico"], "100080000000000")
        self.assertEqual(len(self.engine.calls), 5)  # one setup sample + exactly four existing sampler requests
        self.engine.request = lambda *args: Reply(503,{})
        with patch("mm2_console.app.time.time", return_value=(start+120_000)/1000):
            self.app.sample_once()
        gap = self.app.store.equity_history(now_ms=start+120_000)[-1]
        self.assertIsNone(gap["real_pico"]); self.assertIsNone(gap["paper_pico"])

    def test_minute_persistence_failure_is_secondary_and_not_retried_at_one_hz(self):
        start = 9_000_000
        self.app._last_equity_minute = None
        attempts = []
        self.app.store.equity_sample = lambda *args: (attempts.append(args) or False)
        before = self.app.state()["telemetry"]["history_dropped"]
        for offset in (0, 1000):
            self.current["published_at_ms"] = start+offset
            with patch("mm2_console.app.time.time", return_value=(start+offset)/1000):
                self.app.sample_once()
        self.assertEqual(len(attempts), 1)
        self.assertEqual(self.app.state()["telemetry"]["history_dropped"], before+1)
        self.assertIn("minute valuation", self.app.state()["telemetry"]["storage_error"])
        def unavailable(*args): raise OSError("do not expose private database location")
        self.app.store.equity_sample = unavailable
        self.current["published_at_ms"] = start+60_000
        with patch("mm2_console.app.time.time", return_value=(start+60_000)/1000):
            self.app.sample_once()
            self.assertTrue(self.app.state()["connected"])
        self.assertNotIn("private database location", self.app.state()["telemetry"]["storage_error"])


class EquityStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)/"secondary.sqlite3"
        self.store = Store(self.path)

    def tearDown(self):
        self.store.close(); self.temp.cleanup()

    def test_upsert_is_one_point_per_minute_restart_keeps_exact_strings_and_no_clock_rewind(self):
        stamp = 9_000_000
        self.assertTrue(self.store.equity_sample(stamp, 10**26+1, None, "trading/0"))
        self.assertTrue(self.store.equity_sample(stamp+1000, 10**26+2, 3, "both/0"))
        self.assertTrue(self.store.equity_sample(stamp+500, 0, 0, "wrong-old-context"))
        self.store.close(); self.store = Store(self.path)
        points = self.store.equity_history(now_ms=stamp+2000)
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0], {"observed_at_ms":stamp+1000,"real_pico":str(10**26+2),"paper_pico":"3","context":"both/0"})
        self.assertEqual(self.store.db.execute("PRAGMA max_page_count").fetchone()[0], 16384)
        self.assertFalse(self.store.equity_sample(stamp, True, None, "a"))
        self.assertFalse(self.store.equity_sample(stamp, 1, None, "a"*129))

    def test_equity_retention_and_cap_do_not_delete_raw_history_or_logs(self):
        self.store.sample(0,{"raw":"kept"}); self.store.log(0,"INFO","kept")
        with self.store.db:
            self.store.db.executemany("INSERT INTO equity_minutes VALUES (?,?,?,?,?)", [(minute, minute*60_000,"1",None,"simulation/0") for minute in range(10_081)])
        self.assertTrue(self.store.equity_sample(10_080*60_000, 2, None, "simulation/0"))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM equity_minutes").fetchone()[0], 10_080)
        self.assertEqual(self.store.db.execute("SELECT min(minute) FROM equity_minutes").fetchone()[0], 1)
        future = 10_080*60_000+7*86_400_000+1
        self.assertTrue(self.store.equity_sample(future, None, None, "simulation/0"))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM equity_minutes").fetchone()[0], 1)
        self.assertEqual(self.store.history(1)[0]["state"], {"raw":"kept"})
        self.assertEqual(self.store.logs(1)[0]["message"], "kept")

    def test_history_is_past_24h_bounded_and_never_extends_a_known_mark(self):
        now = 10_000*60_000
        with self.store.db:
            self.store.db.executemany("INSERT INTO equity_minutes VALUES (?,?,?,?,?)", [(minute, minute*60_000,"1",None,"simulation/0") for minute in range(8000,10_001)])
        self.assertTrue(self.store.equity_sample(now+60_000, None, None, "simulation/0"))
        history = self.store.equity_history(99999, now_ms=now)
        self.assertEqual(len(history), 1440)
        self.assertEqual(history[-1]["observed_at_ms"], now)
        self.assertGreaterEqual(history[0]["observed_at_ms"], now-86_400_000)
        self.assertEqual(self.store.equity_history(now_ms=now+60_000)[-1]["real_pico"], None)
        self.assertEqual(self.store.equity_history(now_ms=now+2*86_400_000), [])


class TransportTests(unittest.TestCase):
    def test_sendphoto_is_multipart_and_transport_errors_hide_provider_token(self):
        token = "never-expose-bot-token"
        transport = TelegramTransport(token)
        captured = []
        class Opener:
            def open(self, request, timeout):
                captured.append(request)
                return io.BytesIO(b'{"ok":true,"result":{"message_id":1}}')
        transport._opener = Opener()
        photo = deposit_chart([])
        self.assertEqual(transport.call("sendPhoto", {"chat_id": 42, "photo": photo, "caption": "NAV"})["message_id"], 1)
        request = captured[0]
        self.assertTrue(request.headers["Content-type"].startswith("multipart/form-data; boundary="))
        self.assertIn(b'filename="deposit.png"', request.data)
        self.assertIn(photo, request.data)
        with self.assertRaises(ValueError): transport.call("sendPhoto", {"photo": b"unknown"})
        class Failed:
            def open(self, request, timeout): raise RuntimeError(request.full_url)
        transport._opener = Failed()
        with self.assertRaises(RuntimeError) as raised: transport.call("sendMessage", {"chat_id":42,"text":"test"})
        self.assertNotIn(token, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
