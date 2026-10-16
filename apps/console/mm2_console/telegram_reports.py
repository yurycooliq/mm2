"""Bounded, read-only Telegram reports from secondary engine samples.

Best-bid marks are estimates without depth, fees or settlement guarantees. Missing
prices, old markets or truncated inventory deliberately produce gaps in the chart.
"""
from datetime import datetime, timezone
import json
import re
import struct
import zlib

from .observability import execution_summary, money, _units

PICO = 1_000_000_000_000
MAX_ROWS = 24


def obj(value):
    return value if isinstance(value, dict) else {}


def rows(value, limit=1024):
    return value[:limit] if isinstance(value, list) else []


def label(value):
    # Telegram messages are plain text; retain no control characters or giant IDs.
    return "".join(c for c in str(value)[:80] if c.isprintable())


def fresh(state, observed_at_ms):
    publication = state.get("published_at_ms")
    return (type(observed_at_ms) is int and type(publication) is int
            and 0 <= publication <= observed_at_ms + 5000
            and observed_at_ms - publication <= 3000)


def price_micros(value):
    if not isinstance(value, str) or not re.fullmatch(r"(?:0(?:\.[0-9]{1,6})?|1(?:\.0{1,6})?)", value):
        return None
    whole, _, fraction = value.partition(".")
    return int(whole) * 1_000_000 + int(fraction.ljust(6, "0") or "0")


def valuation_boot(state):
    boot = obj(state).get("boot_id")
    return boot if isinstance(boot, str) and re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", boot) else None


def valuation_context(state):
    state = obj(state)
    profile = state.get("execution_mode")
    if profile not in {"emulator", "simulation", "shadow", "trading", "both"}:
        profile = "unknown"
    return json.dumps({"profile": profile, "reset_epoch": _units(state.get("reset_epoch")),
                       "boot_id": valuation_boot(state)}, separators=(",", ":"))


def bid(state, outcome):
    book = obj(obj(state.get("books")).get(outcome))
    age_limit = _units(obj(state.get("limits")).get("max_feed_age_ms"))
    if age_limit is None or not 1 <= age_limit <= 60_000:
        return None
    if book.get("initialized") is not True:
        return None
    for field in ("exchange_age_ms", "receive_age_ms"):
        age = _units(book.get(field))
        if age is None or age > age_limit:
            return None
    return price_micros(book.get("best_bid"))


def holdings(state, lane):
    """(positions, complete) with market identity and exact share quantities."""
    if lane == "real":
        live = obj(state.get("live"))
        raw = live.get("positions")
        if not isinstance(raw, list) or len(raw) > 1024:
            return [], False
        result = []
        for position in raw:
            position = obj(position)
            quantity = _units(position.get("quantity_micros"))
            outcome = position.get("outcome")
            condition = position.get("condition_id")
            if quantity is None or outcome not in {"up", "down"} or not isinstance(condition, str):
                return [], False
            if quantity:
                result.append({"market": condition, "outcome": outcome, "quantity": quantity})
        return result, True
    portfolio = obj(state.get("paper_portfolio"))
    raw = portfolio.get("positions")
    if isinstance(raw, list):
        if len(raw) > 1024:
            return [], False
        count = portfolio.get("position_count", len(raw))
        truncated = portfolio.get("positions_truncated", 0)
        if (type(count) is not int or type(truncated) is not int
                or not len(raw) <= count <= 4096 or not 0 <= truncated <= 4096
                or count - len(raw) != truncated):
            return [], False
        result, seen, shown_held = [], set(), 0
        for position in raw:
            position = obj(position)
            condition = obj(position.get("market")).get("id")
            if not isinstance(condition, str) or not 0 < len(condition) <= 128 or condition in seen:
                return [], False
            seen.add(condition)
            has_inventory = False
            for outcome in ("up", "down"):
                quantity = _units(obj(position.get(outcome)).get("quantity_micros"))
                if quantity is None:
                    return [], False
                if quantity:
                    has_inventory = True
                    result.append({"market": condition, "outcome": outcome, "quantity": quantity})
            shown_held += int(has_inventory)
        held = portfolio.get("held_market_count")
        if held is None:
            complete = truncated == 0
        elif type(held) is not int or not 0 <= held <= count:
            return [], False
        else:
            # The producer counted all retained markets. If every market with
            # inventory is shown, omitted flat market ledgers need no price mark.
            complete = held == shown_held
        return result, complete
    simulation = obj(state.get("simulation"))
    account = obj(simulation.get("account"))
    condition = state.get("simulation_market_condition")
    result = []
    for outcome in ("up", "down"):
        quantity = _units(account.get(outcome + "_inventory_micros"))
        if quantity is None:
            return [], False
        if quantity:
            if not isinstance(condition, str):
                return [], False
            result.append({"market": condition, "outcome": outcome, "quantity": quantity})
    return result, True


def deposit_value(state, lane, observed_at_ms):
    """Exact pico-USD best-bid estimate or None; no cost-as-value substitution."""
    state = obj(state)
    if not fresh(state, observed_at_ms):
        return None
    summary = execution_summary(state, True)
    selected = summary[lane if lane == "real" else "paper"]
    if not selected["selected"] or (lane == "real" and not selected["balances_known"]):
        return None
    if lane == "real":
        live = obj(state.get("live"))
        if live.get("fault") or live.get("foreign_order_ids"):
            return None
        reconciled_at = _units(live.get("last_reconciliation_ms"))
        if reconciled_at is None or not 0 <= observed_at_ms - reconciled_at <= 5000:
            return None
        # Position fill counters may lead the venue's last cash reconciliation.
        # Combining those floors with older cash would overstate account value.
        for order in rows(live.get("orders")):
            order = obj(order)
            if (_units(order.get("cumulative_filled_micros")) or 0) > 0:
                operation_at = _units(order.get("last_operation_ms"))
                if operation_at is None or operation_at > reconciled_at:
                    return None
        cash = _units(live.get("collateral_micros"))
        cash = cash * 1_000_000 if cash is not None else None
    else:
        cash = _units(obj(state.get("paper_portfolio")).get("cash_pico"))
        if cash is None:
            account = obj(obj(state.get("simulation")).get("account"))
            initial, spent = _units(account.get("initial_cash_micros")), _units(account.get("spent_pico"))
            if initial is not None and spent is not None and spent <= initial * 1_000_000:
                cash = initial * 1_000_000 - spent
    positions, complete = holdings(state, lane)
    if cash is None or not complete:
        return None
    condition = obj(state.get("market")).get("condition_id")
    for position in positions:
        if position["market"] != condition or state.get("feed_ready") is not True:
            return None
        mark = bid(state, position["outcome"])
        if mark is None:
            return None
        cash += position["quantity"] * mark
    return cash


def stats_text(envelope):
    state = obj(envelope.get("state"))
    if not state:
        return "Статистика пока недоступна: состояние движка не получено."
    connected = envelope.get("connected") is True
    summary = execution_summary(state, connected)
    lines = [f"mm2 /stats · {label(summary['profile'])} · {label(state.get('mode', 'unknown'))}"]
    if not connected:
        lines.append("УСТАРЕВШИЙ СНИМОК: текущие деньги и позиции не подтверждены.")
    for lane, title in (("real", "РЕАЛЬНАЯ ТОРГОВЛЯ"), ("paper", "ВИРТУАЛЬНАЯ СИМУЛЯЦИЯ")):
        selected = summary[lane]
        if not selected["selected"]:
            continue
        lines.append(title)
        cash = selected["collateral_usd"] if lane == "real" else selected["cash_usd"]
        lines.append(f"Деньги USD: {cash if connected and cash is not None else 'неизвестно'} · резерв: {selected['reserved_usd'] if connected else 'неизвестно'}")
        value = deposit_value(state, lane, envelope.get("observed_at_ms")) if connected else None
        lines.append("Стоимость депозита USD: " + (money(value, PICO) if value is not None else "неизвестно — нет полной свежей оценки"))
        if lane == "real":
            live = obj(state.get("live"))
            filled = [row for row in rows(live.get("orders")) if (_units(obj(row).get("cumulative_filled_micros")) or 0) > 0]
            lines.append(f"Ордера с исполнением в доступном снимке: {len(filled)} (не число всех сделок)")
            lines.append(f"Консервативная оценка расходов по лимитам USD: {money(live.get('session_spent_pico'), PICO) or 'неизвестно'}")
            lines.append(f"Подтверждённые возвраты USD: {selected['returned_collateral_usd'] or 'неизвестно'}")
            lines.append("Экономическая прибыль/PnL: неизвестно.")
        else:
            simulation = obj(state.get("simulation"))
            stats, venue = obj(simulation.get("stats")), obj(simulation.get("venue_stats"))
            lines.append(f"Обновления исполнений: {_units(stats.get('fill_updates')) if _units(stats.get('fill_updates')) is not None else 'неизвестно'} · исполнено акций: {money(venue.get('simulated_filled_micros')) or 'неизвестно'}")
            lines.append(f"Отправлено ордеров: {_units(stats.get('submitted_posts')) if _units(stats.get('submitted_posts')) is not None else 'неизвестно'} · отмен подтверждено: {_units(stats.get('confirmed_cancels')) if _units(stats.get('confirmed_cancels')) is not None else 'неизвестно'}")
            lines.append("Исполнения и расчёты синтетические; это не реальная доходность.")
    lines.append("График за последние 24 ч: 1 вторичный снимок/мин; оценка по лучшим bid без глубины и комиссий. Пробелы означают неизвестную стоимость. История начинается с включения сбора.")
    return "\n".join(lines)[:3500]


def active_text(envelope):
    state = obj(envelope.get("state"))
    if not state:
        return "Позиции пока недоступны: состояние движка не получено."
    summary = execution_summary(state, envelope.get("connected") is True)
    lines = ["mm2 /active · позиции и рабочие ордера"]
    if not envelope.get("connected"):
        lines.append("УСТАРЕВШИЙ СНИМОК: текущие позиции/ордера не подтверждены.")
    for lane, title in (("real", "РЕАЛЬНАЯ ТОРГОВЛЯ"), ("paper", "ВИРТУАЛЬНАЯ СИМУЛЯЦИЯ")):
        if not summary[lane]["selected"]:
            continue
        lines.append(title)
        if lane == "real" and not summary[lane]["balances_known"]:
            lines.append("Счёт не сверён; полнота позиций и ордеров неизвестна.")
        positions, complete = holdings(state, lane)
        if not complete:
            lines.append("Полный список позиций недоступен/усечён.")
        elif not positions:
            lines.append("Заполненных позиций нет в снимке.")
        for row in positions[:MAX_ROWS]:
            lines.append(f"Позиция {label(row['market'])}: {row['outcome'].upper()} {money(row['quantity'])} акций")
        if len(positions) > MAX_ROWS:
            lines.append(f"Ещё {len(positions) - MAX_ROWS} позиций; полный снимок в Mini App.")
        source = obj(state.get("live" if lane == "real" else "simulation"))
        raw_orders = source.get("orders")
        orders_known = isinstance(raw_orders, list) and len(raw_orders) <= 1024
        orders = [obj(row) for row in rows(raw_orders) if obj(row).get("phase") != "closed"]
        lines.append(f"Рабочие/неопределённые ордера: {len(orders) if orders_known else 'неизвестно'}")
        for row in orders[:12]:
            quote = obj(row.get("quote"))
            quantity, filled = _units(quote.get("quantity_micros")), _units(row.get("cumulative_filled_micros"))
            remaining = money(quantity - filled) if quantity is not None and filled is not None and quantity >= filled else "неизвестно"
            lines.append(f"BUY {label(quote.get('outcome', '?')).upper()} · осталось {remaining} · цена {money(quote.get('price_micros')) or '?'} · {label(row.get('phase', '?'))} · {label(row.get('order_id', row.get('id', '?')))}")
        if len(orders) > 12:
            lines.append(f"Ещё {len(orders) - 12} ордеров в Mini App.")
    return "\n".join(lines)[:3500]


def notification_delta(state, previous):
    """Bounded sampled deltas, stable across console/engine restarts.

    First observation becomes a baseline, never a historic notification flood.
    Caller persists the next cursor before dispatch (Telegram has no send idempotency).
    """
    state, previous = obj(state), obj(previous)
    next_cursor = {"version": 1, "paper": None, "live": None}
    messages, warnings = [], []
    summary = execution_summary(state, True)
    if summary["paper"]["selected"]:
        simulation = obj(state.get("simulation"))
        sequence = _units(simulation.get("audit_sequence"))
        if sequence is not None:
            identity = [state.get("simulation_market_condition"), state.get("reset_epoch", 0)]
            old = obj(previous.get("paper"))
            before = old.get("sequence") if old.get("identity") == identity else (0 if old else sequence)
            if type(before) is not int:
                before = sequence
                warnings.append("Simulation notification cursor invalid; historical messages were skipped")
            elif before > sequence:
                warnings.append("Simulation notification counter regressed; previous high-water mark retained")
            audit = [obj(row) for row in rows(simulation.get("recent_audit"), 256)]
            sequences = [_units(row.get("sequence")) for row in audit]
            valid_sequences = [item for item in sequences if item is not None]
            if valid_sequences and before + 1 < min(valid_sequences):
                warnings.append("Simulation notification audit gap; omitted secondary messages are not trades")
            fills = [row for row in audit if row.get("kind") == "venue_fill" and type(row.get("sequence")) is int and before < row["sequence"] <= sequence and (_units(row.get("quantity_micros")) or 0) > 0]
            if fills:
                total = sum(_units(row["quantity_micros"]) for row in fills)
                messages.append(f"ВИРТУАЛЬНОЕ исполнение · рынок {label(identity[0] or 'demo')}\nНовых sampled fill: {len(fills)} · акций: {money(total)}\n" + "\n".join(f"Ордер {label(row.get('order_id'))}: +{money(row['quantity_micros'])} акций" for row in fills[:6]))
            next_cursor["paper"] = {"identity": identity, "sequence": max(before, sequence)}
    if summary["real"]["selected"] and summary["real"]["balances_known"]:
        live = obj(state.get("live"))
        orders = rows(live.get("orders"))
        old = obj(previous.get("live"))
        known = obj(old.get("orders"))
        current = {}
        deltas = []
        # The live ledger keeps a bounded retained order history. Track that same
        # bounded projection, without deriving executions from position/cash changes.
        for row in orders:
            row = obj(row)
            order_id, filled = row.get("order_id"), _units(row.get("cumulative_filled_micros"))
            if not isinstance(order_id, str) or len(order_id) > 128 or filled is None:
                continue
            current[order_id] = filled
            baseline = known.get(order_id, 0) if old else filled
            if type(baseline) is not int or baseline > filled:
                warnings.append("Live notification fill counter regressed; execution was not inferred")
                if type(baseline) is int and 0 <= baseline < 2**128:
                    current[order_id] = baseline
                continue
            if filled > baseline:
                quote = obj(row.get("quote"))
                deltas.append(f"Ордер {label(order_id)} · {label(quote.get('outcome', '?')).upper()} · +{money(filled - baseline)} акций · лимит {money(quote.get('price_micros')) or '?'} USD")
        if deltas:
            messages.append("РЕАЛЬНОЕ исполнение · приращение сверенного счётчика\n" + "\n".join(deltas[:6]) + (f"\nЕщё {len(deltas) - 6} исполненных ордеров в снимке." if len(deltas) > 6 else ""))
        next_cursor["live"] = {"orders": current}
    elif summary["real"]["selected"]:
        # A temporary reconciliation failure must neither announce unverified
        # executions nor erase the previous verified counters.
        next_cursor["live"] = previous.get("live")
    return next_cursor, messages, warnings


# A small fixed font makes PNG output independent of Pillow, fonts and renderers.
_FONT = {
    "0": [14,17,19,21,25,17,14], "1": [4,12,4,4,4,4,14], "2": [14,17,1,2,4,8,31],
    "3": [30,1,1,14,1,1,30], "4": [2,6,10,18,31,2,2], "5": [31,16,16,30,1,1,30],
    "6": [14,16,16,30,17,17,14], "7": [31,1,2,4,8,8,8], "8": [14,17,17,14,17,17,14],
    "9": [14,17,17,15,1,1,14], ".": [0,0,0,0,0,12,12], ":": [0,12,12,0,12,12,0],
    "-": [0,0,0,31,0,0,0], " ": [0]*7, "/": [1,2,2,4,8,8,16],
    "A": [14,17,17,31,17,17,17], "B": [30,17,17,30,17,17,30], "C": [14,17,16,16,16,17,14],
    "D": [30,17,17,17,17,17,30], "E": [31,16,16,30,16,16,31], "F": [31,16,16,30,16,16,16],
    "G": [14,17,16,23,17,17,15], "H": [17,17,17,31,17,17,17], "I": [14,4,4,4,4,4,14],
    "K": [17,18,20,24,20,18,17], "L": [16,16,16,16,16,16,31], "M": [17,27,21,21,17,17,17],
    "N": [17,25,21,19,17,17,17], "O": [14,17,17,17,17,17,14], "P": [30,17,17,30,16,16,16],
    "R": [30,17,17,30,20,18,17], "S": [15,16,16,14,1,1,30], "T": [31,4,4,4,4,4,4],
    "U": [17,17,17,17,17,17,14], "V": [17,17,17,17,17,10,4], "W": [17,17,17,21,21,21,10],
    "X": [17,17,10,4,10,17,17], "Y": [17,17,10,4,4,4,4],
}


def deposit_chart(history):
    """Fixed PNG, at most 1440 minute marks; legacy sampled fixtures supported."""
    history = rows(history, 1440)
    points = []
    for row in history:
        row = obj(row)
        stamp = row.get("observed_at_ms")
        if type(stamp) is int and 0 <= stamp < 253_402_300_800_000:
            if "state" in row:
                state = obj(row.get("state"))
                points.append((stamp, deposit_value(state, "real", stamp), deposit_value(state, "paper", stamp), valuation_context(state)))
            else:
                context = row.get("context")
                if not isinstance(context, str) or not 0 < len(context.encode()) <= 128 or not context.isascii():
                    points.append((stamp, None, None, "invalid"))
                else:
                    points.append((stamp, _units(row.get("real_pico")), _units(row.get("paper_pico")), context))
    points.sort(key=lambda row: row[0])
    width, height = 720, 360
    image = bytearray(bytes((15, 22, 35)) * width * height)
    def pixel(x, y, color):
        if 0 <= x < width and 0 <= y < height:
            start = (y * width + x) * 3
            image[start:start + 3] = bytes(color)
    def line(x0, y0, x1, y1, color):
        dx, dy, sx, sy = abs(x1-x0), -abs(y1-y0), 1 if x0<x1 else -1, 1 if y0<y1 else -1
        error = dx + dy
        while True:
            pixel(x0, y0, color)
            if x0 == x1 and y0 == y1:
                break
            twice = 2 * error
            if twice >= dy:
                error += dy
                x0 += sx
            if twice <= dx:
                error += dx
                y0 += sy
    def text(x, y, value, color=(191,205,222), scale=1):
        for character in value.upper():
            for yy, bits in enumerate(_FONT.get(character, _FONT[" "])):
                for xx in range(5):
                    if bits & (1 << (4-xx)):
                        for a in range(scale):
                            for b in range(scale):
                                pixel(x+xx*scale+a, y+yy*scale+b, color)
            x += 6*scale
    text(24, 18, "DEPOSIT USD / 24H / 1MIN BEST BID", scale=2)
    text(24, 43, "REAL", (55,210,193))
    text(106, 43, "VIRTUAL", (182,137,255))
    text(228, 43, "GAPS = UNKNOWN / NOT REALIZED PNL")
    values = [value for row in points for value in row[1:3] if value is not None]
    left, right, top, bottom = 92, 695, 76, 298
    line(left, top, left, bottom, (86,101,121))
    line(left, bottom, right, bottom, (86,101,121))
    if values:
        low, high = min(values), max(values)
        span = max(high-low, max(high//100, PICO//100))
        low, high = max(0, low-span//10), high+span//10
        span = max(1, high-low)
        for index in range(5):
            y = top + (bottom-top)*index//4
            value = high-span*index//4
            line(left+1, y, right, y, (32,44,60))
            # Display labels have two decimal places; marks remain exact integers.
            text(12, y-3, f"{value//PICO}.{(value % PICO)//10_000_000_000:02}")
        start, end = points[0][0], points[-1][0]
        for series, color in ((1, (55,210,193)), (2, (182,137,255))):
            previous = None
            for row in points:
                value = row[series]
                if value is None:
                    previous = None
                    continue
                x = left+(right-left)*(row[0]-start)//max(1,end-start)
                y = bottom-(bottom-top)*(value-low)//span
                if previous and previous[3] == row[3] and 0 <= row[0]-previous[2] <= 90_000:
                    line(previous[0], previous[1], x, y, color)
                for dx in range(-1,2):
                    for dy in range(-1,2):
                        pixel(x+dx,y+dy,color)
                previous = (x,y,row[0],row[3])
    else:
        text(175, 177, "NO COMPLETE FRESH VALUATION", scale=2)
    if points:
        text(left, 311, datetime.fromtimestamp(points[0][0]/1000, timezone.utc).strftime("%H:%M UTC"))
        text(right-60, 311, datetime.fromtimestamp(points[-1][0]/1000, timezone.utc).strftime("%H:%M UTC"))
    text(24, 342, "SAMPLED HISTORY / NO DEPTH OR FEES / VIRTUAL EXECUTIONS ARE SYNTHETIC")
    def chunk(name, value):
        return struct.pack("!I", len(value))+name+value+struct.pack("!I", zlib.crc32(name+value)&0xffffffff)
    raw = b"".join(b"\0"+image[y*width*3:(y+1)*width*3] for y in range(height))
    return (b"\x89PNG\r\n\x1a\n"+chunk(b"IHDR", struct.pack("!IIBBBBB",width,height,8,2,0,0,0))
            +chunk(b"IDAT",zlib.compress(raw,6))+chunk(b"IEND",b""))
