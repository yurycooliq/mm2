import json
from pathlib import Path
import sqlite3
import threading

from .audit_projection import digest, project_audit

MAX_SAMPLE_BYTES = 65_536
EQUITY_RETENTION_MS = 7 * 86_400_000
MAX_EQUITY_POINTS = 10_080
ACTION_RETENTION_MS = 7 * 86_400_000
MAX_SAMPLED_ACTIONS = 4096
MAX_ACTION_SCOPES = 64
MAX_ACTION_STREAMS = 256


class Store:
    def __init__(self, path, max_samples=900, max_logs=1000, retention_ms=86_400_000):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.max_samples, self.max_logs, self.retention_ms = max_samples, max_logs, retention_ms
        self._lock = threading.Lock()
        self.action_observer = digest(["local-secondary-console-observer-v1"])
        self.db = sqlite3.connect(path, check_same_thread=False, timeout=0.2)
        self.db.execute("PRAGMA page_size=4096")
        self.db.execute("PRAGMA max_page_count=16384")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA wal_autocheckpoint=128")
        self.db.execute("PRAGMA journal_size_limit=1048576")
        self.db.execute("PRAGMA cache_size=-2048")
        self.db.execute("PRAGMA busy_timeout=200")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY, observed_at_ms INTEGER NOT NULL, state TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS logs(id INTEGER PRIMARY KEY, observed_at_ms INTEGER NOT NULL, level TEXT NOT NULL, message TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS equity_minutes(
                minute INTEGER PRIMARY KEY, observed_at_ms INTEGER NOT NULL,
                real_pico TEXT, paper_pico TEXT, context TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS action_scopes(
                base TEXT PRIMARY KEY, generation TEXT NOT NULL, prefix_sequence TEXT,
                prefix_hash TEXT, boot_id TEXT NOT NULL, observed_at_ms INTEGER NOT NULL,
                warnings TEXT NOT NULL, evicted INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS action_streams(
                stream TEXT PRIMARY KEY, generation TEXT NOT NULL, audit_sequence TEXT,
                observed_at_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS sampled_actions(
                id TEXT PRIMARY KEY, generation TEXT NOT NULL, first_seen_ms INTEGER NOT NULL,
                event_at_ms INTEGER NOT NULL, boot_id TEXT NOT NULL, core TEXT NOT NULL, action TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS sampled_actions_scope_time ON sampled_actions(generation,event_at_ms);
        """)
        self.db.commit()

    def configure_action_observer(self, observer):
        # Only a hash of the normalized public endpoint is retained, never tokens.
        if not isinstance(observer, str) or len(observer) != 64 or any(c not in "0123456789abcdef" for c in observer):
            raise ValueError("invalid action observer identity")
        self.action_observer = observer

    @staticmethod
    def _generation(projected, row):
        if row is None:
            return digest([projected["base"], "initial"]), False
        previous = int(row[2]) if row[2] is not None else None
        current = projected["prefix_sequence"]
        changed = (current is not None and previous is not None and
                   (current < previous or (current == previous and projected["prefix_hash"] != row[3])))
        if changed:
            return digest([projected["base"], "split", row[1], projected["boot_id"], current, projected["prefix_hash"]]), True
        return row[1], False

    @staticmethod
    def _action(action, generation):
        action = dict(action)
        action["id"] = "paper:" + digest([generation, action["id"]])
        action["generation"] = generation
        # Source counters are u64; JSON numbers would lose precision in a browser.
        action["sequence"] = str(action["sequence"])
        action["reset_epoch"] = str(action["reset_epoch"])
        core = digest([action[key] for key in ("boot_id", "reset_epoch", "market", "sequence", "source_time_ms",
                                               "kind", "order_id", "quantity_shares")])
        return action, core

    def _selected_generation(self, projected, row):
        generation, changed = self._generation(projected, row)
        if not changed:
            stream = digest([generation, projected["boot_id"], projected["epoch"], projected["market"]])
            previous = self.db.execute("SELECT audit_sequence FROM action_streams WHERE stream=?", (stream,)).fetchone()
            current = projected["audit_sequence"]
            if previous and previous[0] is not None and current is not None and current < int(previous[0]):
                # A repeated session sequence cannot reuse earlier event IDs.
                generation = digest([projected["base"], "audit-split", generation, projected["boot_id"],
                                     projected["epoch"], projected["market"], current])
                changed = True
        return generation, changed

    def action_sample(self, observed_at_ms, state):
        """Persist only validated sampled audit, before raw-state compaction."""
        projected = project_audit(state, observed_at_ms, self.action_observer)
        if projected is None:
            return True  # No selected paper audit: do not invent a binding/history.
        with self._lock:
            try:
                with self.db:
                    row = self.db.execute("SELECT * FROM action_scopes WHERE base=?", (projected["base"],)).fetchone()
                    # A stale/clock-regressed sample cannot replace the newest binding.
                    if row is not None and observed_at_ms < row[5]:
                        return True
                    generation, changed = self._selected_generation(projected, row)
                    warnings = set(projected["warnings"])
                    evicted = 0
                    if row is not None:
                        warnings.update(json.loads(row[6]))
                        evicted = row[7]
                        if row[4] != projected["boot_id"]:
                            warnings.add("BOOT_CONTINUITY_UNPROVEN")
                    if changed:
                        warnings.add("ACTION_HISTORY_GENERATION_CHANGED")
                        evicted = 0
                    stream = digest([generation, projected["boot_id"], projected["epoch"], projected["market"]])
                    previous = self.db.execute("SELECT audit_sequence,observed_at_ms FROM action_streams WHERE stream=?", (stream,)).fetchone()
                    first = projected["actions"][0]["sequence"] if projected["actions"] else None
                    previous_sequence = int(previous[0]) if previous and previous[0] is not None else None
                    if ((previous is None and first is not None and first > 1)
                            or (previous is not None and observed_at_ms - previous[1] > 3500)
                            or (previous_sequence is not None and first is not None and first > previous_sequence + 1)):
                        warnings.add("ACTION_HISTORY_SAMPLE_GAPS")
                    for action in projected["actions"]:
                        action, core = self._action(action, generation)
                        # Source wall time must not be future at the observation.
                        if action["at_ms"] is not None and action["at_ms"] > observed_at_ms:
                            warnings.add("ACTION_HISTORY_INVALID_SOURCE")
                            continue
                        existing = self.db.execute("SELECT core,action FROM sampled_actions WHERE id=?", (action["id"],)).fetchone()
                        if existing:
                            if existing[0] != core:
                                warnings.add("ACTION_HISTORY_INVALID_SOURCE")
                                continue
                            saved = json.loads(existing[1])
                            # Preserve first observation/clock. Only exact-ID quote
                            # details can fill an unknown field, never overwrite it.
                            for key in ("title", "price_usd", "side", "outcome"):
                                if saved.get(key) is None:
                                    saved[key] = action[key]
                                elif action[key] is not None and saved[key] != action[key]:
                                    warnings.add("ACTION_HISTORY_INVALID_SOURCE")
                            self.db.execute("UPDATE sampled_actions SET action=? WHERE id=?",
                                            (json.dumps(saved, separators=(",", ":")), action["id"]))
                            continue
                        if previous_sequence is not None and int(action["sequence"]) <= previous_sequence:
                            # A missing old row may have been evicted or missed.
                            # A repeated source ring is not a new first sighting.
                            warnings.add("ACTION_HISTORY_SAMPLE_GAPS")
                            continue
                        encoded = json.dumps(action, separators=(",", ":"), allow_nan=False)
                        if len(encoded.encode()) > 2048:
                            warnings.add("ACTION_HISTORY_INVALID_SOURCE")
                            continue
                        self.db.execute("INSERT INTO sampled_actions VALUES (?,?,?,?,?,?,?)",
                            (action["id"], generation, observed_at_ms, action["at_ms"] if action["at_ms"] is not None else observed_at_ms,
                             projected["boot_id"], core, encoded))
                    if previous_sequence is not None and projected["audit_sequence"] is not None:
                        projected["audit_sequence"] = max(previous_sequence, projected["audit_sequence"])
                    self.db.execute("INSERT INTO action_streams VALUES (?,?,?,?) ON CONFLICT(stream) DO UPDATE SET audit_sequence=excluded.audit_sequence,observed_at_ms=excluded.observed_at_ms",
                        (stream, generation, str(projected["audit_sequence"]) if projected["audit_sequence"] is not None else None, observed_at_ms))
                    self.db.execute("INSERT INTO action_scopes VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(base) DO UPDATE SET generation=excluded.generation,prefix_sequence=excluded.prefix_sequence,prefix_hash=excluded.prefix_hash,boot_id=excluded.boot_id,observed_at_ms=excluded.observed_at_ms,warnings=excluded.warnings,evicted=excluded.evicted",
                        (projected["base"], generation, str(projected["prefix_sequence"]) if projected["prefix_sequence"] is not None else None,
                         projected["prefix_hash"], projected["boot_id"], observed_at_ms, json.dumps(sorted(warnings)), evicted))
                    removed = self.db.execute("SELECT generation,COUNT(*) FROM sampled_actions WHERE first_seen_ms < ? OR id NOT IN (SELECT id FROM sampled_actions ORDER BY first_seen_ms DESC,id DESC LIMIT ?) GROUP BY generation",
                        (observed_at_ms - ACTION_RETENTION_MS, MAX_SAMPLED_ACTIONS)).fetchall()
                    self.db.execute("DELETE FROM sampled_actions WHERE first_seen_ms < ? OR id NOT IN (SELECT id FROM sampled_actions ORDER BY first_seen_ms DESC,id DESC LIMIT ?)",
                        (observed_at_ms - ACTION_RETENTION_MS, MAX_SAMPLED_ACTIONS))
                    for removed_generation, count in removed:
                        self.db.execute("UPDATE action_scopes SET evicted=MIN(evicted+?,2147483647) WHERE generation=?", (count, removed_generation))
                    self.db.execute("DELETE FROM action_scopes WHERE base NOT IN (SELECT base FROM action_scopes ORDER BY observed_at_ms DESC LIMIT ?)", (MAX_ACTION_SCOPES,))
                    self.db.execute("DELETE FROM action_streams WHERE observed_at_ms < ? OR stream NOT IN (SELECT stream FROM action_streams ORDER BY observed_at_ms DESC LIMIT ?)",
                                    (observed_at_ms - ACTION_RETENTION_MS, MAX_ACTION_STREAMS))
                return True
            except (sqlite3.Error, ValueError, TypeError):
                return False

    def backfill_action_samples(self, current_state, observed_at_ms):
        """One bounded migration of actual, already retained raw samples only."""
        if self.get_meta("action_backfill_v1") == self.action_observer:
            return True
        current = project_audit(current_state, observed_at_ms, self.action_observer)
        if current is None:
            return False
        last_id, gaps = 0, False
        for _ in range((min(self.max_samples, 900) + 15) // 16):
            with self._lock:
                rows = self.db.execute("SELECT id,observed_at_ms,state FROM samples WHERE id>? ORDER BY id LIMIT 16", (last_id,)).fetchall()
            if not rows:
                break
            for identity, at, raw in rows:
                last_id = identity
                try:
                    state = json.loads(raw)
                    if isinstance(state, dict) and isinstance(state.get("history_projection"), dict) and state["history_projection"].get("compacted") is True:
                        gaps = True
                    candidate = project_audit(state, at, self.action_observer)
                    # Legacy samples lack observer identity. Only an exact
                    # current boot/profile/locator/epoch match qualifies them;
                    # old boots cannot be attributed to a changed endpoint.
                    if (candidate is None or candidate["base"] != current["base"] or candidate["boot_id"] != current["boot_id"]
                            or candidate["epoch"] != current["epoch"]):
                        continue
                    if not self.action_sample(at, state):
                        raise sqlite3.OperationalError("secondary action backfill unavailable")
                except (ValueError, TypeError):
                    gaps = True
        self.set_meta("action_backfill_gaps_v1", self.action_observer if gaps else "")
        self.set_meta("action_backfill_v1", self.action_observer)
        return True

    def action_history(self, state, observed_at_ms, now_ms, start_ms=None):
        """Read-only bounded merge; selected observations never prove a ledger."""
        projected = project_audit(state, observed_at_ms, self.action_observer)
        if projected is None:
            return {"actions": [], "observed_actions": 0, "omitted_actions": 0,
                    "retention_omitted_observations": None, "warnings": ["ACTION_HISTORY_INVALID_SOURCE"], "scope_id": None}
        with self._lock:
            row = self.db.execute("SELECT * FROM action_scopes WHERE base=?", (projected["base"],)).fetchone()
            generation, changed = self._selected_generation(projected, row)
            warnings = set(projected["warnings"])
            if row is not None:
                warnings.update(json.loads(row[6]))
                if row[4] != projected["boot_id"]:
                    warnings.add("BOOT_CONTINUITY_UNPROVEN")
                if row[7]:
                    warnings.add("ACTION_HISTORY_RETENTION_LIMIT")
            if changed:
                warnings.add("ACTION_HISTORY_GENERATION_CHANGED")
            if self.db.execute("SELECT value FROM meta WHERE key='action_backfill_gaps_v1'").fetchone() == (self.action_observer,):
                warnings.add("ACTION_HISTORY_SAMPLE_GAPS")
            lower = max(now_ms - ACTION_RETENTION_MS, start_ms if start_ms is not None else 0)
            rows = self.db.execute("SELECT id,core,action FROM sampled_actions WHERE generation=? ORDER BY event_at_ms DESC,first_seen_ms DESC,id DESC LIMIT ?",
                                  (generation, MAX_SAMPLED_ACTIONS)).fetchall()
            stream = digest([generation, projected["boot_id"], projected["epoch"], projected["market"]])
            prior = self.db.execute("SELECT audit_sequence FROM action_streams WHERE stream=?", (stream,)).fetchone()
            previous_sequence = int(prior[0]) if prior and prior[0] is not None else None
        known = {identity: (core, json.loads(raw)) for identity, core, raw in rows}
        actions = {}
        for identity, (core, action) in known.items():
            at = action["at_ms"] if action["at_ms"] is not None else action["observed_at_ms"]
            if lower <= at <= now_ms and now_ms - ACTION_RETENTION_MS <= action["observed_at_ms"] <= now_ms:
                actions[identity] = (core, action)
        for action in projected["actions"]:
            action, core = self._action(action, generation)
            at = action["at_ms"] if action["at_ms"] is not None else action["observed_at_ms"]
            if not lower <= at <= now_ms or action["observed_at_ms"] > now_ms:
                continue
            if action["id"] in known:
                if known[action["id"]][0] != core:
                    warnings.add("ACTION_HISTORY_INVALID_SOURCE")
                continue
            if previous_sequence is not None and int(action["sequence"]) <= previous_sequence:
                warnings.add("ACTION_HISTORY_SAMPLE_GAPS")
                continue
            action["observation"] = "current-snapshot"
            actions[action["id"]] = (core, action)
        values = [action for _, action in actions.values()]
        if any(action["boot_id"] != projected["boot_id"] for action in values):
            warnings.add("BOOT_CONTINUITY_UNPROVEN")
        values.sort(key=lambda action: (action["at_ms"] if action["at_ms"] is not None else action["observed_at_ms"],
                                       action["observed_at_ms"], int(action["sequence"])), reverse=True)
        count = len(values)
        return {"actions": values[:256], "observed_actions": count, "omitted_actions": max(0, count - 256),
                "retention_omitted_observations": row[7] if row is not None and not changed else 0,
                "warnings": sorted(warnings), "scope_id": generation}

    def sample(self, observed_at_ms, state):
        encoded = json.dumps(state, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode()) > MAX_SAMPLE_BYTES:
            return False
        with self._lock:
            try:
                with self.db:
                    self.db.execute("INSERT INTO samples(observed_at_ms,state) VALUES (?,?)", (observed_at_ms, encoded))
                    self.db.execute("DELETE FROM samples WHERE observed_at_ms < ? OR id NOT IN (SELECT id FROM samples ORDER BY id DESC LIMIT ?)",
                                    (observed_at_ms - self.retention_ms, self.max_samples))
                return True
            except sqlite3.Error:
                return False

    def log(self, observed_at_ms, level, message):
        with self._lock:
            try:
                with self.db:
                    self.db.execute("INSERT INTO logs(observed_at_ms,level,message) VALUES (?,?,?)",
                                    (observed_at_ms, level[:16], message[:256]))
                    self.db.execute("DELETE FROM logs WHERE observed_at_ms < ? OR id NOT IN (SELECT id FROM logs ORDER BY id DESC LIMIT ?)",
                                    (observed_at_ms - self.retention_ms, self.max_logs))
                return True
            except sqlite3.Error:
                return False

    def history(self, limit):
        with self._lock:
            rows = self.db.execute("SELECT observed_at_ms,state FROM samples ORDER BY id DESC LIMIT ?", (min(max(limit, 1), 120),)).fetchall()
        return [{"observed_at_ms": stamp, "state": json.loads(state)} for stamp, state in reversed(rows)]

    def logs(self, limit):
        with self._lock:
            rows = self.db.execute("SELECT id,observed_at_ms,level,message FROM logs ORDER BY id DESC LIMIT ?", (min(max(limit, 1), 100),)).fetchall()
        return [{"id": row[0], "observed_at_ms": row[1], "level": row[2], "message": row[3]} for row in rows]

    def equity_sample(self, observed_at_ms, real_pico, paper_pico, context):
        """One compact row per minute, bounded independently of raw UI samples."""
        if type(observed_at_ms) is not int or not 0 <= observed_at_ms < 253_402_300_800_000:
            return False
        if not isinstance(context, str) or not 0 < len(context.encode()) <= 128 or not context.isascii():
            return False
        values = []
        for value in (real_pico, paper_pico):
            if value is not None and (type(value) is not int or not 0 <= value < 2**128):
                return False
            values.append(str(value) if value is not None else None)
        minute = observed_at_ms // 60_000
        with self._lock:
            try:
                with self.db:
                    self.db.execute("""INSERT INTO equity_minutes(minute,observed_at_ms,real_pico,paper_pico,context)
                        VALUES (?,?,?,?,?) ON CONFLICT(minute) DO UPDATE SET
                        observed_at_ms=excluded.observed_at_ms,real_pico=excluded.real_pico,
                        paper_pico=excluded.paper_pico,context=excluded.context
                        WHERE excluded.observed_at_ms > equity_minutes.observed_at_ms""",
                        (minute, observed_at_ms, *values, context))
                    self.db.execute("""DELETE FROM equity_minutes WHERE observed_at_ms < ?
                        OR minute NOT IN (SELECT minute FROM equity_minutes ORDER BY minute DESC LIMIT ?)""",
                        (observed_at_ms - EQUITY_RETENTION_MS, MAX_EQUITY_POINTS))
                return True
            except sqlite3.Error:
                return False

    def equity_history(self, limit=1440, now_ms=None):
        """At most 24 hours of minute points, in time order; never extrapolate."""
        if now_ms is None:
            import time
            now_ms = int(time.time() * 1000)
        if type(now_ms) is not int or not 0 <= now_ms < 253_402_300_800_000:
            raise ValueError("invalid equity history clock")
        if type(limit) is not int:
            raise ValueError("invalid equity history limit")
        limit = min(max(limit, 1), 1440)
        with self._lock:
            rows = self.db.execute("""SELECT observed_at_ms,real_pico,paper_pico,context FROM equity_minutes
                WHERE observed_at_ms >= ? AND observed_at_ms <= ? ORDER BY minute DESC LIMIT ?""",
                (now_ms - 86_400_000, now_ms, limit)).fetchall()
        return [{"observed_at_ms": row[0], "real_pico": row[1], "paper_pico": row[2], "context": row[3]}
                for row in reversed(rows)]

    def get_meta(self, key, default=""):
        with self._lock:
            row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_meta(self, key, value):
        with self._lock, self.db:
            self.db.execute("INSERT INTO meta(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

    def close(self):
        with self._lock:
            self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self.db.close()
