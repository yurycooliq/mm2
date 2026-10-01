import json
from pathlib import Path
import sqlite3
import threading

MAX_SAMPLE_BYTES = 65_536


class Store:
    def __init__(self, path, max_samples=900, max_logs=1000, retention_ms=86_400_000):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.max_samples, self.max_logs, self.retention_ms = max_samples, max_logs, retention_ms
        self._lock = threading.Lock()
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
        """)
        self.db.commit()

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
