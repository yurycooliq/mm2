from collections import OrderedDict
import hashlib
import hmac
import json
import re
import secrets
import threading
import time
from urllib.parse import parse_qsl

COOKIE = "mm2_owner"
TELEGRAM_MAX_AGE = 300
TELEGRAM_FUTURE_SKEW = 5


class Sessions:
    def __init__(self, ttl=3600, maximum=32, clock=time.monotonic):
        self.ttl, self.maximum, self.clock = ttl, maximum, clock
        self._sessions = OrderedDict()
        self._replays = OrderedDict()
        self._telegram_bindings = {}
        self._lock = threading.Lock()

    def _prune(self):
        now = self.clock()
        for mapping in (self._sessions, self._replays):
            for key in tuple(mapping):
                if mapping[key] <= now:
                    del mapping[key]
                    if mapping is self._sessions:
                        self._telegram_bindings.pop(key, None)

    def create(self, telegram_digest=None):
        with self._lock:
            self._prune()
            if len(self._sessions) >= self.maximum:
                evicted, _ = self._sessions.popitem(last=False)
                self._telegram_bindings.pop(evicted, None)
            token = secrets.token_urlsafe(32)
            self._sessions[token] = self.clock() + self.ttl
            if telegram_digest is not None:
                self._telegram_bindings[token] = telegram_digest
            return token

    def valid(self, token, telegram_digest=None):
        if not isinstance(token, str) or len(token) > 128:
            return False
        with self._lock:
            self._prune()
            return token in self._sessions and (telegram_digest is None or
                hmac.compare_digest(self._telegram_bindings.get(token, ""), telegram_digest))

    def revoke(self, token):
        with self._lock:
            self._sessions.pop(token, None)
            self._telegram_bindings.pop(token, None)

    def consume_telegram(self, digest):
        with self._lock:
            self._prune()
            if digest in self._replays:
                raise ValueError("Telegram authentication was already used; reopen the Mini App")
            if len(self._replays) >= 256:
                raise ValueError("authentication capacity reached")
            # A timestamp can initially be five seconds ahead. Retain its replay
            # fence through the complete accepted interval, including its boundary.
            self._replays[digest] = self.clock() + TELEGRAM_MAX_AGE + TELEGRAM_FUTURE_SKEW + 1


def _telegram_identity(init_data, bot_token, owner_id, now=None):
    if not isinstance(bot_token, str) or not bot_token or type(owner_id) is not int or not 0 < owner_id < 2**52:
        raise ValueError("Telegram authentication is disabled")
    if not isinstance(init_data, str) or not 1 <= len(init_data.encode("utf-8")) <= 8192:
        raise ValueError("invalid Telegram authentication length")
    if re.search(r"%(?![0-9a-fA-F]{2})", init_data):
        raise ValueError("invalid Telegram authentication encoding")
    pairs = parse_qsl(init_data, keep_blank_values=True, strict_parsing=True, max_num_fields=20,
                      encoding="utf-8", errors="strict")
    if len({key for key, _ in pairs}) != len(pairs) or any(not re.fullmatch(r"[a-z_]{1,32}", key) for key, _ in pairs):
        raise ValueError("invalid Telegram authentication fields")
    fields = dict(pairs)
    digest = fields.pop("hash", "")
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("invalid Telegram authentication signature")
    check = "\n".join(f"{key}={fields[key]}" for key in sorted(fields))
    key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    expected = hmac.new(key, check.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(digest, expected):
        raise ValueError("invalid Telegram authentication signature")
    auth_date = fields.get("auth_date", "")
    if not re.fullmatch(r"[1-9][0-9]{0,12}", auth_date):
        raise ValueError("invalid Telegram authentication date")
    age = (int(time.time()) if now is None else now) - int(auth_date)
    if age < -TELEGRAM_FUTURE_SKEW:
        raise ValueError("Telegram authentication expired")
    def unique_object(pairs):
        if len({key for key, _ in pairs}) != len(pairs):
            raise ValueError("duplicate Telegram user fields")
        return dict(pairs)
    user = json.loads(fields.get("user", "null"), object_pairs_hook=unique_object,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite Telegram user")))
    if not isinstance(user, dict) or type(user.get("id")) is not int or user["id"] != owner_id or user.get("is_bot") is True:
        raise ValueError("Telegram user is not the configured owner")
    return digest, age


def validate_telegram(init_data, bot_token, owner_id, now=None):
    digest, age = _telegram_identity(init_data, bot_token, owner_id, now)
    if age > TELEGRAM_MAX_AGE:
        raise ValueError("Telegram authentication expired")
    return digest


def telegram_session(sessions, existing, init_data, bot_token, owner_id, now=None):
    # Always validate signed identity, even with a cookie: a switched owner or
    # tampered launch must revoke access at the caller. Old data can ONLY reuse
    # its original still-live bound session, never mint or extend a new one.
    digest, age = _telegram_identity(init_data, bot_token, owner_id, now)
    if sessions.valid(existing, telegram_digest=digest):
        return existing
    if age > TELEGRAM_MAX_AGE:
        raise ValueError("Telegram authentication expired; reopen the Mini App")
    sessions.consume_telegram(digest)
    return sessions.create(telegram_digest=digest)


class RateLimiter:
    def __init__(self, capacity=20, refill=5, maximum=256, clock=time.monotonic):
        self.capacity, self.refill, self.maximum, self.clock = capacity, refill, maximum, clock
        self._buckets = {}
        self._lock = threading.Lock()

    def allow(self, key):
        with self._lock:
            now = self.clock()
            for stale in tuple(self._buckets):
                if now - self._buckets[stale][1] > 600:
                    del self._buckets[stale]
            if key not in self._buckets and len(self._buckets) >= self.maximum:
                return False
            available, last = self._buckets.get(key, (self.capacity, now))
            available = min(self.capacity, available + max(0, now - last) * self.refill)
            allowed = available >= 1
            self._buckets[key] = (available - 1 if allowed else available, now)
            return allowed
