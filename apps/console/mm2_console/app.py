import copy
import re
import threading
import time

from .auth import RateLimiter, Sessions
from .engine import EngineClient, Reply, redact
from .store import Store


def command_payload(value, limits=False):
    keys = {"id", "expected_version", "limits" if limits else "action"}
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError("command requires id, expected_version and action/limits only")
    if not isinstance(value["id"], str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value["id"]):
        raise ValueError("invalid command ID")
    if type(value["expected_version"]) is not int or not 0 <= value["expected_version"] < 2**64:
        raise ValueError("invalid expected_version")
    if not limits:
        if not isinstance(value["action"], str) or value["action"] not in {"pause", "resume", "kill", "reset"}:
            raise ValueError("unsupported owner action")
    else:
        fields = {"initial_collateral_usd", "max_imbalance_shares", "lot_shares", "half_width", "max_feed_age_ms"}
        data = value["limits"]
        if not isinstance(data, dict) or set(data) != fields:
            raise ValueError("unsupported limit fields")
        for field in fields - {"max_feed_age_ms"}:
            decimal = data[field]
            if not isinstance(decimal, str) or not re.fullmatch(r"[0-9]{1,20}(?:\.[0-9]{1,24})?", decimal):
                raise ValueError("limits require exact fixed decimal strings")
            fraction = decimal.partition(".")[2]
            if any(digit != "0" for digit in fraction[6:]):
                raise ValueError("limit precision exceeds six decimals")
        if type(data["max_feed_age_ms"]) is not int or not 1 <= data["max_feed_age_ms"] <= 60_000:
            raise ValueError("invalid feed age")
    return value


class App:
    def __init__(self, config, engine=None, store=None):
        self.config = config
        self.engine = engine or EngineClient(config.engine_url, config.engine_token, config.secret_values)
        self.store = store or Store(config.state_dir / "secondary.sqlite3")
        self.sessions = Sessions()
        self.ingress = RateLimiter()
        self.auth_attempts = RateLimiter(capacity=6, refill=0.1)
        self.command_rate = RateLimiter(capacity=6, refill=0.2)
        self._lock = threading.Lock()
        self._sample_lock = threading.Lock()
        self._snapshot = {"state": None, "observed_at_ms": None, "connected": False, "error": "waiting for first engine sample"}
        self._metrics = {"sampling_interval_ms": 1000, "sample_count": 0, "history_dropped": 0,
                         "logs_dropped": 0, "overruns": 0, "last_sampling_ms": None, "storage_error": None}
        self._last_log_identity = None
        self.stop = threading.Event()
        self._sampler = None

    def log(self, level, message):
        message = redact(str(message), self.config.secret_values)
        if not self.store.log(int(time.time() * 1000), level, message):
            with self._lock:
                self._metrics["logs_dropped"] += 1
                self._metrics["storage_error"] = "secondary log persistence unavailable"

    def sample_once(self):
        # At most one sampler can ever issue a private state request.
        with self._sample_lock:
            started = time.monotonic()
            reply = self.engine.request("GET", "/v1/state")
            now = int(time.time() * 1000)
            connected = reply.status == 200 and type(reply.payload.get("command_version")) is int
            with self._lock:
                self._metrics["sample_count"] += 1
                self._metrics["last_sampling_ms"] = round((time.monotonic() - started) * 1000, 3)
                if connected:
                    state = redact(reply.payload, self.config.secret_values)
                    self._snapshot = {"state": state, "observed_at_ms": now, "connected": True, "error": None}
                else:
                    state = None
                    self._snapshot["connected"] = False
                    self._snapshot["error"] = "engine unavailable; displayed state is the last successful sample" if self._snapshot["state"] else "engine unavailable; no state has been sampled"
            if state is not None:
                if not self.store.sample(now, state):
                    with self._lock:
                        self._metrics["history_dropped"] += 1
                        self._metrics["storage_error"] = "sample exceeds 64 KiB or secondary persistence unavailable"
                identity = (state.get("boot_id"), state.get("command_version"), state.get("mode"),
                            state.get("fault"), state.get("source_status", {}).get("phase"), state.get("feed_ready"))
                if identity != self._last_log_identity:
                    self.log("ERROR" if state.get("fault") else "INFO",
                             f"Engine {state.get('mode', 'unknown')}; feeds {'fresh' if state.get('feed_ready') else 'not ready'}; command version {state.get('command_version')}")
                    self._last_log_identity = identity
            elif self._last_log_identity != ("offline",):
                self.log("WARN", "Engine unavailable; secondary state is stale")
                self._last_log_identity = ("offline",)

    def state(self):
        with self._lock:
            result = copy.deepcopy(self._snapshot)
            result["telemetry"] = dict(self._metrics)
        now = int(time.time() * 1000)
        if result["observed_at_ms"] is not None and now - result["observed_at_ms"] > 3500:
            result["connected"] = False
            result["error"] = "sampler has not produced a fresh state; displayed state is stale"
        publication = (result["state"] or {}).get("published_at_ms")
        result["telemetry"]["publication_lag_ms"] = max(0, now - publication) if type(publication) is int else None
        if result["state"] is not None:
            if type(publication) is not int or publication < 0 or publication > now + 5000:
                result["connected"] = False
                result["error"] = "engine publication clock is invalid; displayed state is stale"
            elif now - publication > 3000:
                result["connected"] = False
                result["error"] = "engine publication is older than 3 seconds; displayed state is stale"
        return result

    def proxy(self, value, limits=False):
        value = command_payload(value, limits)
        reply = self.engine.request("PUT" if limits else "POST", "/v1/limits" if limits else "/v1/control", value)
        self.log("INFO" if 200 <= reply.status < 300 else "WARN",
                 f"Owner {'limits' if limits else value['action']} command {value['id']}: HTTP {reply.status}; durable receipt required")
        return Reply(reply.status, redact(reply.payload, self.config.secret_values))

    def start(self):
        with self._lock:
            if self._sampler is not None:
                return
            self._sampler = threading.Thread(target=self._sample_loop, name="single-engine-sampler", daemon=True)
            self._sampler.start()

    def _sample_loop(self):
        while not self.stop.is_set():
            started = time.monotonic()
            try:
                self.sample_once()
            except Exception:
                # Do not serialize exceptions: request URLs may contain provider secrets.
                with self._lock:
                    self._snapshot["connected"] = False
                    self._snapshot["error"] = "secondary sampler failed"
                    self._metrics["history_dropped"] += 1
            elapsed = time.monotonic() - started
            if elapsed > 1:
                with self._lock:
                    self._metrics["overruns"] += 1
            self.stop.wait(max(0.05, 1 - elapsed))

    def close(self):
        self.stop.set()
        if self._sampler:
            self._sampler.join(timeout=3)
            if self._sampler.is_alive():
                return
        self.store.close()
