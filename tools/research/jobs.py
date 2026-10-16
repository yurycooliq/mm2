"""Bounded durable research jobs and fenced publication intents.

Only the trusted controller/broker may open this database. Worker tickets are
capabilities presented back to that controller, not database access grants.
"""
from contextlib import contextmanager
import hashlib
from pathlib import Path
import re
import sqlite3
import time
from .schema import ResearchError, canonical, digest, parse_json

STAGES = {"analysis", "reliability", "coding", "review", "replay", "retention"}
WAITING = {"WAITING_DATA", "WAITING_RESOURCE", "WAITING_ACCESS", "WAITING_REVIEW", "WAITING_OWNER"}
TERMINAL = {"SUCCEEDED", "FAILED", "INCONCLUSIVE", "SUPERSEDED"}
MODEL = "gpt-6-astra"
REASONING = {"xhigh", "max", "ultra"}
MAX_DOCUMENT = 512 * 1024
MAX_JOBS = 10_000
MAX_EVENTS = 100_000
MAX_DB_BYTES = 128 * 1024 * 1024
MAX_ATTEMPTS = 3


def bounded(value):
    data = canonical(value)
    if len(data) > MAX_DOCUMENT:
        raise ResearchError("controller document exceeds byte bound")
    return data.decode("utf-8")


def identity(value, name="identity"):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}", value):
        raise ResearchError(f"invalid {name}")
    return value


def sha(value, name="SHA256"):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ResearchError(f"invalid {name}")
    return value


class JobStore:
    def __init__(self, path, clock=None):
        self.path = Path(path)
        if self.path.is_symlink() or any(p.is_symlink() for p in self.path.parents):
            raise ResearchError("controller database symlink rejected")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock or (lambda: int(time.time()))
        self._quota()
        self.db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=10000")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY, stage TEXT NOT NULL, inputs TEXT NOT NULL, inputs_hash TEXT NOT NULL,
          policy_hash TEXT NOT NULL, candidate_sha TEXT, status TEXT NOT NULL,
          generation INTEGER NOT NULL DEFAULT 0, owner TEXT, run_id TEXT, lease_until INTEGER,
          attempts INTEGER NOT NULL DEFAULT 0, model TEXT, reasoning TEXT,
          result TEXT, created INTEGER NOT NULL, updated INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS events (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL,kind TEXT NOT NULL,
          generation INTEGER NOT NULL,body TEXT NOT NULL,created INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS checkpoints (
          job_id TEXT NOT NULL,generation INTEGER NOT NULL,name TEXT NOT NULL,body TEXT NOT NULL,
          hash TEXT NOT NULL,created INTEGER NOT NULL,PRIMARY KEY(job_id,generation,name));
        CREATE TABLE IF NOT EXISTS outbox (
          id TEXT PRIMARY KEY,job_id TEXT NOT NULL,generation INTEGER NOT NULL,
          fingerprint TEXT NOT NULL UNIQUE,repository TEXT NOT NULL,marker TEXT NOT NULL UNIQUE,
          body TEXT NOT NULL,body_hash TEXT NOT NULL,status TEXT NOT NULL,
          remote_id TEXT,remote_url TEXT,attempts INTEGER NOT NULL DEFAULT 0,
          created INTEGER NOT NULL,updated INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS crash_signatures (
          fingerprint TEXT PRIMARY KEY,family TEXT NOT NULL,epoch TEXT NOT NULL,
          first_seen INTEGER NOT NULL,last_seen INTEGER NOT NULL,count INTEGER NOT NULL,
          evidence TEXT NOT NULL,issue_job_id TEXT);
        CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(stage,status,created);
        INSERT OR IGNORE INTO metadata VALUES ('schema_version','1');
        """)
        if self.db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()[0] != "1":
            raise ResearchError("unsupported controller schema")

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def _quota(self):
        total = sum(p.stat().st_size for p in (self.path, Path(str(self.path) + "-wal")) if p.exists())
        if total > MAX_DB_BYTES:
            raise ResearchError("controller database capacity exhausted; archive before continuation")

    @contextmanager
    def transaction(self):
        self._quota()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def _event(self, job_id, kind, generation, body):
        if self.db.execute("SELECT count(*) FROM events").fetchone()[0] >= MAX_EVENTS:
            raise ResearchError("controller audit event quota exhausted")
        self.db.execute("INSERT INTO events(job_id,kind,generation,body,created) VALUES(?,?,?,?,?)",
                        (job_id, kind, generation, bounded(body), self.clock()))

    def enqueue(self, stage, inputs, policy_hash, candidate_sha=None):
        if stage not in STAGES:
            raise ResearchError("unsupported research stage")
        sha(policy_hash, "policy hash")
        if candidate_sha is not None and not re.fullmatch(r"[0-9a-f]{40}", candidate_sha):
            raise ResearchError("candidate must be exact Git commit SHA")
        content = bounded(inputs)
        inputs_hash = hashlib.sha256(content.encode()).hexdigest()
        job_id = digest({"stage": stage, "inputs_hash": inputs_hash, "policy_hash": policy_hash,
                         "candidate_sha": candidate_sha})
        now = self.clock()
        with self.transaction():
            if self.db.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone():
                return self.get(job_id)
            if self.db.execute("SELECT count(*) FROM jobs").fetchone()[0] >= MAX_JOBS:
                raise ResearchError("controller job quota exhausted")
            self.db.execute("INSERT INTO jobs(id,stage,inputs,inputs_hash,policy_hash,candidate_sha,status,created,updated) "
                            "VALUES(?,?,?,?,?,?,'PENDING',?,?)",
                            (job_id, stage, content, inputs_hash, policy_hash, candidate_sha, now, now))
            self._event(job_id, "ENQUEUED", 0, {"inputs_hash": inputs_hash})
        return self.get(job_id)

    @staticmethod
    def _decode(row):
        if row is None:
            return None
        item = dict(row)
        for name in ("inputs", "result", "body", "evidence"):
            if name in item and item[name] is not None:
                item[name] = parse_json(item[name])
        return item

    def get(self, job_id):
        sha(job_id, "job id")
        return self._decode(self.db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())

    def list(self, limit=100):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ResearchError("invalid job list bound")
        return [self._decode(r) for r in self.db.execute("SELECT * FROM jobs ORDER BY updated DESC,id LIMIT ?", (limit,))]

    def claim(self, stage, owner, run_id, *, model=MODEL, reasoning="max", lease_seconds=900, expected_policy_hash=None):
        if stage not in STAGES or model != MODEL or reasoning not in REASONING:
            raise ResearchError("chosen strongest model unavailable or invalid stage; no fallback")
        identity(owner, "worker identity")
        identity(run_id, "run identity")
        if type(lease_seconds) is not int or not 60 <= lease_seconds <= 3600:
            raise ResearchError("lease must be bounded 60..3600 seconds")
        now = self.clock()
        with self.transaction():
            # One active worker per stage; coding includes independent review/replay.
            group = ("coding", "review", "replay") if stage in {"coding", "review", "replay"} else (stage,)
            placeholders = ",".join("?" for _ in group)
            if self.db.execute(f"SELECT id FROM jobs WHERE stage IN ({placeholders}) AND status='RUNNING' "
                               "AND lease_until>? LIMIT 1", (*group, now)).fetchone():
                return None
            rows = self.db.execute("SELECT * FROM jobs WHERE stage=? AND (status='PENDING' OR "
                                   "(status='RUNNING' AND lease_until<=?)) ORDER BY created,id LIMIT 100",
                                   (stage, now)).fetchall()
            selected = None
            for row in rows:
                if expected_policy_hash is not None and row["policy_hash"] != expected_policy_hash:
                    continue
                # Unresolved effects fence all generations until trusted reconciliation.
                if self.db.execute("SELECT id FROM outbox WHERE job_id=? AND status IN ('DISPATCHING','UNKNOWN') LIMIT 1",
                                   (row["id"],)).fetchone():
                    continue
                if row["attempts"] >= MAX_ATTEMPTS:
                    self.db.execute("UPDATE jobs SET status='WAITING_REVIEW',updated=? WHERE id=?", (now, row["id"]))
                    self._event(row["id"], "ATTEMPTS_EXHAUSTED", row["generation"], {})
                    continue
                selected = row
                break
            if selected is None:
                return None
            generation = selected["generation"] + 1
            self.db.execute("UPDATE jobs SET status='RUNNING',generation=?,owner=?,run_id=?,lease_until=?,"
                            "attempts=attempts+1,model=?,reasoning=?,updated=? WHERE id=? AND generation=?",
                            (generation, owner, run_id, now + lease_seconds, model, reasoning, now,
                             selected["id"], selected["generation"]))
            self._event(selected["id"], "CLAIMED", generation,
                        {"owner": owner, "run_id": run_id, "model": model, "reasoning": reasoning,
                         "model_receipt_kind": "controller-declared-settings; provider-attestation-required"})
        return self.ticket(self.get(selected["id"]))

    @staticmethod
    def ticket(job):
        return {"schema_version": 1, "job_id": job["id"], "stage": job["stage"],
                "generation": job["generation"], "owner": job["owner"], "run_id": job["run_id"],
                "lease_until": job["lease_until"], "inputs_hash": job["inputs_hash"],
                "policy_hash": job["policy_hash"], "candidate_sha": job["candidate_sha"],
                "model": job["model"], "reasoning": job["reasoning"], "inputs": job["inputs"]}

    def _current(self, ticket, margin=0):
        job = self.get(ticket.get("job_id"))
        if (not job or job["status"] != "RUNNING" or job["lease_until"] <= self.clock() + margin
                or any(ticket.get(k) != job[k] for k in ("generation", "owner", "run_id", "inputs_hash", "policy_hash"))):
            raise ResearchError("stale, expired or mismatched worker ticket")
        return job

    def renew(self, ticket, lease_seconds=900):
        if type(lease_seconds) is not int or not 60 <= lease_seconds <= 3600:
            raise ResearchError("invalid renewal bound")
        with self.transaction():
            job = self._current(ticket)
            self.db.execute("UPDATE jobs SET lease_until=?,updated=? WHERE id=?",
                            (self.clock() + lease_seconds, self.clock(), job["id"]))
            self._event(job["id"], "RENEWED", job["generation"], {})
        return self.ticket(self.get(job["id"]))

    def checkpoint(self, ticket, name, body):
        identity(name, "checkpoint name")
        content, body_hash = bounded(body), digest(body)
        with self.transaction():
            job = self._current(ticket)
            row = self.db.execute("SELECT hash FROM checkpoints WHERE job_id=? AND generation=? AND name=?",
                                  (job["id"], job["generation"], name)).fetchone()
            if row:
                if row[0] != body_hash:
                    raise ResearchError("immutable checkpoint already has different content")
                return body_hash
            self.db.execute("INSERT INTO checkpoints VALUES(?,?,?,?,?,?)",
                            (job["id"], job["generation"], name, content, body_hash, self.clock()))
            self._event(job["id"], "CHECKPOINT", job["generation"], {"name": name, "hash": body_hash})
        return body_hash

    def finish(self, ticket, status, result):
        if status not in TERMINAL | WAITING:
            raise ResearchError("invalid completion state")
        content = bounded(result)
        with self.transaction():
            job = self._current(ticket)
            if self.db.execute("SELECT id FROM outbox WHERE job_id=? AND status IN ('PENDING','DISPATCHING','UNKNOWN') LIMIT 1",
                               (job["id"],)).fetchone():
                raise ResearchError("publication unresolved; reconcile before job completion")
            self.db.execute("UPDATE jobs SET status=?,result=?,lease_until=NULL,updated=? WHERE id=?",
                            (status, content, self.clock(), job["id"]))
            self._event(job["id"], status, job["generation"], {"result_hash": digest(result)})
        return self.get(job["id"])

    def wake(self, job_id, prerequisite):
        identity(prerequisite, "resolved prerequisite")
        with self.transaction():
            job = self.get(job_id)
            if not job or job["status"] not in WAITING:
                raise ResearchError("only explicit waiting prerequisite can be resumed")
            if self.db.execute("SELECT id FROM outbox WHERE job_id=? AND status IN ('DISPATCHING','UNKNOWN') LIMIT 1", (job_id,)).fetchone():
                raise ResearchError("unknown external effect still unresolved")
            self.db.execute("UPDATE jobs SET status='PENDING',updated=? WHERE id=?", (self.clock(), job_id))
            self._event(job_id, "PREREQUISITE_RESOLVED", job["generation"], {"prerequisite": prerequisite})

    def prepare_publication(self, ticket, repository, fingerprint, request):
        sha(fingerprint, "issue fingerprint")
        content = bounded(request)
        body_hash = digest(request)
        marker = f"mm2-research:{fingerprint}"
        effect_id = digest({"repository": repository, "fingerprint": fingerprint})
        with self.transaction():
            job = self._current(ticket)
            row = self.db.execute("SELECT * FROM outbox WHERE fingerprint=?", (fingerprint,)).fetchone()
            if row:
                if row["repository"] != repository or row["body_hash"] != body_hash:
                    raise ResearchError("fingerprint already bound; new evidence requires reviewed update")
                if row["status"] == "PENDING" and row["job_id"] == job["id"] and row["generation"] != job["generation"]:
                    # PREPARED is definitely pre-dispatch; the same exact intent may
                    # be adopted by a replacement generation without duplicating effects.
                    self.db.execute("UPDATE outbox SET generation=?,updated=? WHERE id=?",
                                    (job["generation"], self.clock(), row["id"]))
                    return self.outbox(row["id"])
                return self._decode(row)
            now = self.clock()
            self.db.execute("INSERT INTO outbox(id,job_id,generation,fingerprint,repository,marker,body,body_hash,status,created,updated) "
                            "VALUES(?,?,?,?,?,?,?,?,'PENDING',?,?)",
                            (effect_id, job["id"], job["generation"], fingerprint, repository,
                             marker, content, body_hash, now, now))
            self._event(job["id"], "PUBLICATION_PREPARED", job["generation"], {"effect_id": effect_id, "body_hash": body_hash})
        return self.outbox(effect_id)

    def outbox(self, effect_id):
        sha(effect_id, "publication id")
        return self._decode(self.db.execute("SELECT * FROM outbox WHERE id=?", (effect_id,)).fetchone())

    def dispatch_publication(self, ticket, effect_id, timeout=30):
        with self.transaction():
            job = self._current(ticket, timeout + 10)
            row = self.outbox(effect_id)
            if not row or row["job_id"] != job["id"] or row["generation"] != job["generation"] or row["status"] != "PENDING":
                raise ResearchError("effect cannot be dispatched by this generation")
            self.db.execute("UPDATE outbox SET status='DISPATCHING',attempts=attempts+1,updated=? WHERE id=?",
                            (self.clock(), effect_id))
            self._event(job["id"], "PUBLICATION_DISPATCHED", job["generation"], {"effect_id": effect_id})
        return self.outbox(effect_id)

    def publication_unknown(self, effect_id):
        with self.transaction():
            row = self.outbox(effect_id)
            if not row or row["status"] not in {"DISPATCHING", "UNKNOWN"}:
                raise ResearchError("invalid unknown publication state")
            self.db.execute("UPDATE outbox SET status='UNKNOWN',updated=? WHERE id=?", (self.clock(), effect_id))
            self.db.execute("UPDATE jobs SET status='WAITING_ACCESS',lease_until=NULL,updated=? WHERE id=? AND generation=?",
                            (self.clock(), row["job_id"], row["generation"]))
            self._event(row["job_id"], "PUBLICATION_UNKNOWN", row["generation"], {"effect_id": effect_id})

    def confirm_publication(self, effect_id, remote_id, remote_url, verified_body_hash):
        identity(str(remote_id), "remote issue id")
        sha(verified_body_hash, "verified body hash")
        with self.transaction():
            row = self.outbox(effect_id)
            if not row or row["status"] not in {"PENDING", "DISPATCHING", "UNKNOWN", "CONFIRMED"} or verified_body_hash != row["body_hash"]:
                raise ResearchError("publication reconciliation hash/state mismatch")
            if row["status"] == "CONFIRMED" and (row["remote_id"] != str(remote_id) or row["remote_url"] != remote_url):
                raise ResearchError("remote effect identity changed")
            self.db.execute("UPDATE outbox SET status='CONFIRMED',remote_id=?,remote_url=?,updated=? WHERE id=?",
                            (str(remote_id), remote_url, self.clock(), effect_id))
            self._event(row["job_id"], "PUBLICATION_CONFIRMED", row["generation"], {"effect_id": effect_id, "remote_id": str(remote_id)})
        return self.outbox(effect_id)

    def unknown_effects(self):
        return [self._decode(r) for r in self.db.execute("SELECT * FROM outbox WHERE status IN ('UNKNOWN','DISPATCHING') ORDER BY created LIMIT 100")]

    def register_crash(self, family, epoch, signature, evidence, actionable=False):
        identity(family, "failure family")
        identity(epoch, "deployment epoch")
        sha(signature, "sanitized crash signature")
        fingerprint = digest({"family": family, "epoch": epoch, "signature": signature})
        content, now = bounded(evidence), self.clock()
        with self.transaction():
            if self.db.execute("SELECT count(*) FROM crash_signatures").fetchone()[0] >= MAX_JOBS:
                raise ResearchError("crash signature quota exhausted")
            self.db.execute("INSERT INTO crash_signatures VALUES(?,?,?,?,?,1,?,NULL) "
                            "ON CONFLICT(fingerprint) DO UPDATE SET last_seen=excluded.last_seen,count=count+1,evidence=excluded.evidence",
                            (fingerprint, family, epoch, now, now, content))
        return {"fingerprint": fingerprint, "actionable": actionable is True,
                "observed_count": self.db.execute("SELECT count FROM crash_signatures WHERE fingerprint=?", (fingerprint,)).fetchone()[0]}
