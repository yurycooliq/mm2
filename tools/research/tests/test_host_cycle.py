"""Real immutable account packets and crash-safe controller cycles; no network."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tools.research import host
from tools.research.archive import publish_bundle, read_member, verify_bundle
from tools.research.controller import DEFAULT_POLICY, load_policy
from tools.research.jobs import JobStore
from tools.research.observe_mo_money import observe_account
from tools.research.registry import load_registry
from tools.research.schema import ResearchError, load_json, parse_json, quality


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = Path(__file__).parent / "fixtures"


class FixtureAPI:
    def __init__(self, name):
        self.name = name

    def get(self, endpoint, parameters):
        rows = load_json(FIXTURES / (self.name + "-" + endpoint + ".json"))["rows"]
        offset, limit = parameters["offset"], parameters["limit"]
        if endpoint == "activity":
            rows = [row for row in rows if parameters["start"] <= row["timestamp"] <= parameters["end"]]
        selected = deepcopy(rows[offset:offset + limit])
        return selected, {"observed_at_utc": "2026-10-06T10:04:01Z", "rows": len(selected)}


class FixtureS3:
    def __init__(self):
        self.exports = []

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self

    def paginate(self, **arguments):
        assert arguments["Prefix"] == "exports/mm2/"
        yield {"Contents": [{"Key": "exports/mm2/" + identity + "/manifest.json"} for identity in self.exports]}


class HostCycleTest(unittest.TestCase):
    def setUp(self):
        (ROOT / "output").mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="host-cycle-", dir=ROOT / "output")
        self.root = Path(self.temporary.name)
        self.now = datetime(2026, 10, 6, 10, 4, tzinfo=timezone.utc)
        self.s3 = FixtureS3()
        self.config = {"state": str(self.root), "region": "fixture-only", "bucket": "fixture-only",
                       "policy": str(DEFAULT_POLICY), "source_revision": "a" * 40}
        self.uploads = []
        self.uploader = patch.object(host, "upload_bundle", side_effect=self.upload)
        self.uploader.start()

    def tearDown(self):
        self.uploader.stop()
        self.temporary.cleanup()

    def upload(self, _, bucket, packet, prefix):
        self.uploads.append((bucket, verify_bundle(packet)["bundle_id"], prefix))
        return {"fixture": True}

    def observe(self, root, revision, start, end):
        result = []
        for account in load_registry()["accounts"]:
            observed = observe_account(FixtureAPI(account["name"]), account, start, end)
            bundle = publish_bundle(root / "archive", {
                "events.jsonl": observed["events"], "closing.json": observed["snapshot"],
                "coverage.json": observed["coverage"]},
                scope={"kind": "one-public-account", "lane": "public-benchmark", "account_id": account["account_id"]},
                utc_range={"start": start, "end": end}, quality=observed["quality"],
                source_revision=revision, created_at_utc=end)
            result.append({"account": account["name"], "bundle": str(bundle)})
        return result

    def cycle(self, now=None, observe=None):
        return host.run_cycle(self.config, s3=self.s3, observe=observe or self.observe, now=now or self.now)

    def diagnostic(self, incidents, *, end=None, engine_revision="b" * 40, context=None):
        end = end or self.now
        fmt = lambda stamp: stamp.isoformat().replace("+00:00", "Z")
        packet = publish_bundle(self.root / "exports", {
            "health.json": {"api_ready": False, "fault_class": "runtime_fault"},
            "diagnostics.json": {"incidents": incidents, **(context or {})}, "coverage.json": {"financial_complete": False}},
            scope={"kind": "sanitized-host-diagnostics", "source_id": "fixture-engine", "lane": "public-paper",
                   "engine_source_revision": engine_revision, "incident_epoch_attribution": "UNVERIFIED_HISTORICAL_TAIL"},
            utc_range={"start": fmt(end - timedelta(hours=1)), "end": fmt(end)},
            quality=quality("PARTIAL", ["BOUNDED_DIAGNOSTICS"]), source_revision="c" * 40,
            created_at_utc=fmt(end))
        self.s3.exports.append(packet.name)
        return packet

    @staticmethod
    def incident(stamp="1791281000000000", source="docker_engine"):
        return {"category": "runtime_unknown", "message": "diagnostic_code=runtime_fault",
                "fingerprint": "d" * 64, "source": source, "observed_timestamp_us": stamp}

    def test_each_backfill_window_keeps_its_own_lifecycle_context_and_old_unknowns(self):
        contexts = [None, {"captured_at_utc": "2026-10-06T10:04:00+00:00", "systemd": {"NRestarts": "0"},
                          "containers": [{"service": "engine", "restarts": 0, "oom_killed": False}]},
                         {"captured_at_utc": "2026-10-06T10:04:02Z", "systemd": {"NRestarts": "7"},
                          "containers": [{"service": "engine", "restarts": 7, "oom_killed": True}]}]
        bundles = [self.diagnostic([], end=self.now + timedelta(seconds=index), context=context)
                   for index, context in enumerate(contexts)]
        result = self.cycle(self.now + timedelta(seconds=3))
        windows = read_member(self.root / "archive" / result["packet"], "diagnostics.json")["windows"]
        by_id = {window["bundle_id"]: window["lifecycle_context"] for window in windows}
        self.assertIsNone(by_id[bundles[0].name]["systemd"])
        self.assertIsNone(by_id[bundles[0].name]["captured_at_utc"])
        for index in (1, 2):
            self.assertEqual(by_id[bundles[index].name]["systemd"], contexts[index]["systemd"])
            self.assertEqual(by_id[bundles[index].name]["containers"][0]["restarts"], (0, 7)[index - 1])
            self.assertEqual(by_id[bundles[index].name]["captured_at_utc"], contexts[index]["captured_at_utc"])
            self.assertFalse(by_id[bundles[index].name]["historic_root_cause_proven"])
        self.assertEqual(self.cycle(self.now + timedelta(seconds=4))["diagnostic_windows_processed"], 0)


    def test_two_account_archive_packets_repeat_without_duplicate_window_or_false_profit(self):
        first = self.cycle()
        second = self.cycle(self.now + timedelta(minutes=10))
        self.assertNotEqual(first["packet"], second["packet"])
        self.assertEqual(len(first["new_jobs"]), 1)
        self.assertEqual(second["new_jobs"], [])
        packet = self.root / "archive" / first["packet"]
        accounts = read_member(packet, "accounting-summary.json")
        self.assertEqual(set(accounts), {"mo-money", "mm2-public"})
        self.assertNotEqual(accounts["mo-money"]["scope"]["account_id"], accounts["mm2-public"]["scope"]["account_id"])
        for account in accounts.values():
            self.assertIsNone(account["net_pnl_pico"])
            self.assertIsNone(account["roi"])
            self.assertFalse(account["quality"]["eligibility"]["accounting"])
        self.assertFalse(read_member(packet, "provenance.json")["financial_outperformance_established"])
        self.assertFalse(first["outperformance_established"])
        self.assertFalse(first["primary_mutations"])
        with JobStore(self.root / "jobs.sqlite3") as store:
            self.assertEqual(len(store.list()), 1)
            self.assertEqual(store.get(first["new_jobs"][0])["inputs"]["bundles"][0]["bundle_id"], first["packet"])

    def test_lost_ack_after_enqueue_reuses_reserved_first_packet_on_restart(self):
        enqueue = host.enqueue

        def crash_after_enqueue(*args, **kwargs):
            enqueue(*args, **kwargs)
            raise RuntimeError("fixture process death after durable enqueue")

        with patch.object(host, "enqueue", side_effect=crash_after_enqueue), self.assertRaises(RuntimeError):
            self.cycle()
        with JobStore(self.root / "jobs.sqlite3") as store:
            old = store.list()[0]
            frozen = old["inputs"]["bundles"][0]["bundle_id"]
            record = parse_json(store.db.execute("SELECT value FROM metadata WHERE key='analysis-window-2026-10-06-1'").fetchone()[0])
            self.assertIsNone(record["job_id"])
            self.assertEqual(record["reference"]["bundle_id"], frozen)
        resumed = self.cycle(self.now + timedelta(minutes=10))
        self.assertNotEqual(resumed["packet"], frozen)
        with JobStore(self.root / "jobs.sqlite3") as store:
            self.assertEqual(len(store.list()), 1)
            self.assertEqual(store.list()[0]["id"], old["id"])
        self.assertEqual(self.cycle(self.now + timedelta(minutes=20))["new_jobs"], [])

    def test_concurrent_connections_freeze_one_packet_and_complete_one_job(self):
        first, second = self.cycle(), self.cycle(self.now + timedelta(minutes=10))
        packets = [self.root / "archive" / value["packet"] for value in (first, second)]
        policy, policy_hash = load_policy(DEFAULT_POLICY)
        barrier = threading.Barrier(2)

        def schedule(index):
            with JobStore(self.root / "jobs.sqlite3") as store:
                barrier.wait()
                record = host._reserve_job(store, "parallel-window", packets[index], policy_hash, {"window_key": "parallel"})
                return host._finish_reserved_job(store, "parallel-window", record, policy, policy_hash, "analysis")

        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(schedule, [0, 1]))
        self.assertEqual(results[0][0], results[1][0])
        self.assertEqual(sum(created for _, created in results), 1)
        with JobStore(self.root / "jobs.sqlite3") as store:
            self.assertEqual(store.db.execute("SELECT count(*) FROM jobs WHERE inputs LIKE '%parallel%'").fetchone()[0], 1)

    def test_old_rolling_diagnostics_are_not_new_crashes_or_analytics_epoch(self):
        self.diagnostic([self.incident()])
        first = self.cycle()
        self.assertEqual(len(first["new_jobs"]), 2)
        self.assertEqual(self.cycle(self.now + timedelta(minutes=1))["new_jobs"], [])
        self.diagnostic([self.incident()], end=self.now + timedelta(minutes=2), engine_revision="e" * 40)
        self.assertEqual(self.cycle(self.now + timedelta(minutes=2))["new_jobs"], [])
        with JobStore(self.root / "jobs.sqlite3") as store:
            row = store.db.execute("SELECT * FROM crash_signatures").fetchone()
            self.assertEqual(row["count"], 1)
            self.assertTrue(row["epoch"].startswith("unverified:"))
            self.assertNotIn(self.config["source_revision"], row["epoch"])
            evidence = parse_json(row["evidence"])
            self.assertEqual(evidence["current_engine_source_revision"], "b" * 40)
            self.assertIsNone(evidence["incident_engine_source_revision"])
            self.assertFalse(evidence["historic_root_cause_proven"])

    def test_distinct_timestamp_increments_occurrence_without_duplicate_reliability_job(self):
        self.diagnostic([self.incident(), self.incident("1791281001000000")])
        self.cycle()
        self.cycle(self.now + timedelta(minutes=1))
        with JobStore(self.root / "jobs.sqlite3") as store:
            self.assertEqual(store.db.execute("SELECT count FROM crash_signatures").fetchone()[0], 2)
            self.assertEqual(store.db.execute("SELECT count(*) FROM jobs WHERE stage='reliability'").fetchone()[0], 1)

    def test_timestamp_unknown_and_secondary_console_never_claim_engine_epoch(self):
        self.diagnostic([self.incident(None), self.incident(None, "docker_console")])
        self.cycle()
        self.diagnostic([self.incident(None), self.incident(None, "docker_console")], end=self.now + timedelta(minutes=1))
        self.cycle(self.now + timedelta(minutes=1))
        with JobStore(self.root / "jobs.sqlite3") as store:
            rows = store.db.execute("SELECT * FROM crash_signatures").fetchall()
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row["count"] == 1 for row in rows))
            secondary = next(parse_json(row["evidence"]) for row in rows if parse_json(row["evidence"])["component"] == "secondary-console")
            self.assertIsNone(secondary["current_engine_source_revision"])
            self.assertFalse(secondary["unique_incident_occurrence_verified"])

    def test_older_only_failure_is_backfilled_even_when_latest_window_is_empty(self):
        old = self.diagnostic([self.incident()])
        latest = self.diagnostic([], end=self.now + timedelta(minutes=1))
        result = self.cycle(self.now + timedelta(minutes=1))
        self.assertEqual(result["diagnostic_windows_processed"], 2)
        self.assertEqual(len(result["new_jobs"]), 2)
        packet = self.root / "archive" / result["packet"]
        self.assertEqual(read_member(packet, "provenance.json")["diagnostics_bundle"], latest.name)
        self.assertEqual(read_member(packet, "diagnostics.json")["incidents"][0]["diagnostic_bundle_id"], old.name)
        self.assertFalse(read_member(packet, "diagnostics.json")["historic_root_cause_proven"])
        repeated = self.cycle(self.now + timedelta(minutes=2))
        self.assertEqual(repeated["new_jobs"], [])
        self.assertEqual(repeated["diagnostic_windows_processed"], 0)

    def test_backfill_queue_is_bounded_to_sixteen_windows_and_resumes_remainder(self):
        for index in range(17):
            incident = self.incident(str(1791281000000000 + index))
            incident["fingerprint"] = format(index + 1, "064x")
            self.diagnostic([incident], end=self.now + timedelta(seconds=index))
        first = self.cycle(self.now + timedelta(minutes=1))
        self.assertEqual(first["diagnostic_windows_processed"], 16)
        self.assertEqual(first["diagnostic_backlog"], 1)
        second = self.cycle(self.now + timedelta(minutes=2))
        self.assertEqual(second["diagnostic_windows_processed"], 1)
        self.assertEqual(second["diagnostic_backlog"], 0)
        self.assertEqual(len(second["new_jobs"]), 1)
        with JobStore(self.root / "jobs.sqlite3") as store:
            self.assertEqual(store.db.execute("SELECT count(*) FROM metadata WHERE key LIKE 'diagnostics-consumed-%'").fetchone()[0], 17)
            self.assertEqual(store.db.execute("SELECT count(*) FROM jobs WHERE stage='reliability'").fetchone()[0], 17)

    def test_crash_before_consumed_receipt_does_not_repeat_failure_or_job(self):
        self.diagnostic([self.incident()])
        insert = host._metadata_insert

        def crash_before_receipt(store, key, value):
            if key.startswith("diagnostics-consumed-"):
                raise RuntimeError("fixture death before consumed receipt")
            return insert(store, key, value)

        with patch.object(host, "_metadata_insert", side_effect=crash_before_receipt), self.assertRaises(RuntimeError):
            self.cycle()
        resumed = self.cycle(self.now + timedelta(minutes=1))
        self.assertEqual(resumed["new_jobs"], [])
        self.assertEqual(resumed["diagnostic_windows_processed"], 1)
        with JobStore(self.root / "jobs.sqlite3") as store:
            self.assertEqual(store.db.execute("SELECT count FROM crash_signatures").fetchone()[0], 1)
            self.assertEqual(len(store.list()), 2)

    def test_new_six_hour_bucket_creates_one_new_analysis(self):
        self.cycle()
        next_window = self.cycle(self.now.replace(hour=12))
        self.assertEqual(len(next_window["new_jobs"]), 1)
        self.assertEqual(self.cycle(self.now.replace(hour=12, minute=5))["new_jobs"], [])

    def test_timezone_is_normalized_before_selecting_the_utc_window(self):
        self.cycle()
        local_noon = datetime(2026, 10, 6, 12, 4, tzinfo=timezone(timedelta(hours=1)))
        self.assertEqual(self.cycle(local_noon)["new_jobs"], [])
        with self.assertRaisesRegex(ResearchError, "explicit timezone"):
            self.cycle(self.now.replace(tzinfo=None))

    def test_empty_directory_index_is_also_finite(self):
        for index in range(4):
            (self.root / str(index)).mkdir()
        with patch.object(host, "MAX_TREE_ENTRIES", 3), self.assertRaisesRegex(ResearchError, "quota"):
            host.tree_budget(self.root)

    def test_foreign_account_label_is_rejected_before_any_job(self):
        def forged(*arguments):
            captures = self.observe(*arguments)
            captures[0]["account"], captures[1]["account"] = captures[1]["account"], captures[0]["account"]
            return captures

        with self.assertRaisesRegex(ResearchError, "identity"):
            # A public account's name cannot substitute for the verified wallet scope.
            self.cycle(observe=forged)

    def test_tree_headroom_and_metadata_quotas_preserve_existing_evidence(self):
        first = self.cycle()
        bytes_before = (self.root / "archive" / first["packet"] / "manifest.json").read_bytes()
        with patch.object(host, "MAX_TREE", host.MAX_CYCLE_GROWTH), self.assertRaisesRegex(ResearchError, "headroom"):
            self.cycle(self.now + timedelta(minutes=1))
        with patch.object(host, "MAX_METADATA", 1), self.assertRaisesRegex(ResearchError, "metadata quota"):
            self.cycle(self.now.replace(hour=12))
        self.assertEqual((self.root / "archive" / first["packet"] / "manifest.json").read_bytes(), bytes_before)
        with JobStore(self.root / "jobs.sqlite3") as store:
            self.assertEqual(len(store.list()), 1)

    def test_filesystem_free_space_exhaustion_stops_before_observer_or_upload(self):
        with patch.object(host.shutil, "disk_usage", return_value=SimpleNamespace(free=1)), \
                patch.object(host, "collect_public") as observe, self.assertRaisesRegex(ResearchError, "filesystem headroom"):
            host.run_cycle(self.config, s3=self.s3, now=self.now)
        observe.assert_not_called()
        self.assertEqual(self.uploads, [])

    def test_export_index_bounds_include_nonmanifest_keys(self):
        class ExcessKeys:
            def get_paginator(self, _):
                return self

            def paginate(self, **_):
                yield {"Contents": [{"Key": "unrecognized/" + str(index)} for index in range(501)]}

        with self.assertRaisesRegex(ResearchError, "page quota"):
            host.export_ingest(ExcessKeys(), "fixture-only", self.root / "exports")


class ChildBoundTest(unittest.TestCase):
    def test_stdout_and_stderr_share_one_quota_without_spool_files(self):
        script = "import os; os.write(1,b'x'*20000); os.write(2,b'y'*20000)"
        with self.assertRaisesRegex(ResearchError, "combined output quota"):
            host.bounded_observer([sys.executable, "-B", "-c", script], deadline=3, maximum=30000)
        self.assertEqual(host.bounded_observer([sys.executable, "-B", "-c", "print('{}')"], deadline=3), b"{}\n" if os.name != "nt" else b"{}\r\n")

    def test_global_deadline_kills_a_silent_dns_equivalent_child(self):
        started = time.monotonic()
        with self.assertRaisesRegex(ResearchError, "global deadline"):
            host.bounded_observer([sys.executable, "-B", "-c", "import time; time.sleep(10)"], deadline=0.15)
        self.assertLess(time.monotonic() - started, 4)

    @unittest.skipIf(os.name == "nt", "Linux deployment process-group cleanup")
    def test_exited_parent_with_descendant_owned_pipes_has_bounded_cleanup(self):
        script = "import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time; time.sleep(10)'])"
        started = time.monotonic()
        with self.assertRaisesRegex(ResearchError, "global deadline"):
            host.bounded_observer([sys.executable, "-B", "-c", script], deadline=0.15)
        self.assertLess(time.monotonic() - started, 2)


if __name__ == "__main__":
    unittest.main()
