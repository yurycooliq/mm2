"""The exporter preserves the bounded Rust diagnostics contract, never raw state."""
import json
import unittest

from scripts.research_export import safe_health


class SafeHealthContractTest(unittest.TestCase):
    def test_actual_rust_diagnostic_fields_and_recovery_phases_survive_projection(self):
        source = {"attempts": 7, "transport_disconnects": 6, "bootstrap_timeouts": 2,
            "backoff_resets": 1, "retry_delay_ms": 500, "snapshots_seen": [30, 20],
            "snapshots_discarded_before_cutoff": [2, 3], "bootstrap_cutoff_exchange_ms": [1791283500000, 1791283500123],
            "phase": "awaiting_native_snapshot", "last_failure": "native_snapshot_bootstrap_timeout",
            "last_failure_fatal": False, "last_failure_at_mono_ms": 45000}
        state = {"feed_diagnostics": {"clob": source, "binance": {**source, "phase": "rest_recovery"}}}
        projected = safe_health(state)["feed_diagnostics"]
        self.assertEqual(projected["clob"], source)
        self.assertEqual(projected["binance"]["phase"], "rest_recovery")
        state["feed_diagnostics"]["clob"]["phase"] = "faulted"
        self.assertEqual(safe_health(state)["feed_diagnostics"]["clob"]["phase"], "faulted")
        source["snapshots_seen"][0] = 999
        self.assertEqual(projected["clob"]["snapshots_seen"], [30, 20])

    def test_malformed_counter_arrays_raw_errors_and_unapproved_fields_are_dropped(self):
        marker = "never-publish-private-marker"
        clob = {"attempts": marker, "transport_disconnects": True, "bootstrap_timeouts": -1,
            "backoff_resets": 2**64, "retry_delay_ms": {"private_key": marker},
            "snapshots_seen": [1, marker], "snapshots_discarded_before_cutoff": [False, 1],
            "bootstrap_cutoff_exchange_ms": [1, 2, 3], "last_failure": [marker], "phase": {"private_key": marker},
            "last_failure_fatal": 1, "last_failure_at_mono_ms": None,
            "rpc_url": marker, "signed_intents": marker, "token": marker, "raw_error": marker}
        result = safe_health({"mode": [marker], "feed_diagnostics": {"clob": clob, "unapproved": clob},
            "live": {"signed_intents": marker}, "fault": marker})
        self.assertEqual(result["feed_diagnostics"], {"clob": {"last_failure_at_mono_ms": None}})
        self.assertNotIn(marker, json.dumps(result))

    def test_old_snapshots_do_not_fabricate_zero_counters_or_ready_phases(self):
        result = safe_health({"mode": "paused", "api_ready": True})
        self.assertNotIn("feed_diagnostics", result)
        legacy = safe_health({"feed_diagnostics": {"clob": {"reconnects": 4,
            "native_snapshots_received": 8, "phase": "bootstrap"}}})
        self.assertEqual(legacy["feed_diagnostics"]["clob"], {})


if __name__ == "__main__":
    unittest.main()
