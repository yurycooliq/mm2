"""Offline acquisition tests use actual Parquet bytes and a strict fake Range server."""

import contextlib
import copy
import decimal
import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
import subprocess

from pendulumflow_import import (ArchiveHTTP, acquire, candidate_groups, canonical_event,
                                 decimal_value, hour_time, normalize_spec, projection_decision,
                                 supervise_worker, uint_string)
from pendulumflow_probe import BASE, PRODUCTS, ProbeError

CONDITION = "0x" + "11" * 32
OTHER_CONDITION = "0x" + "ee" * 32
UP, DOWN = str(2 ** 255 + 123), str(2 ** 255 + 456)
START = 1790712600
PAYLOADS = {
    "book": ["market", "asset_id", "bids", "asks"],
    "price_change": ["market", "asset_id", "price", "size", "side", "best_bid", "best_ask"],
    "best_bid_ask": ["market", "asset_id", "best_bid", "best_ask", "spread"],
    "last_trade_price": ["market", "asset_id", "price", "size", "side", "fee_rate_bps", "transaction_hash"],
    "new_market": ["market", "id", "assets_ids", "outcomes", "question", "slug"],
    "market_resolved": ["market", "id", "assets_ids", "winning_asset_id", "winning_outcome"],
    "tick_size_change": ["market", "asset_id", "old_tick_size", "new_tick_size"],
}
COMMON = ["timestamp", "timestamp_received", "sequence", "source_witness", "witness_set", "arrival_skew"]


def base_spec():
    return {"schemaVersion": 1, "source": "pendulumflow-v3", "datasetId": "offline-acquisition",
            "primaryHours": ["2026-09-29T20"],
            "hours": [{"hour": f"2026-09-29T{hour}", "manifestSha256": "a" * 64, "fileSha256Expected": "b" * 64} for hour in (19, 20, 21)],
            "limits": {"maxNetworkBytes": 10_000_000, "maxSelectedRows": 100,
                       "maxRuntimeSeconds": 30, "maxOutputBytes": 1_000_000},
            "selection": {"holdoutEligible": False, "reason": "Fixed offline development fixture"},
            "markets": [{"conditionId": CONDITION, "slug": f"btc-updown-5m-{START}",
                         "windowStart": START, "windowEnd": START + 300,
                         "tokens": [{"tokenId": UP, "outcome": "Up"}, {"tokenId": DOWN, "outcome": "Down"}],
                         "metadataSource": {"url": "https://example.test/metadata", "path": "fixture.json", "sha256": "c" * 64}}]}


def native_row(kind="last_trade_price", hour="2026-09-29T20", sequence=1, selected=True, token=UP):
    condition = CONDITION if selected else OTHER_CONDITION
    return {"event_type": kind, "timestamp": START * 1000 + 123,
            "timestamp_received": int(hour_time(hour).timestamp()) * 1_000_000 + 123456,
            "sequence": sequence, "source_witness": "c", "witness_set": "|c|k|", "arrival_skew": 123,
            "market": bytes.fromhex(condition[2:]), "asset_id": int(token).to_bytes(32, "big"),
            "price": decimal.Decimal("0.4000"), "size": decimal.Decimal("1.234567"), "side": "BUY",
            "best_bid": decimal.Decimal("0.4000"), "best_ask": decimal.Decimal("0.6000"), "spread": decimal.Decimal("0.2000"),
            "bids": [{"price": decimal.Decimal("0.4000"), "size": decimal.Decimal("123.456789")}],
            "asks": [{"price": decimal.Decimal("0.6000"), "size": decimal.Decimal("10.000000")}],
            "fee_rate_bps": 700, "transaction_hash": bytes.fromhex("ab" * 32),
            "id": "native-market-id", "assets_ids": [int(UP).to_bytes(32, "big"), int(DOWN).to_bytes(32, "big")],
            "outcomes": ["Up", "Down"], "question": "BTC five-minute fixture", "slug": f"btc-updown-5m-{START}",
            "winning_asset_id": int(DOWN).to_bytes(32, "big"), "winning_outcome": "Down",
            "old_tick_size": decimal.Decimal("0.0100"), "new_tick_size": decimal.Decimal("0.0010")}


class FakeResponse(io.BytesIO):
    def __init__(self, data, status, headers):
        super().__init__(data)
        self.status, self.headers = status, headers


class FakeArchive:
    def __init__(self, directory, market_statistics=True, timestamps=None):
        import pyarrow as pa
        import pyarrow.parquet as pq
        self.spec, self.resources, self.calls = base_spec(), {}, []
        metadata = directory / "identity.json"
        metadata.write_bytes(b'{"purpose":"synthetic identity fixture"}')
        self.spec["markets"][0]["metadataSource"].update({"path": str(metadata), "sha256": hashlib.sha256(metadata.read_bytes()).hexdigest()})
        depth = pa.list_(pa.struct([("price", pa.decimal128(9, 4)), ("size", pa.decimal128(18, 6))]))
        fields = [("event_type", pa.string()), ("timestamp", pa.timestamp("ms", tz="UTC")),
                  ("timestamp_received", pa.timestamp("us", tz="UTC")), ("sequence", pa.uint64()),
                  ("source_witness", pa.string()), ("witness_set", pa.string()), ("arrival_skew", pa.int64()),
                  ("market", pa.binary()), ("asset_id", pa.binary()), ("bids", depth), ("asks", depth),
                  ("price", pa.decimal128(9, 4)), ("size", pa.decimal128(18, 6)), ("side", pa.string()),
                  ("best_bid", pa.decimal128(9, 4)), ("best_ask", pa.decimal128(9, 4)), ("spread", pa.decimal128(9, 4)),
                  ("fee_rate_bps", pa.uint32()), ("transaction_hash", pa.binary()), ("id", pa.string()),
                  ("assets_ids", pa.list_(pa.binary())), ("outcomes", pa.list_(pa.string())),
                  ("question", pa.string()), ("slug", pa.string()), ("winning_asset_id", pa.binary()),
                  ("winning_outcome", pa.string()), ("old_tick_size", pa.decimal128(9, 4)), ("new_tick_size", pa.decimal128(9, 4))]
        schema = pa.schema(fields)
        for pin in self.spec["hours"]:
            hour = pin["hour"]
            buffer, groups = io.BytesIO(), {}
            sequence, index = 1, 0
            statistics = True if market_statistics else ["event_type"]
            with pq.ParquetWriter(buffer, schema, write_statistics=statistics) as writer:
                for kind in sorted(PRODUCTS):
                    first = index
                    for selected in (True, False):
                        tokens = [UP, DOWN] if selected and kind == "book" else [UP]
                        rows = []
                        for token in tokens:
                            original = native_row(kind, hour, sequence, selected, token)
                            if selected and timestamps and kind in timestamps:
                                original["timestamp"] = timestamps[kind]
                            sequence += 1
                            relevant = {"event_type", *COMMON, *PAYLOADS[kind]}
                            rows.append({name: original.get(name) if name in relevant else None for name, _ in fields})
                        writer.write_table(pa.Table.from_pylist(rows, schema=schema))
                        index += 1
                    groups[kind] = [first, index - 1]
            data = buffer.getvalue()
            parquet = pq.ParquetFile(io.BytesIO(data))
            products = {}
            for kind, (first, last) in groups.items():
                offsets, ends, row_count = [], [], 0
                for group_index in range(first, last + 1):
                    group = parquet.metadata.row_group(group_index)
                    row_count += group.num_rows
                    for col_index in range(group.num_columns):
                        col = group.column(col_index)
                        offset = min(x for x in (col.dictionary_page_offset, col.data_page_offset) if x is not None and x >= 0)
                        offsets.append(offset)
                        ends.append(offset + col.total_compressed_size)
                start, end = min(offsets), max(ends)
                products[kind] = {"byte_range": [start, end], "row_groups": [first, last],
                                  "columns": [*COMMON, *PAYLOADS[kind]], "row_count": row_count,
                                  "sha256": hashlib.sha256(data[start:end]).hexdigest()}
            root_hash = hashlib.sha256(data).hexdigest()
            manifest = {"hour": hour.replace("T", "/"), "file": f"{hour}.parquet", "bytes": len(data),
                        "row_count": sum(x["row_count"] for x in products.values()), "sha256": root_hash,
                        "products": products, "merger_git_sha": "synthetic-fixture", "sequence_note": "per-hour fixture"}
            raw_manifest = json.dumps(manifest, separators=(",", ":")).encode()
            pin.update({"manifestSha256": hashlib.sha256(raw_manifest).hexdigest(), "fileSha256Expected": root_hash})
            base = f"{BASE}/v3/{hour.replace('T', '/')}/"
            self.resources[base + "manifest.json"] = raw_manifest
            self.resources[base + manifest["file"]] = data

    def open(self, request, timeout):
        self.calls.append(request)
        data = self.resources[request.full_url]
        bounds = request.headers.get("Range")
        if bounds:
            start, end = map(int, bounds.removeprefix("bytes=").split("-"))
            body = data[start:end + 1]
            return FakeResponse(body, 206, {"Content-Range": f"bytes {start}-{end}/{len(data)}", "Content-Length": str(len(body))})
        return FakeResponse(data, 200, {"Content-Length": str(len(data))})


class SelectionAndPrecision(unittest.TestCase):
    def test_fixed_selection_requires_adjacent_hours_native_ids_and_non_holdout(self):
        self.assertIs(normalize_spec(base_spec())["selection"]["holdoutEligible"], False)
        for change in (lambda x: x["hours"].pop(), lambda x: x["markets"][0]["tokens"][0].update(tokenId="001"),
                       lambda x: x["selection"].update(holdoutEligible=True), lambda x: x.update(schemaVersion=True),
                       lambda x: x.update(primaryHours=[{}]), lambda x: x["markets"][0].update(tokens=[None, {}])):
            spec = base_spec()
            change(spec)
            with self.assertRaises(ProbeError):
                normalize_spec(spec)

    def test_native_decimal_microsecond_and_uint64_values_survive(self):
        market = base_spec()["markets"][0]
        original = native_row(sequence=2 ** 64 - 1)
        original = {key: original[key] for key in ["event_type", *COMMON, *PAYLOADS["last_trade_price"]]}
        event = canonical_event(original, market, "2026-09-29T20", {"rowGroup": 0})
        self.assertEqual(event["sequence"], str(2 ** 64 - 1))
        self.assertEqual(event["receivedTimeUs"], "1790712000123456")
        self.assertEqual(event["eventTimeMs"], "1790712600123")
        self.assertEqual(event["arrivalSkewUs"], "123")
        self.assertEqual(event["tokenId"], UP)
        self.assertEqual(event["payload"]["fee_rate_bps"], "700")
        self.assertEqual(event["payload"]["size"], "1.234567")
        self.assertEqual(event["payload"]["transaction_hash"], "0x" + "ab" * 32)
        self.assertEqual(decimal_value(decimal.Decimal("1E-7")), "0.0000001")
        with self.assertRaises(ProbeError):
            decimal_value(0.4)
        with self.assertRaises(ProbeError):
            uint_string(2 ** 64, 64)

    def test_wrong_token_resolution_and_partition_clock_fail_closed(self):
        market = base_spec()["markets"][0]
        for update in ({"asset_id": (123).to_bytes(32, "big")}, {"timestamp_received": 1},
                       {"event_type": "market_resolved", "winning_outcome": "Up"}):
            original = native_row()
            original.update(update)
            with self.assertRaises(ProbeError):
                canonical_event(original, market, "2026-09-29T20", {})

    def test_projection_bounds_are_inclusive_cover_window_and_retain_exceptions(self):
        spec = base_spec()
        market = spec["markets"][0]
        market.update(archiveStartMs=(START - 60) * 1000, archiveEndMs=(START + 420) * 1000)
        normalize_spec(spec)
        for timestamp in (market["archiveStartMs"], market["archiveEndMs"]):
            self.assertEqual(projection_decision({"event_type": "book", "timestamp": timestamp}, market), (True, "inside-projection"))
        self.assertEqual(projection_decision({"event_type": "book", "timestamp": None}, market), (True, "unknown-clock-retained"))
        self.assertEqual(projection_decision({"event_type": "new_market", "timestamp": None}, market), (True, "lifecycle-unknown-clock-retained"))
        self.assertEqual(projection_decision({"event_type": "market_resolved", "timestamp": market["archiveEndMs"] + 1}, market), (True, "lifecycle-outside-projection-retained"))
        self.assertEqual(projection_decision({"event_type": "price_change", "timestamp": market["archiveEndMs"] + 1}, market), (False, "outside-projection-excluded"))
        market["archiveStartMs"] = START * 1000 + 1
        with self.assertRaisesRegex(ProbeError, "cover"):
            normalize_spec(spec)


class BoundedCanonicalAcquisition(unittest.TestCase):
    def setUp(self):
        try:
            import pyarrow
        except ImportError:
            self.skipTest("optional PyArrow unavailable")
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.archive = FakeArchive(self.directory)

    def run_acquire(self, spec=None, name="output"):
        with contextlib.redirect_stderr(io.StringIO()):
            return acquire(spec or self.archive.spec, self.directory / name, self.archive.open)

    def test_all_seven_types_both_outcomes_provenance_and_pruning(self):
        report = self.run_acquire()
        out = self.directory / "output"
        events = [json.loads(line) for line in (out / "events.jsonl").read_text().splitlines()]
        self.assertEqual(report["eventCount"], 24)
        self.assertEqual({e["eventType"] for e in events}, PRODUCTS)
        self.assertEqual({e["outcome"] for e in events if e["eventType"] == "book"}, {"Up", "Down"})
        self.assertEqual({e["partitionHour"] for e in events}, {"2026-09-29T19", "2026-09-29T20", "2026-09-29T21"})
        self.assertEqual(report["status"], "acquired-unvalidated")
        self.assertIs(report["holdoutEligible"], False)
        self.assertIs(report["liveEligible"], False)
        self.assertIs(report["coverage"]["bookParityValidated"], False)
        self.assertEqual(hashlib.sha256((out / "events.jsonl").read_bytes()).hexdigest(), report["eventsSha256"])
        for hour in report["hours"]:
            self.assertFalse(hour["wholeFileChecksumVerified"])
            for product in hour["products"].values():
                self.assertEqual(len(product["selectedRowGroups"]), 1)
                self.assertFalse(product["wholeProductChecksumVerified"])
                self.assertEqual(product["decodedRows"], product["selectedRows"])
        for request in report["requests"]:
            artifact = out / request["artifact"]
            self.assertEqual(hashlib.sha256(artifact.read_bytes()).hexdigest(), request["sha256"])
        self.assertFalse((out / "events.partial.jsonl").exists())

    def test_absent_market_statistics_scan_instead_of_silently_drop(self):
        self.archive = FakeArchive(self.directory, market_statistics=False)
        report = self.run_acquire()
        self.assertEqual(report["eventCount"], 24)
        for hour in report["hours"]:
            for product in hour["products"].values():
                self.assertEqual(len(product["selectedRowGroups"]), 2)
                self.assertGreater(product["decodedRows"], product["selectedRows"])

    def test_projected_acquisition_counts_exclusions_and_retains_lifecycle_unknown_clocks(self):
        lower, upper = (START - 60) * 1000, (START + 420) * 1000
        self.archive = FakeArchive(self.directory, timestamps={"best_bid_ask": lower - 1, "price_change": None,
                                   "new_market": None, "market_resolved": upper + 1,
                                   "last_trade_price": upper, "tick_size_change": lower})
        self.archive.spec["markets"][0].update(archiveStartMs=lower, archiveEndMs=upper)
        report = self.run_acquire()
        self.assertEqual(report["eventCount"], 21)
        self.assertTrue(report["coverage"]["completeProjectedSelection"])
        self.assertFalse(report["coverage"]["completeSelectedPartitions"])
        for field in ("excludedByTime", "unknownClockRetained", "lifecycleOutsideRetained", "lifecycleUnknownClockRetained"):
            self.assertEqual(report["projection"][field], 3)
        self.assertTrue(report["eventChecksumComplete"])
        for hour in report["hours"]:
            self.assertEqual(set(hour["products"]), PRODUCTS)
            product = hour["products"]["best_bid_ask"]
            self.assertEqual(product["matchedMarketRows"], 1)
            self.assertEqual(product["selectedRows"], 0)
            self.assertEqual(product["excludedByTime"], 1)

    def test_row_and_network_limits_leave_explicit_incomplete_dataset(self):
        for field, value, expected in [("maxSelectedRows", 1, "selected-row"), ("maxNetworkBytes", 1, "byte limit")]:
            spec = copy.deepcopy(self.archive.spec)
            spec["limits"][field] = value
            with self.assertRaisesRegex(ProbeError, expected):
                self.run_acquire(spec, field)
            out = self.directory / field
            report = json.loads((out / "dataset-manifest.json").read_text())
            self.assertEqual(report["status"], "incomplete")
            self.assertFalse(report["coverage"]["completeSelectedPartitions"])
            self.assertFalse((out / "events.jsonl").exists())
            self.assertLessEqual(report["eventCount"], spec["limits"]["maxSelectedRows"])
            self.assertLessEqual(report["networkBytes"], spec["limits"]["maxNetworkBytes"])

    def test_changed_manifest_or_local_metadata_pin_aborts(self):
        spec = copy.deepcopy(self.archive.spec)
        spec["hours"][0]["manifestSha256"] = "0" * 64
        with self.assertRaisesRegex(ProbeError, "manifest checksum"):
            self.run_acquire(spec)
        before = len(self.archive.calls)
        Path(spec["markets"][0]["metadataSource"]["path"]).write_bytes(b"changed")
        with self.assertRaisesRegex(ProbeError, "identity metadata checksum"):
            self.run_acquire(self.archive.spec, "metadata-changed")
        self.assertEqual(before, len(self.archive.calls))

    def test_expired_runtime_prevents_network(self):
        calls = []
        reader = ArchiveHTTP(100, time.monotonic() - 1, self.directory, lambda req, timeout: calls.append(req))
        with self.assertRaisesRegex(ProbeError, "runtime limit"):
            reader.fetch("https://example.test/manifest.json")
        self.assertEqual(calls, [])

    def test_hung_owned_worker_is_killed_and_checkpoint_is_incomplete(self):
        out = self.directory / "hung-worker"
        out.mkdir()
        (out / "events.partial.jsonl").write_bytes(b"partial\n")
        (out / "dataset-manifest.json").write_text(json.dumps({"eventCount": 12, "eventsSha256": "a" * 64,
                    "coverage": {"completeSelectedPartitions": True, "completeProjectedSelection": True}}))
        class HungWorker:
            def __init__(self):
                self.terminated, self.killed, self.waits = False, False, []
            def wait(self, timeout):
                self.waits.append(timeout)
                if not self.killed:
                    raise subprocess.TimeoutExpired("only-owned-worker", timeout)
                return -9
            def terminate(self):
                self.terminated = True
            def kill(self):
                self.killed = True
        worker = HungWorker()
        commands = []
        def factory(command):
            commands.append(command)
            return worker
        self.assertEqual(supervise_worker(["owned-python", "worker"], out, self.archive.spec, factory, timeout=0.01), 124)
        self.assertTrue(worker.terminated and worker.killed)
        self.assertEqual(commands, [["owned-python", "worker"]])
        report = json.loads((out / "dataset-manifest.json").read_text())
        self.assertEqual(report["eventCount"], 12)
        self.assertEqual(report["status"], "incomplete")
        self.assertFalse(report["coverage"]["completeProjectedSelection"])
        self.assertFalse(report["coverage"]["completeSelectedPartitions"])
        self.assertTrue(report["countsAreCheckpointed"])
        self.assertIsNone(report["eventsSha256"])
        self.assertEqual(report["partialFileBytes"], 8)


if __name__ == "__main__":
    unittest.main()
