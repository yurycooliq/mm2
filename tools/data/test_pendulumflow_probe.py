import copy
import io
import json
from pathlib import Path
import struct
import unittest

from pendulumflow_probe import BoundedHTTP, ProbeError, RemoteParquet, check_footer, integer_clocks, market_id, normalize_manifest


class Response(io.BytesIO):
    def __init__(self, data, status=206, content_range="bytes 4-7/16", content_length="4"):
        super().__init__(data)
        self.status = status
        self.headers = {"Content-Range": content_range, "Content-Length": content_length}
        self.body_read = False

    def read(self, size=-1):
        self.body_read = True
        return super().read(size)


class ArchiveContracts(unittest.TestCase):
    def setUp(self):
        self.raw = json.loads((Path(__file__).parent / "tests/fixtures/pendulumflow_manifest.json").read_text())

    def test_half_open_ranges_and_inclusive_row_groups_remain_distinct(self):
        product = normalize_manifest(self.raw, "2026-09-28T20")["products"]["last_trade_price"]
        self.assertEqual(product["byte_range"], [4, 12])
        self.assertEqual(product["row_groups"], [0, 0])

    def test_wrong_hour_digest_range_and_rows_fail_closed(self):
        for field, value in [("hour", "2026-09-29/20"), ("file", "../foreign.parquet"), ("sha256", "bad"), ("bytes", True), ("row_count", 2)]:
            with self.subTest(field=field):
                raw = {**self.raw, field: value}
                with self.assertRaises(ProbeError):
                    normalize_manifest(raw, "2026-09-28T20")
        raw = copy.deepcopy(self.raw)
        raw["products"]["last_trade_price"]["byte_range"] = [4, 24]
        with self.assertRaises(ProbeError):
            normalize_manifest(raw, "2026-09-28T20")

    def test_overlapping_products_and_unknown_columns_are_rejected(self):
        raw = copy.deepcopy(self.raw)
        raw["products"]["book"] = copy.deepcopy(raw["products"]["last_trade_price"])
        raw["row_count"] = 2
        with self.assertRaises(ProbeError):
            normalize_manifest(raw, "2026-09-28T20")
        raw = copy.deepcopy(self.raw)
        raw["products"]["last_trade_price"]["columns"].append(42)
        with self.assertRaises(ProbeError):
            normalize_manifest(raw, "2026-09-28T20")

    def test_condition_ids_preserve_binary_and_ascii_encodings(self):
        expected = "0x" + "ab" * 32
        self.assertEqual(market_id(bytes.fromhex("ab" * 32)), expected)
        self.assertEqual(market_id(expected.encode("ascii")), expected)
        with self.assertRaises(ProbeError):
            market_id(b"short")


class RangeProtocol(unittest.TestCase):
    def test_inclusive_http_range_exact_bytes_and_budget(self):
        requests = []
        def opener(req, timeout):
            requests.append(req)
            return Response(b"data")
        http = BoundedHTTP(4, opener)
        self.assertEqual(http.fetch("https://example.test/file", 4, 8, 16), b"data")
        self.assertEqual(requests[0].headers["Range"], "bytes=4-7")
        self.assertEqual(http.bytes_read, 4)
        with self.assertRaises(ProbeError):
            http.fetch("https://example.test/file", 8, 9, 16)
        self.assertEqual(len(requests), 1)

    def test_range_refusal_wrong_bounds_and_large_response_do_not_read_body(self):
        for response in [Response(b"huge", status=200), Response(b"data", content_range="bytes 5-8/16"), Response(b"huge", content_length="1000000")]:
            with self.subTest(response=response.headers):
                http = BoundedHTTP(4, lambda req, timeout: response)
                with self.assertRaises(ProbeError):
                    http.fetch("https://example.test/file", 4, 8, 16)
                self.assertFalse(response.body_read)

    def test_truncated_range_is_not_success(self):
        http = BoundedHTTP(4, lambda req, timeout: Response(b"bad"))
        with self.assertRaisesRegex(ProbeError, "truncated"):
            http.fetch("https://example.test/file", 4, 8, 16)

    def test_footer_magic_and_budget_are_verified_without_whole_file_read(self):
        data = b"PAR1meta" + struct.pack("<I", 4) + b"PAR1"
        def opener(req, timeout):
            start, end = map(int, req.headers["Range"].removeprefix("bytes=").split("-"))
            return Response(data[start:end + 1], content_range=f"bytes {start}-{end}/{len(data)}", content_length=str(end - start + 1))
        http = BoundedHTTP(16, opener)
        reader = RemoteParquet(http, "https://example.test/file", len(data))
        footer = check_footer(reader)
        self.assertTrue(footer["magic_verified"])
        self.assertEqual(footer["bytes"], 4)
        self.assertEqual(http.bytes_read, 16)
        reader.seek(4)
        self.assertEqual(reader.read(4), b"meta")
        self.assertEqual(http.bytes_read, 16)

    def test_unbounded_read_rejected_before_network(self):
        calls = []
        http = BoundedHTTP(4, lambda req, timeout: calls.append(req))
        with self.assertRaisesRegex(ProbeError, "budget"):
            RemoteParquet(http, "https://example.test/file", 1_000_000).read()
        self.assertEqual(calls, [])


class ClockContract(unittest.TestCase):
    def test_pyarrow_clock_units_fail_closed_and_keep_epoch_precision(self):
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest("optional PyArrow is unavailable")
        received = 1790625600123456
        table = pa.table({"timestamp": pa.array([1790625600123], type=pa.timestamp("ms", tz="UTC")), "timestamp_received": pa.array([received], type=pa.timestamp("us", tz="UTC"))})
        self.assertEqual(integer_clocks(table).to_pylist()[0]["timestamp_received"], received)
        wrong = table.set_column(1, "timestamp_received", pa.array([1790625600123], type=pa.timestamp("ms", tz="UTC")))
        with self.assertRaisesRegex(ProbeError, "units"):
            integer_clocks(wrong)


if __name__ == "__main__":
    unittest.main()
