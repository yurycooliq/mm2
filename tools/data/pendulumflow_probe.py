"""Bounded archive inspection, not a fill simulator or a historical book converter."""

import argparse
import datetime as dt
import decimal
import hashlib
import io
import json
from pathlib import Path
import re
import struct
import urllib.request

BASE = "https://archive.pendulumflow.com"
PRODUCTS = {"book", "price_change", "best_bid_ask", "last_trade_price", "new_market", "market_resolved", "tick_size_change"}


class ProbeError(ValueError):
    pass


def positive_int(value, name):
    if type(value) is not int or value <= 0:
        raise ProbeError(f"{name} must be a positive integer")
    return value


def digest(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", value):
        raise ProbeError("invalid SHA256")
    return value.lower()


def normalize_manifest(raw, hour):
    if not isinstance(raw, dict):
        raise ProbeError("manifest must be an object")
    expected_file = f"{hour}.parquet"
    if raw.get("file") != expected_file or raw.get("hour") != hour.replace("T", "/"):
        raise ProbeError("manifest hour/file does not match the requested hour")
    size = positive_int(raw.get("bytes"), "bytes")
    rows = positive_int(raw.get("row_count"), "row_count")
    products = raw.get("products")
    if not isinstance(products, dict) or not products or set(products) - PRODUCTS:
        raise ProbeError("unknown or missing product map")
    normalized = {}
    for name, product in products.items():
        if not isinstance(product, dict):
            raise ProbeError("product must be an object")
        bounds, groups, columns = product.get("byte_range"), product.get("row_groups"), product.get("columns")
        if not isinstance(bounds, list) or len(bounds) != 2 or any(type(x) is not int for x in bounds):
            raise ProbeError("invalid half-open byte range")
        start, end = bounds
        if not 4 <= start < end <= size - 8:
            raise ProbeError("product range outside Parquet body")
        if not isinstance(groups, list) or len(groups) != 2 or any(type(x) is not int for x in groups) or not 0 <= groups[0] <= groups[1]:
            raise ProbeError("invalid inclusive row-group range")
        if not isinstance(columns, list) or not columns or any(not isinstance(x, str) for x in columns) or len(set(columns)) != len(columns):
            raise ProbeError("invalid product columns")
        normalized[name] = {**product, "sha256": digest(product.get("sha256")), "row_count": positive_int(product.get("row_count"), "product rows")}
    ranges = sorted((p["byte_range"][0], p["byte_range"][1]) for p in normalized.values())
    if any(left[1] > right[0] for left, right in zip(ranges, ranges[1:])):
        raise ProbeError("overlapping product byte ranges")
    if sum(p["row_count"] for p in normalized.values()) != rows:
        raise ProbeError("product row counts do not reconcile")
    return {**raw, "bytes": size, "sha256": digest(raw.get("sha256")), "products": normalized}


class BoundedHTTP:
    def __init__(self, max_bytes, opener=urllib.request.urlopen):
        self.max_bytes = positive_int(max_bytes, "max_bytes")
        self.bytes_read = 0
        self.opener = opener
        self.requests = []

    def fetch(self, url, start=None, end=None, total=None, limit=512_000):
        if start is not None and (type(start) is not int or type(end) is not int or type(total) is not int or not 0 <= start < end <= total):
            raise ProbeError("invalid requested range")
        amount = end - start if start is not None else min(limit, self.max_bytes - self.bytes_read)
        if amount <= 0 or amount > self.max_bytes - self.bytes_read:
            raise ProbeError("explicit network byte budget exhausted")
        headers = {"Accept-Encoding": "identity", "User-Agent": "olympus-updown-archive-probe/1"}
        if start is not None:
            headers["Range"] = f"bytes={start}-{end - 1}"
        with self.opener(urllib.request.Request(url, headers=headers), timeout=30) as response:
            if start is not None:
                expected = f"bytes {start}-{end - 1}/{total}"
                if response.status != 206 or response.headers.get("Content-Range") != expected:
                    raise ProbeError("server refused or changed the requested Range")
            elif response.status != 200:
                raise ProbeError("unexpected manifest HTTP status")
            content_length = response.headers.get("Content-Length")
            if content_length is not None and (not content_length.isdecimal() or int(content_length) > amount):
                raise ProbeError("response exceeds request byte limit")
            if start is not None and content_length is not None and int(content_length) != amount:
                raise ProbeError("Range Content-Length mismatch")
            data = response.read(amount)
            self.bytes_read += len(data)
            if start is not None and len(data) != amount:
                raise ProbeError("truncated Range response")
            self.requests.append({"url": url, "byte_range": [start, end] if start is not None else None, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
            return data


class RemoteParquet(io.RawIOBase):
    def __init__(self, http, url, size):
        self.http, self.url, self.size, self.position = http, url, size, 0
        self.cache = []

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        position = offset + (self.position if whence == 1 else self.size if whence == 2 else 0)
        if whence not in (0, 1, 2) or not 0 <= position <= self.size:
            raise ProbeError("seek outside Parquet")
        self.position = position
        return position

    def range(self, start, end):
        for cached_start, data in self.cache:
            if cached_start <= start and end <= cached_start + len(data):
                return data[start - cached_start:end - cached_start]
        data = self.http.fetch(self.url, start, end, self.size)
        self.cache.append((start, data))
        return data

    def read(self, size=-1):
        end = self.size if size is None or size < 0 else min(self.size, self.position + size)
        data = self.range(self.position, end) if end > self.position else b""
        self.position = end
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)


def check_footer(reader):
    if reader.range(0, 4) != b"PAR1":
        raise ProbeError("missing Parquet header")
    trailer = reader.range(reader.size - 8, reader.size)
    if trailer[4:] != b"PAR1":
        raise ProbeError("missing Parquet footer magic")
    length = struct.unpack("<I", trailer[:4])[0]
    if not 0 < length <= reader.size - 12:
        raise ProbeError("invalid Parquet footer length")
    footer = reader.range(reader.size - 8 - length, reader.size - 8)
    return {"bytes": length, "sha256": hashlib.sha256(footer).hexdigest(), "magic_verified": True}


def check_product_footer(metadata, product, name):
    first, last = product["row_groups"]
    if last >= metadata.num_row_groups:
        raise ProbeError("manifest row group not present in footer")
    rows = 0
    for index in range(first, last + 1):
        group = metadata.row_group(index)
        rows += group.num_rows
        found_type = False
        for i in range(group.num_columns):
            col = group.column(i)
            offset = min(x for x in (col.dictionary_page_offset, col.data_page_offset) if x is not None and x >= 0)
            if offset < product["byte_range"][0] or offset + col.total_compressed_size > product["byte_range"][1]:
                raise ProbeError("footer column chunk outside manifest product")
            if col.path_in_schema == "event_type":
                stats = col.statistics
                if stats is None or not stats.has_min_max or stats.min != name or stats.max != name:
                    raise ProbeError("row group event type does not match manifest")
                found_type = True
        if not found_type:
            raise ProbeError("missing event type statistics")
    if rows != product["row_count"]:
        raise ProbeError("footer product row count mismatch")


def json_value(value):
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, bytes):
        return "0x" + value.hex()
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def market_id(value):
    if isinstance(value, bytes):
        value = "0x" + value.hex() if len(value) == 32 else value.decode("ascii")
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value):
        raise ProbeError("unrecognized condition-id encoding")
    return value.lower()


def integer_clocks(table):
    import pyarrow as pa
    for name, unit in (("timestamp", "ms"), ("timestamp_received", "us")):
        index = table.schema.get_field_index(name)
        if index < 0:
            raise ProbeError("missing event/receive clock column")
        clock_type = table.schema.field(index).type
        if not pa.types.is_timestamp(clock_type) or clock_type.unit != unit or clock_type.tz != "UTC":
            raise ProbeError("unexpected event/receive clock units")
        # Keep source epoch units exact; Python timezone database availability must not change a sample.
        table = table.set_column(index, name, table.column(index).cast(pa.int64()))
    return table


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("inspect", "trades"))
    parser.add_argument("--hour", required=True, help="UTC YYYY-MM-DDTHH")
    parser.add_argument("--max-bytes", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True, help="artifact directory outside the checkout")
    market_choice = parser.add_mutually_exclusive_group()
    market_choice.add_argument("--market", help="one explicit market condition id")
    market_choice.add_argument("--first-market", action="store_true", help="generic pilot, first actual trade market; no strategy relevance")
    parser.add_argument("--max-row-groups", type=int, default=1)
    parser.add_argument("--max-rows", type=int, default=100)
    args = parser.parse_args()
    dt.datetime.strptime(args.hour, "%Y-%m-%dT%H")
    positive_int(args.max_row_groups, "max_row_groups")
    positive_int(args.max_rows, "max_rows")
    if args.mode == "trades" and not (args.market or args.first_market):
        raise ProbeError("trades requires --market or explicit generic --first-market")
    selected_market = market_id(args.market) if args.market else None
    out = args.out.resolve()
    checkout = Path(__file__).resolve().parents[3]
    if out == checkout or checkout in out.parents:
        raise ProbeError("provenance must be stored outside the checkout")
    hour_url = f"{BASE}/v3/{args.hour.replace('T', '/')}/"
    http = BoundedHTTP(args.max_bytes)
    raw_manifest = http.fetch(hour_url + "manifest.json")
    manifest = normalize_manifest(json.loads(raw_manifest), args.hour)
    reader = RemoteParquet(http, hour_url + manifest["file"], manifest["bytes"])
    report = {"hour": args.hour, "mode": args.mode, "market": selected_market, "products": manifest["products"], "footer": check_footer(reader), "whole_file_sha256": {"expected": manifest["sha256"], "verified": False}, "liveEligible": False, "coverage": {"books_converted": False, "binance_ticks": False, "rtds_twap": False, "complete_replay": False}}
    product_bytes, rows = None, []
    try:
        import pyarrow.parquet as pq
    except ImportError:
        if args.mode == "trades":
            raise ProbeError("trades needs PyArrow; inspect remains stdlib-only")
        report["pyarrow"] = "unavailable; footer bytes inspected, schema not parsed"
    else:
        parquet = pq.ParquetFile(reader)
        report["schema"] = str(parquet.schema_arrow)
        report["row_groups"] = parquet.metadata.num_row_groups
        for name, product in manifest["products"].items():
            check_product_footer(parquet.metadata, product, name)
        report["manifest_footer_verified"] = True
        if args.mode == "trades":
            product = manifest["products"].get("last_trade_price")
            if product is None:
                raise ProbeError("hour has no trade product")
            first, last = product["row_groups"]
            if last - first + 1 > args.max_row_groups:
                raise ProbeError("trade product exceeds explicit row-group limit")
            product_bytes = reader.range(*product["byte_range"])
            actual = hashlib.sha256(product_bytes).hexdigest()
            if actual != product["sha256"]:
                raise ProbeError("trade product SHA256 mismatch")
            report["trade_product_sha256"] = {"expected": product["sha256"], "actual": actual, "verified": True}
            columns = ["event_type", *product["columns"]]
            table = parquet.read_row_groups(list(range(first, last + 1)), columns=columns)
            all_rows = integer_clocks(table).to_pylist()
            if args.first_market:
                if not all_rows:
                    raise ProbeError("no actual trades from which to choose a market")
                selected_market = market_id(all_rows[0]["market"])
                report["market"] = selected_market
                report["market_selection"] = "first archive trade row, generic pilot, strategy relevance unverified"
            rows = [row for row in all_rows if market_id(row["market"]) == selected_market]
            rows.sort(key=lambda r: (r["timestamp_received"], str(r["source_witness"]), r["sequence"]))
            report["matching_rows"] = len(rows)
            report["saved_rows"] = min(len(rows), args.max_rows)
            report["sample_truncated"] = len(rows) > args.max_rows
            report["clocks"] = {"timestamp": "integer exchange epoch milliseconds, UTC", "timestamp_received": "integer recorder epoch microseconds, UTC", "sequence": "collector-local, not a cross-host order", "arrival_order": "merged approximation, not one socket"}
            for row in rows:
                row["sequence"] = str(row["sequence"])
    report["requests"] = http.requests
    report["bytes_read"] = http.bytes_read
    report["max_bytes"] = args.max_bytes
    report["manifest_sha256"] = hashlib.sha256(raw_manifest).hexdigest()
    report["acquired_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    report["attribution"] = {"collector": "pendulumflow", "licence": "CC BY 4.0", "source": BASE}
    out.mkdir(parents=True, exist_ok=True)
    (out / "manifest.json").write_bytes(raw_manifest)
    (out / "provenance.json").write_text(json.dumps(report, default=json_value, indent=2) + "\n", encoding="utf-8")
    if product_bytes is not None:
        (out / "trades-product-range.bin").write_bytes(product_bytes)
        (out / "trades-sample.json").write_text(json.dumps(rows[:args.max_rows], default=json_value, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"out": str(out), "bytes_read": http.bytes_read, "manifest_footer_verified": report.get("manifest_footer_verified", False), "matching_rows": report.get("matching_rows"), "liveEligible": False}))


if __name__ == "__main__":
    main()
