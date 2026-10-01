"""Acquire all seven native CLOB event types for a fixed, bounded market selection."""

import argparse
from collections import Counter
import datetime as dt
import decimal
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

from pendulumflow_probe import (BASE, PRODUCTS, BoundedHTTP, ProbeError, RemoteParquet,
                               check_footer, check_product_footer, digest, integer_clocks,
                               market_id, normalize_manifest, positive_int)

COMMON = {"event_type", "timestamp", "timestamp_received", "sequence", "source_witness", "witness_set", "arrival_skew"}


def uint_string(value, bits=256):
    if isinstance(value, bytes):
        if len(value) == 32:
            value = int.from_bytes(value, "big")
        else:
            value = value.decode("ascii")
    if type(value) is int:
        value = str(value)
    if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]*", value) or not 0 <= int(value) < 2 ** bits:
        raise ProbeError(f"invalid unsigned {bits}-bit value")
    return value


def hour_time(hour):
    if not isinstance(hour, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}", hour):
        raise ProbeError("invalid UTC partition hour")
    return dt.datetime.strptime(hour, "%Y-%m-%dT%H").replace(tzinfo=dt.timezone.utc)


def normalize_spec(raw):
    if not isinstance(raw, dict) or type(raw.get("schemaVersion")) is not int or raw.get("schemaVersion") != 1 or raw.get("source") != "pendulumflow-v3":
        raise ProbeError("invalid acquisition schema/source")
    if not isinstance(raw.get("datasetId"), str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,100}", raw["datasetId"]):
        raise ProbeError("invalid dataset id")
    limits = raw.get("limits")
    if not isinstance(limits, dict):
        raise ProbeError("explicit limits are required")
    for name in ("maxNetworkBytes", "maxSelectedRows", "maxRuntimeSeconds", "maxOutputBytes"):
        positive_int(limits.get(name), name)
    hours = raw.get("hours")
    if not isinstance(hours, list) or not hours:
        raise ProbeError("pinned hours are required")
    hour_names = set()
    for pin in hours:
        if not isinstance(pin, dict):
            raise ProbeError("invalid hour pin")
        hour_time(pin.get("hour"))
        if pin["hour"] in hour_names:
            raise ProbeError("duplicate partition pin")
        hour_names.add(pin["hour"])
        digest(pin.get("manifestSha256"))
        digest(pin.get("fileSha256Expected"))
        if "footerSha256" in pin:
            digest(pin["footerSha256"])
    primary = raw.get("primaryHours")
    if not isinstance(primary, list) or not primary or any(not isinstance(x, str) for x in primary) or len(set(primary)) != len(primary):
        raise ProbeError("explicit primary hours are required")
    for hour in primary:
        instant = hour_time(hour)
        for offset in (-1, 0, 1):
            if (instant + dt.timedelta(hours=offset)).strftime("%Y-%m-%dT%H") not in hour_names:
                raise ProbeError("primary hour requires both adjacent receive partitions")
    markets = raw.get("markets")
    if not isinstance(markets, list) or not markets:
        raise ProbeError("explicit BTC 5m market identities are required")
    seen, token_owner = set(), set()
    for market in markets:
        if not isinstance(market, dict) or market_id(market.get("conditionId")) != market["conditionId"]:
            raise ProbeError("condition id must be canonical lower-case hex")
        if market["conditionId"] in seen:
            raise ProbeError("duplicate market")
        seen.add(market["conditionId"])
        start, end = market.get("windowStart"), market.get("windowEnd")
        positive_int(start, "windowStart seconds")
        positive_int(end, "windowEnd seconds")
        if end - start != 300 or market.get("slug") != f"btc-updown-5m-{start}":
            raise ProbeError("market is not the specified BTC five-minute window")
        if dt.datetime.fromtimestamp(start, dt.timezone.utc).strftime("%Y-%m-%dT%H") not in primary:
            raise ProbeError("market window is outside fixed primary hours")
        projection = (market.get("archiveStartMs"), market.get("archiveEndMs"))
        if any(value is not None for value in projection):
            lower, upper = projection
            positive_int(lower, "archiveStartMs")
            positive_int(upper, "archiveEndMs")
            if not lower <= start * 1000 < end * 1000 <= upper:
                raise ProbeError("inclusive archive projection must cover the full market window")
        tokens = market.get("tokens")
        if not isinstance(tokens, list) or len(tokens) != 2 or any(not isinstance(t, dict) for t in tokens) or {t.get("outcome") for t in tokens} != {"Up", "Down"}:
            raise ProbeError("market needs exact Up/Down token identities")
        for token in tokens:
            value = uint_string(token.get("tokenId"))
            if not isinstance(token.get("tokenId"), str) or value in token_owner:
                raise ProbeError("token identities must be distinct canonical decimal strings")
            token_owner.add(value)
        source = market.get("metadataSource")
        if not isinstance(source, dict) or not isinstance(source.get("url"), str) or not source["url"].startswith("https://") or not isinstance(source.get("path"), str) or not source["path"]:
            raise ProbeError("market identity needs source metadata trace")
        digest(source.get("sha256"))
    selection = raw.get("selection")
    if not isinstance(selection, dict) or selection.get("holdoutEligible") is not False or not isinstance(selection.get("reason"), str) or not selection["reason"]:
        raise ProbeError("this development importer requires explicit non-holdout selection provenance")
    if any("archiveStartMs" in market for market in markets) and not all("archiveStartMs" in market for market in markets):
        raise ProbeError("mixed projected/full partition selections are unsupported")
    return raw


def decimal_value(value):
    if value is None:
        return None
    if not isinstance(value, (decimal.Decimal, str)):
        raise ProbeError("price/size lost its source decimal precision")
    number = decimal.Decimal(value)
    if not number.is_finite():
        raise ProbeError("non-finite decimal")
    return format(number, "f")


def hex_id(value):
    if isinstance(value, bytes):
        value = "0x" + value.hex() if len(value) == 32 else value.decode("ascii")
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value):
        raise ProbeError("invalid native hash")
    return value.lower()


def payload_value(name, value):
    if value is None:
        return None
    if name == "market":
        return market_id(value)
    if name in ("asset_id", "winning_asset_id"):
        return uint_string(value)
    if name == "assets_ids":
        return [uint_string(x) for x in value]
    if name == "transaction_hash":
        return hex_id(value)
    if name == "fee_rate_bps":
        return uint_string(value, 32)
    if name in ("bids", "asks"):
        if not isinstance(value, list):
            raise ProbeError("snapshot depth must be a native list")
        return [{"price": decimal_value(x["price"]), "size": decimal_value(x["size"])} for x in value]
    if name in ("price", "size", "best_bid", "best_ask", "spread", "old_tick_size", "new_tick_size"):
        return decimal_value(value)
    if isinstance(value, (str, int, bool)) or isinstance(value, list) and all(isinstance(x, str) for x in value):
        return value
    raise ProbeError(f"unhandled native payload field {name}")


def canonical_event(row, market, hour, source_ref):
    kind = row.get("event_type")
    if kind not in PRODUCTS:
        raise ProbeError("unhandled event type")
    condition = market_id(row.get("market"))
    if condition != market["conditionId"]:
        raise ProbeError("market mapping changed")
    tokens = {t["tokenId"]: t["outcome"] for t in market["tokens"]}
    token = None if row.get("asset_id") is None else uint_string(row["asset_id"])
    if token is not None and token not in tokens:
        raise ProbeError("event token does not belong to pinned market")
    payload = {name: payload_value(name, value) for name, value in row.items() if name not in COMMON}
    if payload.get("assets_ids") is not None and set(payload["assets_ids"]) != set(tokens):
        raise ProbeError("native asset list disagrees with pinned identity")
    if kind == "new_market":
        if payload.get("slug") is not None and payload["slug"] != market["slug"]:
            raise ProbeError("native slug disagrees with pinned identity")
        if payload.get("assets_ids") is not None and payload.get("outcomes") is not None:
            if len(payload["assets_ids"]) != len(payload["outcomes"]) or any(tokens[t] != o for t, o in zip(payload["assets_ids"], payload["outcomes"])):
                raise ProbeError("native outcome mapping disagrees with pinned identity")
    if kind == "market_resolved" and payload.get("winning_asset_id") is not None:
        winner = payload["winning_asset_id"]
        if winner not in tokens or payload.get("winning_outcome") != tokens[winner]:
            raise ProbeError("native resolution identity disagrees with pinned market")
    event_time = row.get("timestamp")
    received = row.get("timestamp_received")
    if event_time is not None and (type(event_time) is not int or event_time < 0):
        raise ProbeError("invalid exchange millisecond clock")
    if type(received) is not int or received <= 0:
        raise ProbeError("missing native receive microsecond clock")
    partition_start = int(hour_time(hour).timestamp()) * 1_000_000
    if not partition_start <= received < partition_start + 3_600_000_000:
        raise ProbeError("native receive clock disagrees with partition hour")
    sequence = uint_string(row.get("sequence"), 64)
    skew = row.get("arrival_skew")
    if skew is not None and (type(skew) is not int or not 0 <= skew < 2 ** 63):
        raise ProbeError("invalid arrival skew microseconds")
    return {"schemaVersion": 1, "source": "pendulumflow-v3", "eventType": kind,
            "conditionId": condition, "tokenId": token, "outcome": tokens.get(token),
            "eventTimeMs": None if event_time is None else str(event_time), "clockPrecision": "ms",
            "receivedTimeUs": str(received), "sequence": sequence, "partitionHour": hour,
            "eventId": f"{hour}:{kind}:{sequence}", "sourceWitness": row.get("source_witness"),
            "witnessSet": row.get("witness_set"), "arrivalSkewUs": None if skew is None else str(skew),
            "sourceRef": source_ref, "payload": payload}


def candidate_groups(metadata, product, condition_ids):
    binary_targets = [value for condition in condition_ids for value in (bytes.fromhex(condition[2:]), condition.encode("ascii"))]
    selected = []
    for index in range(product["row_groups"][0], product["row_groups"][1] + 1):
        group = metadata.row_group(index)
        stats = None
        for col_index in range(group.num_columns):
            column = group.column(col_index)
            if column.path_in_schema == "market":
                stats = column.statistics
                break
        if stats is None or not stats.has_min_max:
            selected.append(index)
        elif isinstance(stats.min, bytes) and isinstance(stats.max, bytes):
            if any(stats.min <= value <= stats.max for value in binary_targets):
                selected.append(index)
        elif isinstance(stats.min, str) and isinstance(stats.max, str):
            if any(stats.min <= value <= stats.max for value in condition_ids):
                selected.append(index)
        else:
            raise ProbeError("unknown market footer statistics encoding")
    return selected


def projection_decision(row, market):
    """Discard only known-clock non-lifecycle events explicitly outside fixed bounds."""
    timestamp = row.get("timestamp")
    if "archiveStartMs" not in market:
        return True, "full-partition"
    if row.get("event_type") in {"new_market", "market_resolved"}:
        if timestamp is None:
            return True, "lifecycle-unknown-clock-retained"
        if not market["archiveStartMs"] <= timestamp <= market["archiveEndMs"]:
            return True, "lifecycle-outside-projection-retained"
        return True, "inside-projection"
    if timestamp is None:
        return True, "unknown-clock-retained"
    if type(timestamp) is not int or timestamp < 0:
        raise ProbeError("invalid native exchange clock during projection")
    if not market["archiveStartMs"] <= timestamp <= market["archiveEndMs"]:
        return False, "outside-projection-excluded"
    return True, "inside-projection"


class ArchiveHTTP(BoundedHTTP):
    def __init__(self, limit, deadline, out, opener=None):
        self.deadline, self.out = deadline, out
        if opener is None:
            import urllib.request
            opener = urllib.request.urlopen
        def timed_open(request, timeout):
            self.check_deadline()
            return opener(request, timeout=min(timeout, max(0.1, self.deadline - time.monotonic())))
        super().__init__(limit, timed_open)

    def check_deadline(self):
        if time.monotonic() >= self.deadline:
            raise ProbeError("explicit acquisition runtime limit exhausted")

    def fetch(self, url, start=None, end=None, total=None, limit=512_000):
        self.check_deadline()
        data = super().fetch(url, start, end, total, limit)
        key = self.requests[-1]["sha256"]
        path = self.out / "source-ranges" / f"{key}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_bytes(data)
        self.requests[-1]["artifact"] = str(path.relative_to(self.out)).replace("\\", "/")
        self.check_deadline()
        return data


def write_json(path, value):
    temporary = path.with_name(path.name + ".writing")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def verify_identity_sources(spec):
    """Pin local metadata bytes; historical truth is independently reviewed downstream."""
    for market in spec["markets"]:
        source = market["metadataSource"]
        path = Path(source["path"])
        if path.stat().st_size > 1_000_000:
            raise ProbeError("identity metadata artifact exceeds 1 MB")
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest(source["sha256"]):
            raise ProbeError("pinned identity metadata checksum changed")


def acquire(spec, out, opener=None):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    spec = normalize_spec(spec)
    verify_identity_sources(spec)
    limits = spec["limits"]
    out = Path(out).resolve()
    checkout = Path(__file__).resolve().parents[2]
    if out == checkout or checkout in out.parents:
        raise ProbeError("acquisition output must be outside the checkout")
    if out.exists() and any(out.iterdir()):
        raise ProbeError("acquisition output directory is not empty")
    out.mkdir(parents=True, exist_ok=True)
    write_json(out / "selection.json", spec)
    http = ArchiveHTTP(limits["maxNetworkBytes"], time.monotonic() + limits["maxRuntimeSeconds"], out, opener)
    markets = {market["conditionId"]: market for market in spec["markets"]}
    targets = pa.array([value for condition in markets for value in (bytes.fromhex(condition[2:]), condition.encode("ascii"))], type=pa.binary())
    report = {"schemaVersion": 1, "source": "pendulumflow-v3", "datasetId": spec["datasetId"],
              "markets": spec["markets"], "primaryHours": spec["primaryHours"], "hours": [],
              "selection": spec["selection"], "holdoutEligible": False,
              "storageOrder": "partition/product/rowgroup/row", "captureMode": "merged-union",
              "eventFile": "events.partial.jsonl", "eventCount": 0, "status": "incomplete", "liveEligible": False,
              "coverage": {"allSevenTypesHandled": True, "completeSelectedPartitions": False, "completeProjectedSelection": False,
                           "bookParityValidated": False, "binance": False, "rtdsTwap": False},
              "projection": {"enabled": "archiveStartMs" in spec["markets"][0], "clock": "exchange-ms",
                             "bounds": "inclusive", "unknownClockRetained": 0, "lifecycleOutsideRetained": 0,
                             "lifecycleUnknownClockRetained": 0, "excludedByTime": 0},
              "limits": limits, "attribution": {"collector": "pendulumflow", "licence": "CC BY 4.0", "source": BASE}}
    event_hash, output_bytes = hashlib.sha256(), 0
    def checkpoint(final=False):
        report["networkBytes"] = http.bytes_read
        report["outputBytes"] = output_bytes
        report["eventsSha256"] = event_hash.hexdigest()
        report["eventChecksumComplete"] = final
        report["requests"] = http.requests
        report["checkpointAtUtc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        write_json(out / "dataset-manifest.json", report)
    checkpoint()
    next_checkpoint = time.monotonic() + 2
    try:
        with (out / "events.partial.jsonl").open("wb") as output:
            for pin in sorted(spec["hours"], key=lambda item: item["hour"]):
                http.check_deadline()
                hour = pin["hour"]
                base = f"{BASE}/v3/{hour.replace('T', '/')}/"
                request_start = len(http.requests)
                manifest_bytes = http.fetch(base + "manifest.json")
                if hashlib.sha256(manifest_bytes).hexdigest() != digest(pin["manifestSha256"]):
                    raise ProbeError("pinned manifest checksum changed")
                manifest = normalize_manifest(json.loads(manifest_bytes), hour)
                if manifest["sha256"] != digest(pin["fileSha256Expected"]):
                    raise ProbeError("pinned Parquet checksum claim changed")
                if set(manifest["products"]) != PRODUCTS:
                    raise ProbeError("hour is missing a canonical product")
                reader = RemoteParquet(http, base + manifest["file"], manifest["bytes"])
                footer = check_footer(reader)
                if pin.get("footerSha256") and footer["sha256"] != digest(pin["footerSha256"]):
                    raise ProbeError("pinned footer checksum changed")
                parquet = pq.ParquetFile(reader)
                hour_report = {"hour": hour, "manifestSha256": pin["manifestSha256"],
                               "fileSha256Expected": pin["fileSha256Expected"], "footerSha256": footer["sha256"],
                               "manifestFooterVerified": True, "wholeFileChecksumVerified": False,
                               "mergerGitSha": manifest.get("merger_git_sha"), "sequenceNote": manifest.get("sequence_note"),
                               "schema": str(parquet.schema_arrow), "products": {}, "requests": []}
                report["hours"].append(hour_report)
                source_dir = out / "source" / hour
                source_dir.mkdir(parents=True)
                (source_dir / "manifest.json").write_bytes(manifest_bytes)
                identities = set()
                for name in sorted(PRODUCTS):
                    product = manifest["products"][name]
                    check_product_footer(parquet.metadata, product, name)
                    groups = candidate_groups(parquet.metadata, product, markets)
                    product_report = {"rowCount": product["row_count"], "expectedProductSha256": product["sha256"],
                                      "wholeProductChecksumVerified": False, "selectedRowGroups": groups,
                                      "selectedRows": 0, "decodedRows": 0, "matchedMarketRows": 0,
                                      "excludedByTime": 0, "unknownClockRetained": 0,
                                      "lifecycleOutsideRetained": 0, "lifecycleUnknownClockRetained": 0, "marketCounts": {}}
                    hour_report["products"][name] = product_report
                    counts = Counter()
                    columns = ["event_type", *product["columns"]]
                    for group_index in groups:
                        for batch in parquet.iter_batches(batch_size=1024, row_groups=[group_index], columns=columns, use_threads=False):
                            http.check_deadline()
                            product_report["decodedRows"] += batch.num_rows
                            batch = batch.filter(pc.is_in(batch.column(batch.schema.get_field_index("market")), value_set=targets))
                            table = integer_clocks(pa.Table.from_batches([batch]))
                            for row in table.to_pylist():
                                if row["event_type"] != name:
                                    raise ProbeError("row disagrees with native product type")
                                condition = market_id(row["market"])
                                product_report["matchedMarketRows"] += 1
                                retain, reason = projection_decision(row, markets[condition])
                                counters = {"outside-projection-excluded": "excludedByTime",
                                            "unknown-clock-retained": "unknownClockRetained",
                                            "lifecycle-outside-projection-retained": "lifecycleOutsideRetained",
                                            "lifecycle-unknown-clock-retained": "lifecycleUnknownClockRetained"}
                                if reason in counters:
                                    field = counters[reason]
                                    product_report[field] += 1
                                    report["projection"][field] += 1
                                if not retain:
                                    continue
                                event = canonical_event(row, markets[condition], hour, {"manifestSha256": pin["manifestSha256"], "fileSha256Expected": pin["fileSha256Expected"], "rowGroup": group_index})
                                identity = event["sequence"]
                                if identity in identities:
                                    raise ProbeError("duplicate native sequence within partition")
                                identities.add(identity)
                                if report["eventCount"] >= limits["maxSelectedRows"]:
                                    raise ProbeError("explicit selected-row limit exhausted; nothing silently truncated")
                                encoded = (json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
                                if output_bytes + len(encoded) > limits["maxOutputBytes"]:
                                    raise ProbeError("explicit output-byte limit exhausted")
                                output.write(encoded)
                                event_hash.update(encoded)
                                output_bytes += len(encoded)
                                report["eventCount"] += 1
                                product_report["selectedRows"] += 1
                                counts[condition] += 1
                            if time.monotonic() >= next_checkpoint:
                                output.flush()
                                checkpoint()
                                next_checkpoint = time.monotonic() + 2
                    product_report["marketCounts"] = dict(counts)
                    output.flush()
                    checkpoint()
                    http.check_deadline()
                    print(json.dumps({"hour": hour, "product": name, "rowGroups": len(groups), "selectedRows": product_report["selectedRows"], "networkBytes": http.bytes_read}), file=sys.stderr, flush=True)
                hour_report["requests"] = http.requests[request_start:]
                reader.cache.clear()
        if report["eventCount"] == 0:
            raise ProbeError("no selected events acquired")
        http.check_deadline()
        (out / "events.partial.jsonl").rename(out / "events.jsonl")
        report["eventFile"] = "events.jsonl"
        report["status"] = "acquired-unvalidated"
        if report["projection"]["enabled"]:
            report["coverage"]["completeProjectedSelection"] = True
        else:
            report["coverage"]["completeSelectedPartitions"] = True
    except Exception as error:
        report["failure"] = {"type": type(error).__name__, "message": str(error)}
        report["eventFile"] = "events.partial.jsonl"
        raise
    finally:
        report["acquiredAtUtc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        checkpoint(final=True)
    return report


def mark_worker_timeout(out, spec):
    """Called only after the owned worker stopped; checkpoints never imply completion."""
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = out / "dataset-manifest.json"
    report = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {
        "schemaVersion": 1, "source": "pendulumflow-v3", "datasetId": spec["datasetId"],
        "markets": spec["markets"], "selection": spec["selection"], "hours": [],
        "eventCount": 0, "networkBytes": 0, "outputBytes": 0, "limits": spec["limits"], "coverage": {}}
    report.update({"status": "incomplete", "liveEligible": False, "holdoutEligible": False,
                   "hardRuntimeTimedOut": True, "eventChecksumComplete": False,
                   "eventsSha256": None, "countsAreCheckpointed": True,
                   "failure": {"type": "HardRuntimeLimit", "message": "owned CLI worker exceeded explicit wall-clock limit"}})
    report["coverage"].update({"completeSelectedPartitions": False, "completeProjectedSelection": False,
                               "bookParityValidated": False, "binance": False, "rtdsTwap": False})
    partial = out / "events.partial.jsonl"
    report["eventFile"] = "events.partial.jsonl" if partial.exists() else report.get("eventFile")
    report["partialFileBytes"] = partial.stat().st_size if partial.exists() else 0
    report["supervisorStoppedAtUtc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    write_json(manifest_path, report)


def supervise_worker(command, out, spec, popen=None, timeout=None):
    """Enforce the CLI deadline even when DNS, socket reads or Arrow hang."""
    factory = subprocess.Popen if popen is None else popen
    process = factory(command)
    try:
        return process.wait(timeout=spec["limits"]["maxRuntimeSeconds"] if timeout is None else timeout)
    except subprocess.TimeoutExpired:
        # The process handle belongs to this launch. Never scan or kill other Python processes.
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        mark_worker_timeout(out, spec)
        return 124


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--_worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.spec.stat().st_size > 1_000_000:
        raise ProbeError("acquisition specification exceeds 1 MB")
    spec = normalize_spec(json.loads(args.spec.read_text(encoding="utf-8-sig")))
    verify_identity_sources(spec)
    out = args.out.resolve()
    checkout = Path(__file__).resolve().parents[2]
    if out == checkout or checkout in out.parents or out.exists() and any(out.iterdir()):
        raise ProbeError("CLI output must be new or empty and outside the checkout")
    if not args._worker:
        command = [sys.executable, "-B", str(Path(__file__).resolve()), "--spec", str(args.spec.resolve()), "--out", str(out), "--_worker"]
        return supervise_worker(command, out, spec)
    report = acquire(spec, out)
    print(json.dumps({"status": report["status"], "eventCount": report["eventCount"], "networkBytes": report["networkBytes"], "out": str(args.out.resolve()), "liveEligible": False}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
