"""Manual bounded fixture refresh; never runs as a unit test or touches credentials."""
from pathlib import Path
from tools.research.observe_mo_money import PublicHTTP, ACTIVITY_FIELDS, POSITION_FIELDS, _public_row
from tools.research.registry import load_registry
from tools.research.schema import canonical, utc_now


def main():
    output = Path(__file__).resolve().parent / "fixtures"
    output.mkdir(exist_ok=True)
    http = PublicHTTP(max_bytes=200_000, max_requests=8, max_seconds=40)
    for account in load_registry()["accounts"]:
        for endpoint, fields in (("activity", ACTIVITY_FIELDS), ("positions", POSITION_FIELDS)):
            parameters = {"user": account["account_id"], "limit": 2, "offset": 0}
            if endpoint == "activity":
                parameters["excludeDepositsWithdrawals"] = "false"
            else:
                parameters.update({"sizeThreshold": "0", "includeArchived": "true"})
            rows, receipt = http.get(endpoint, parameters)
            fixture = {"schema_version": 1, "fixture_kind": "captured-public-report-only",
                "capture_at_utc": receipt["observed_at_utc"], "source_contract_checked_at": "2026-10-06",
                "source_url": "https://data-api.polymarket.com/" + endpoint, "request": parameters,
                "receipt": receipt, "rows": [_public_row(row, fields) for row in rows],
                "financial_complete": False, "note": "Two-row page, not complete history or canonical balance evidence."}
            name = account["name"] + "-" + endpoint + ".json"
            target = output / name
            with target.open("xb") as stream:
                stream.write(canonical(fixture) + b"\n")
            print(name, len(rows), receipt["response_sha256"])


if __name__ == "__main__":
    main()
