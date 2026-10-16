"""Financial continuity and eligibility. Ambiguous evidence never grants ROI."""
import argparse
from collections import Counter
from .schema import ResearchError, address, canonical, digest, integer, load_json, quality, utc, validate_event, validate_snapshot


def reconcile_events(events, account_id, utc_range=None):
    account_id = address(account_id)
    if not isinstance(events, list) or len(events) > 100_000:
        raise ResearchError("event input quota exceeded")
    unique, seen, duplicate_count, reasons, conflicted = [], {}, 0, [], False
    for raw in events:
        event = validate_event(raw)
        if address(event["account_id"]) != account_id:
            raise ResearchError("mixed accounts in financial input")
        if utc_range and event["event_time"] is not None:
            stamp = utc(event["event_time"])
            if not utc(utc_range["start"]) <= stamp < utc(utc_range["end"]):
                raise ResearchError("event outside declared UTC window")
        identity = event["event_id"]
        # A repeated GET has a new observation/availability/receipt timestamp.
        # Compare economic content, retaining the earlier actual observation as evidence.
        content = {key: value for key, value in event.items() if key not in {"observed_at_utc", "available_at", "source"}}
        content["source"] = {key: value for key, value in event["source"].items() if key != "request_receipt_hash"}
        fingerprint = digest(content)
        if identity in seen:
            duplicate_count += 1
            previous_hash, index = seen[identity]
            if previous_hash != fingerprint:
                conflicted = True
                reasons.append("CONFLICTING_EVENT_ID")
            elif utc(event["observed_at_utc"]) < utc(unique[index]["observed_at_utc"]):
                unique[index] = event
            continue
        seen[identity] = (fingerprint, len(unique))
        unique.append(event)
        q = event.get("quality", {})
        if q.get("status") != "COMPLETE" or q.get("eligibility", {}).get("accounting") is not True:
            reasons.extend(q.get("reasons") or ["EVENT_ACCOUNTING_UNVERIFIED"])
        if event["evidence_class"] in {"public-observation", "inferred"}:
            reasons.append("EVENT_ACCOUNTING_AUTHORITY_UNPROVEN")
        if event["event_time"] is None:
            reasons.append("FINANCIAL_OCCURRED_TIME_UNKNOWN")
        if event.get("effects", {}).get("cash_delta_pico") is None:
            reasons.append("UNKNOWN_CASH_EFFECT")
        if event.get("effects", {}).get("external_contribution_pico") is None:
            reasons.append("UNKNOWN_EXTERNAL_CONTRIBUTION")
    # Occurred order is diagnostic only: ties are retained, never made into FIFO proof.
    unique.sort(key=lambda event: (event["event_time"] or event["observed_at_utc"], event["event_id"]))
    return {"events": unique, "duplicates": duplicate_count,
            "quality": quality("CONFLICTED" if conflicted else "PARTIAL" if reasons else "COMPLETE", reasons,
                               accounting=not reasons and not conflicted)}


def compare_positions(expected, observed):
    differences = []
    all_assets = set(expected) | set(observed)
    for asset in sorted(all_assets):
        left, right = expected.get(asset, 0), observed.get(asset, 0)
        if left != right:
            differences.append({"asset_id": asset, "expected_quantity_micros": str(left),
                                "observed_quantity_micros": str(right), "difference_micros": str(right - left)})
    return differences


def journal_coverage_gate(coverage):
    """Accept explicit producer field coverage, never infer facts from exact integers."""
    if coverage is None:
        return quality("PARTIAL", ["JOURNAL_COVERAGE_NOT_SUPPLIED"])
    if not isinstance(coverage, dict):
        raise ResearchError("journal coverage must be an object")
    reasons = list(coverage.get("quality_reasons", []))
    if coverage.get("financial_effects_complete") is not True:
        reasons.append("JOURNAL_FINANCIAL_COMPLETENESS_UNPROVEN")
    if coverage.get("financial_effects_complete") is False:
        reasons.append("JOURNAL_PROJECTION_NOT_A_COMPLETE_ACTUAL_LEDGER")
    if coverage.get("committed_continuity") is not True:
        reasons.append("JOURNAL_COMMITTED_CONTINUITY_UNPROVEN")
    fields = coverage.get("facts", {})
    for name in ("cash_effects", "position_effects", "external_cashflows", "actual_fees"):
        if fields.get(name) not in {"observed", "authoritative"}:
            reasons.append("JOURNAL_FACT_UNAVAILABLE:" + name)
    if fields.get("fill_cost_basis") not in {"observed", "authoritative"}:
        reasons.append("ACTUAL_FILL_BASIS_UNAVAILABLE")
    return quality("PARTIAL" if reasons else "COMPLETE", reasons, accounting=not reasons)


def projection_coverage(path):
    """Bridge the independent safe exporter; never invent exact fills from checkpoints."""
    from .journal_export import verify_bundle
    manifest = verify_bundle(path)
    return {"schema_version": 1, "projection_publication_id": manifest["publication_id"],
        "committed_continuity": True, "financial_effects_complete": False,
        "facts": {"cash_effects": "bound", "position_effects": "derived", "external_cashflows": "unavailable",
                  "actual_fees": "unavailable", "fill_cost_basis": "bound", "fill_timestamps": "unavailable"},
        "quality_reasons": list(manifest.get("quality_reasons", []))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--coverage")
    inputs.add_argument("--journal-projection")
    args = parser.parse_args()
    coverage = projection_coverage(args.journal_projection) if args.journal_projection else load_json(args.coverage)
    print(canonical({"coverage": coverage, "quality": journal_coverage_gate(coverage)}).decode())


if __name__ == "__main__":
    main()
