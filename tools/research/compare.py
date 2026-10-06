"""Three comparisons with fixed scopes; a dollar gap is never scaled into ROI."""
import argparse
from .archive import publish_bundle
from .schema import VERSION, ResearchError, canonical, digest, integer, load_json, quality


def _comparison(candidate, reference, label, policy):
    reasons = []
    if candidate.get("schema_version") != VERSION or reference.get("schema_version") != VERSION:
        raise ResearchError("unsupported accounting result schema")
    if candidate.get("utc_range") != reference.get("utc_range"):
        reasons.append("DIFFERENT_UTC_WINDOWS")
    if candidate.get("lane") != reference.get("lane"):
        reasons.append("DIFFERENT_EXECUTION_EVIDENCE")
    for item in (candidate, reference):
        if item.get("quality", {}).get("eligibility", {}).get("accounting") is not True:
            reasons.append("ACCOUNTING_NOT_COMPLETE")
    requirements = ("capital_pico", "risk_policy_hash", "market_universe_hash", "fee_policy_hash")
    if not isinstance(policy, dict) or any(policy.get(name) is None for name in requirements):
        reasons.append("COMPARISON_POLICY_UNFROZEN")
    else:
        integer(policy["capital_pico"])
        for item in (candidate, reference):
            if any(item.get("comparison_policy", {}).get(name) != policy[name] for name in requirements):
                reasons.append("INCOMPARABLE_CAPITAL_RISK_OR_UNIVERSE")
    left = integer(candidate.get("net_pnl_pico"), signed=True, nullable=True)
    right = integer(reference.get("net_pnl_pico"), signed=True, nullable=True)
    # A raw dollar gap is useful only for the same interval and evidence lane.
    # Unequal catch-up windows or synthetic-vs-observed ledgers must not receive
    # a numeric headline that could be mistaken for relative performance.
    same_measurement = not set(reasons).intersection({"DIFFERENT_UTC_WINDOWS", "DIFFERENT_EXECUTION_EVIDENCE"})
    difference = str(left - right) if same_measurement and left is not None and right is not None else None
    interval = None
    a, b = candidate.get("net_pnl_interval_pico"), reference.get("net_pnl_interval_pico")
    if same_measurement and isinstance(a, dict) and isinstance(b, dict):
        lo = integer(a.get("lower"), signed=True) - integer(b.get("upper"), signed=True)
        hi = integer(a.get("upper"), signed=True) - integer(b.get("lower"), signed=True)
        interval = {"lower": str(lo), "upper": str(hi)}
    if difference is None:
        reasons.append("EXACT_PROFIT_UNKNOWN")
    # Inferential economics/performance gates are outside this deterministic accounting comparison.
    reasons.append("PROSPECTIVE_STATISTICAL_AND_PERFORMANCE_GATES_REQUIRED")
    return {"kind": label, "candidate_inputs_hash": candidate.get("inputs_hash"), "reference_inputs_hash": reference.get("inputs_hash"),
            "status": "INCONCLUSIVE", "reasons": sorted(set(reasons)), "observed_dollar_difference_pico": difference,
            "difference_interval_pico": interval, "comparable_accounting": len(set(reasons) - {"PROSPECTIVE_STATISTICAL_AND_PERFORMANCE_GATES_REQUIRED"}) == 0,
            "outperformance_claim": False, "promotion_allowed": False}


def build_comparison(mm2, mo_money, baseline=None, *, policy=None, matched_cohorts=None):
    output = {"schema_version": VERSION, "utc_range": mm2["utc_range"],
        "whole_account": _comparison(mm2, mo_money, "whole-account-public-benchmark", policy),
        "matched_cohorts": [], "candidate_vs_baseline": None,
        "quality": quality("PARTIAL", ["ECONOMIC_OUTPERFORMANCE_NOT_ESTABLISHED"]),
        "assumptions": ["Public dollar profit is not scaled linearly to candidate capital.",
                        "Synthetic/accounting results do not establish real execution or profitability."]}
    for cohort in matched_cohorts or []:
        output["matched_cohorts"].append(_comparison(cohort["candidate"], cohort["reference"], "matched-market-cohort", policy))
    if not output["matched_cohorts"]:
        output["matched_cohort_status"] = "NO_VERIFIED_MATCHED_COHORTS"
    if baseline is not None:
        output["candidate_vs_baseline"] = _comparison(mm2, baseline, "candidate-vs-frozen-mm2-baseline", policy)
    else:
        output["baseline_status"] = "FROZEN_BASELINE_NOT_SUPPLIED"
    output["inputs_hash"] = digest({"mm2": mm2, "mo_money": mo_money, "baseline": baseline, "policy": policy,
                                  "matched_cohorts": matched_cohorts})
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", required=True, help="accounting.json")
    parser.add_argument("--reference", required=True, help="mo-money accounting.json")
    parser.add_argument("--baseline", help="frozen mm2 accounting.json")
    parser.add_argument("--policy")
    parser.add_argument("--cohorts", help="list of independently reconciled matched cohort results")
    parser.add_argument("--out", help="publish immutable comparison bundle")
    args = parser.parse_args()
    candidate = load_json(args.candidate)
    result = build_comparison(candidate, load_json(args.reference), load_json(args.baseline) if args.baseline else None,
        policy=load_json(args.policy) if args.policy else None, matched_cohorts=load_json(args.cohorts) if args.cohorts else None)
    if args.out:
        output = publish_bundle(args.out, {"compare.json": result}, scope={"kind": "research-comparison"},
                                utc_range=result["utc_range"], quality=result["quality"], config_hash=result["inputs_hash"])
        print(canonical({"bundle": str(output), "quality": result["quality"]}).decode())
    else:
        print(canonical(result).decode())


if __name__ == "__main__":
    main()
