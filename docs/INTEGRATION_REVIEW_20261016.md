# Integration review — October 16, 2026

Ready changes had accumulated in a seven-PR dependency chain while `main`
remained at its October 5 revision. Most PRs were Draft, independent review was
missing, and merged/superseded local checkouts had no lifecycle cleanup. Existing
green checks belonged to older revisions. This review separates stable repairs
from changes to experimental strategy and measurement rules.

## Reviewed disposition

| PR | Reviewed source head | Disposition |
| --- | --- | --- |
| #6 | `cebdc92cbf893adb0eda786637f601af5c4de7fa` | Workflow documentation merged first at `89697ff217fb2d5571f54b468de16211e71cce81`; original exact-head checks passed. |
| #1 | `8946e88f1808aa7eb6e0c23e9f2bfff384624871` | Source approved after fixing orphaned Docker validation workers. |
| #5 | `177c4c221892e8819fdee4e73226c54873cc6618` | Source approved after preventing Reset/Resume from bypassing finalized fee/role mismatches. |
| #7 | `33e900436faa6ed915a50c4ff95bff262c76724e` | Paper accounting, native recovery and source shutdown repairs approved. |
| #9 | `fbad427bf14e3c42cb9f67b4c17551259d5ed65a` | Obsolete full-snapshot handling approved; malformed input and delta/tick regressions remain strict. |
| #8 | `54a811fe17b51386cd38deb9e3bbc771d1e0b83f` | Access helper approved after replacing unbounded exception text with fixed diagnostics. |
| #13 | policy code `c5afa029bc3534ad208ca28a8d7c505788c69320`, followed by this documentation update | Independent review approved the policy helper and all nineteen regressions. |
| #10 | `24a6ec415c1147590ed6aecce3192843609377da` | HOLD / Draft: changes public-paper defaults; economic acceptance is incomplete. |
| #11 | `f541bc425f4ccad9fa99b3ee72bed5beb10753d3` | HOLD / Draft: admission repair depends on the unaccepted #10 experiment. |
| #12 | `f111458074df36e611c6d6e5c3be7bf0856561c3` | HOLD / Draft: experimental paired policy, simulator and research changes remain coupled. Latest capture/checkpoint delta reviewed separately. |

Independent reviewers were `agent:/root/review_console_access`,
`agent:/root/review_fee_recovery` and `agent:/root/review_paper_runtime`.
Their owner-account COMMENT receipts are agent reviews, not human approvals.
Integration order is #1, #5, #7, #9, then independent #8 and #13. Each merge must
recheck current main, the exact head and its incremental diff. Preserve ancestry
with merge commits and retarget children before deleting prerequisite branches.

## Corrected review findings

The patch broker previously stopped its Docker client at a time/output quota
without reliably removing the daemon-side worker. Workers are now created under
a unique ownership label before attachment; cleanup verifies the exact immutable
container ID, removes it and confirms absence. Unproved cleanup cannot pass.
Both real timeout and excessive-output regressions pass without surviving workers.

A finalized fee or maker/taker mismatch could outlive a cleared mutable fault.
Health/readiness, duplicate observations, Reset and Resume now consult the
persistent mismatch itself. The Actor/Journal regression exercises the complete
Reset/Resume/recovery path and an identical valid-fee control without resetting
spend or proofs.

The access helper no longer exposes arbitrary nested exception text in diagnostics.
The integration helper also rejects a newer FAIL/HOLD hidden behind an older PASS,
and accepts agent receipts only from the configured trusted owner publisher.

## Combined local validation

The frozen combined candidate is `2f8f99158f07f090614bcca04f3bb0ceac7e96e2`,
tree `dc9c2f53ae97a62ebe1722506ad28ddd17d07f84`. It includes the six approved
source heads and excludes #10–#12. This report and the validation appendix are
documentation-only additions after that snapshot.

* Rust: 333 workspace tests; formatting, all-target Clippy with warnings denied
  and locked release build passed. Rust inputs are identical between the first
  full-build snapshot and the final policy-helper snapshot.
* Python: 556 cases run, 555 passed; one existing Windows DPAPI check skipped in
  Linux. This includes 128 console, 21 data, 34 latency, 188 script and 185
  research cases, with nineteen integration-policy regressions.
* Node: 55 passed — 6 strategy parity, 19 reference and 30 dashboard tests.
  TypeScript/Vite production build passed.
* Real containers: all 14 stack checks plus both worker-cleanup regressions
  passed. Compose parsed with disposable fixture tokens. The observed graceful
  engine stop was 610 ms; this is one local observation, not a production SLA.
* Browser: wrong-token rejection, owner login, comparison/unknown-data display,
  Resume receipt version 1, Pause receipt version 2 and logout passed against a
  separate deterministic demo. Final engine state was Paused with funded
  eligibility false. Disposable fixture containers and network were removed.

Candidate tests ran from disposable snapshots with no production credentials.
Unit/build commands ran offline in pinned installed images; npm dependencies were
installed separately with lifecycle scripts disabled. Container/browser scenarios
used only generated fixture tokens, local demo data and their own Docker resources.
No production deployment, AWS change, funded order or restart was performed.

Hosted Actions did not execute any test step. GitHub reports failed account
payments or a spending-limit restriction. The owner, after that limitation and
completed local checks were disclosed, instructed this chat to conduct review
here and merge. Record that one-time exception against each exact merged head;
do not reinterpret it as a standing CI waiver or change billing implicitly.

Local production-image validation adapted the build harness: fresh release
binaries were copied into the unchanged previously tested engine runtime image,
and the standard console runtime stage consumed the freshly tested frontend.
An offline stock-image build could not fetch its named Rust toolchain/dependencies;
it is not claimed as a successful fresh stock-Dockerfile build. A reference-test
fixture relocation and Docker Desktop test-network setup were corrected in the
disposable harness, with no repository assertion weakened.

External HTTPS/Telegram acceptance, Windows DPAPI on Windows, selected-host
performance and economic/funded acceptance remain separate gates.

## Experimental evidence gates

#10's reference-direction guard and loss-lock/imbalance-repair defaults require a
frozen accepted policy and an untouched, eligible economic holdout. #11 remains
dependent; it may be extracted only with a separately reviewed stable baseline.

#12's completed reports do not select an eligible strategy. Prior public tapes
retain real gaps, missing native initialization and incomplete recovery evidence;
the strict local recovery certificate was rejected in all five completed cases.
The latest opt-in capture profile and read-only comparison checkpoints add no
demonstrated economic acceptance. Freeze source/profile/plan/sampling bindings
before capture, retain complete native recovery and final outcomes, and obtain
independent accounting for the same period before promotion. Checkpoints preserve
pending effects and reservations but do not prove NAV or an atomic public-chain
cutoff. Controlled tests do not prove prevention of peer-close 1013.

## Recurrence controls and cleanup

`scripts/pr_hygiene.py` and its PR/daily workflow expose missing/cyclic
prerequisites, stacks beyond three open PRs, stale integration work, held work
marked ready and stale review/head/base/check evidence. The helper is read-only;
it neither merges nor deletes. Contributor instructions and the PR template
require independent review and deliberate experimental promotion.

Enable deletion of merged remote branches, then remove only clean worktrees
whose commits are integrated in main and whose ignored files are disposable.
Keep dated evidence directories, dirty local work and all three experimental
branches. The original reliability checkout has uncommitted work and must remain
even after its corrected remote branch is merged. Refresh the active R18 checkout
before deciding whether it is dirty; another chat continues work there.

Private-repository branch protection/rulesets are unavailable on the current plan
(HTTP 403). The new workflow also requires operational Actions. These controls do
not technically prevent an administrator from bypassing the process; restoring
Actions and enabling server-side protection remain explicit account-level work.
