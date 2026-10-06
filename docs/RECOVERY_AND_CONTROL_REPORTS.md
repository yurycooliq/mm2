# Reliability and control-report recovery

Owner-directed candidate based on `66ec7df`. This is separate from autonomous
coding leases: it does not self-issue READY, independent review, merge permission
or a funded authorization. Keep the original checkout clean for existing jobs.

## Changes

- Backfilled diagnostic windows retain their own sanitized lifecycle context.
  Missing old counters/capture timestamps remain unknown. Current metadata does
  not identify the revision or cause of historical incidents.
- Future engine diagnostics distinguish `feed_queue_overflow` from
  `feed_consumer_closed`. Old `feed_queue_unavailable` rows remain ambiguous.
  Whole-frame reservation, fault latching, queue capacities and stopping behavior
  are unchanged. This exposes evidence for a subsequent throughput/root-cause fix;
  it does not claim to have eliminated the observed production restarts.
- Accounting exposes observed cost-bound subtotal separately. A complete declared
  bound requires complete source-window evidence and an explicit bound (including
  true zero) on every reconciled event. Legacy absent declarations and non-trade
  cost applicability remain unknown. Actual-cost and fee subtotals likewise do
  not become complete totals from an incomplete source. Cash/NAV computation is
  unchanged: fee-inclusive effects are never charged twice, and missing cost
  decomposition does not override otherwise valid NAV accounting.
- A signed Telegram launch can reuse only its original live digest-bound cookie
  after launch-data expiry. Signature, exact owner, future-clock and origin checks
  remain required. Server TTL is not extended; expired sessions/restarts require
  fresh launch data. Unauthorized clients still receive neutral 404 responses.
  The panel explains recovery and preserves pending command identity.
- A secondary control-report CLI renders JSON, full Markdown and compact Telegram
  text from one verified research packet. It cannot promote public-wallet results
  into bot-only live profit. Legacy zero bounds, diagnostic lines and CONNECTED
  snapshots do not become complete costs, crash counts or uptime.

## Generate a report without sending a message

From the candidate repository, with Python 3.13 and timezone data installed:

```powershell
python -B -m tools.research.control_report --bundle VERIFIED_PACKET --cutoff-utc 2026-10-07T00:30:00Z --out output/control-report-NEW
```

The output directory must be new. `manifest.json` is written last, with view
hashes. Partial directories are not complete reports. No Telegram API call,
execution command or primary-state mutation is performed. An optional controller
snapshot has shape `{captured_at_utc, status}`; its observation cannot postdate
cutoff. This is a timestamped service observation, not a daily uptime measure.
Optional `--engine-snapshot` and `--diagnostic-snapshot` inputs preserve original
observation timestamps. Allowlisted recent incidents are grouped as rows, never
crash episodes. Fatal feed faults and a blocked server connection become explicit
operational priorities in both views. Server eligibility is distinct from owner
jurisdiction and does not introduce a laptop-IP gate.

The public research packet does not yet contain a full authenticated MM2 ledger.
Connecting bounded intent-linked receipts, opening/closing state, external flows,
valuation and coverage remains a separate producer change. Reports explicitly
state those gaps; they do not manufacture metrics. Report collection/generation
is secondary and never a dependency of engine decisions.

## Acceptance and release

Run Telegram auth/HTTP regressions, frontend tests/build, accounting/backfill and
report tests. Run the pinned offline Linux suite and real denial probes; preserve
exact source hashes and distinguish unchanged trusted baseline tests from new
candidate regressions. Windows directory-publication errors are an unresolved
platform issue until separately explained; do not hide them by swallowing errors.

Independent exact-candidate review, Linux integration and real Telegram Desktop
long-session acceptance are release gates. Preserve old deployment/state on
rollout, start paused and keep pending commands. This change neither relaxes host
eligibility nor requests funded execution.

## AWS access

Human login restores access temporarily. Existing EC2 roles do not renew the
Windows worker's credentials. Machine access needs a separate scoped identity,
certificate/trust enrollment and renewal/revocation probes. No root access keys
are created. A renewable identity does not keep a sleeping desktop running.
Keep machine transport identity and native worker availability as separate
reported health signals. Never Resume execution or repeat unknown external
effects when access returns.
