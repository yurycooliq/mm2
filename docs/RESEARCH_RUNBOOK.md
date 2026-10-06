# mm2 research and reliability operations

This is the operational implementation of SELF_IMPROVEMENT_PLAN.md. Funded
trading, financial risk limits, approvals, primary retention and automatic merge
are separate gates. Deployment starts the engine paused and preserves its state.

## Hosts and roles

- Trading: `i-053db5508efc00b4a`, eu-west-1. `mm2.service` owns execution.
- Research: `i-030b39845ba1e0f78`, eu-west-1, t3.small, no inbound ports.
- Archive: `mm2-research-archive-i4dob5a52tpp`, private/encrypted/versioned S3.
- Producer role: only `exports/mm2/` put/get/list; conditional writes only.
- Research role: sanitized exports read, `research/` immutable writes, SSM.
- Desktop broker: GitHub credentials stay local; remote controller has none.

The trusted exporter reads cached owner state locally and allowlisted journal
projections under a 5% CPU cap, idle I/O priority and 192 MiB memory cap. It never
modifies primary state. It runs every 30 minutes. Research polls public activity
and positions every 15 minutes; analysis freezes one packet per six-hour window.
Both timers are persistent. Desktop analysis/coding can wait without losing jobs.

Current projection is derived checkpoints/replay input, **not a complete actual
fill/cashflow ledger**. Public owner-wallet observations can include other bots
and manual activity. They are not attributed to mm2 without intent linkage.
Exact profit, comparable capital and outperformance stay unavailable until their
separate accounting, ownership and prospective experiment gates pass.

## Desktop controller

Use bundled Python with `-B` from the repository root. `output/` is ignored.
SSH uses the checked host keys and SSM proxy in `output/deployment-access/ssh-config`.

```powershell
python -B scripts/research_desktop.py status
python -B scripts/research_desktop.py pull --stage reliability
python -B scripts/research_desktop.py pull --stage analysis
python -B scripts/research_desktop.py resume --ticket output/research-inbox/JOB-GENERATION/ticket.json
python -B scripts/research_desktop.py renew --ticket TICKET.json
python -B scripts/research_desktop.py checkpoint --ticket TICKET.json --name analysis --result ANALYSIS.json
python -B scripts/research_desktop.py publish --ticket TICKET.json --spec ISSUE_SPEC.json
python -B scripts/research_desktop.py complete --ticket TICKET.json --status SUCCEEDED --result RESULT.json
```

Read `research/prompts/analyst.md` and contracts before interpreting input. A ticket
declaring a model is not proof of provider execution: native scheduled tasks must
pin `gpt-6-astra/max`, with no fallback. Subscription quota, sleeping desktop or
revoked credentials produce waiting states, not fictitious completed analysis.
Notify only meaningful findings/failures/owner actions; daily reports require a
separate owner request. No model runs on the trading host.

GitHub publication uses an exact immutable marker/body, a durable fenced outbox
and reconciliation. An unknown POST is never blindly retried. Issues require
facts, unknowns, hypothesis/alternative, mechanism, target metric, concrete files,
tests, acceptance, rollout, recovery risk and rollback with member hashes.

## Code proposals

Native Codex is a trusted desktop proposer with owner context; it is not described
as an isolated OS worker. It produces a bounded untrusted patch artifact. The
broker accepts only approved analysis/test/documentation paths; executor, secrets,
CI, policy and broker self-modification are excluded. Candidate code is tested in
an exact offline container with no production/home mounts, credentials or Docker
socket, read-only source, non-root UID and resource caps. Trusted baseline tests
are mounted separately. A real denial probe and exact SHA receipt are mandatory.
The broker can publish a draft PR; automatic merge/deployment are disabled.

## Diagnostics and retention

`scripts/incident_capture.py` is read-only and emits fixed codes, not raw log text,
headers, environment values or private signed intents. Historical tails cannot
prove a root cause or absent crashes. Current container revision is distinct from
unverified historical incident attribution. Restart counts, fatal classes and
source recovery counters are evidence, not a profitability metric.

Retention applies only to explicitly registered disposable diagnostics/cache/build
roots, by exact inventory/checksum plans with surviving receipts and unchanged
pins/leases. Primary journals and financial research bundles are never eligible.
Spool/controller/archive quotas stop ingestion with a visible failure; they do
not permit deleting evidence or resetting equity. Compaction remains a separately
reviewed change before any primary deletion.

## Validation and rollout

Run Rustfmt, Clippy `-D warnings`, locked workspace tests; Python console/data/
latency/ops/research suites, strategy parity and container integration. Verify
CloudFormation with cfn-lint, cfn-guard and pre-deployment validation. Package only
clean exact committed source; images and public code bind that SHA.

Stop owned services, back up retained journals to a **new** path, verify backup,
import exact release, activate preserved public profile and start paused. Do not
run Compose down with volumes, prune, reset state or issue Resume during rollout.
Check HTTPS/Telegram, source diagnostics, zero new restarts and exporter on/off
loopback latency. Loopback latency is not exchange matching latency. Retained
compatible code is the rollback; current financial state must remain mounted.
