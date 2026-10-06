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

Two independent native schedules use the complete task prompts in
`research/prompts/automation-analysis.md` (hourly reliability/analysis queue poll)
and `research/prompts/automation-development.md` (review/coding every two hours).
Each pins Astra/max in native settings. Polling cadence is separate from the
controller's six-hour frozen analysis window; an idle hourly run creates no issue
or report. Review may prepare evidence under A0, while coding waits for current
A1 policy and actual sandbox/independent-broker receipts. A second model context
is not a GitHub approval or a self-issued check principal.
The trusted native settings view is sufficient configuration proof; cryptographic
provider attestation is not required. Record available execution metadata honestly
and fail closed on missing settings, unavailable selected model or reported
settings mismatch. A ticket declaration alone is not the native settings view.

The bridge grants 1800-second leases; renew at least every ten minutes and before
external effects. Original tickets/checkpoints remain immutable: save renewal
replies separately and obey current server generation. Preserve analysis/spec
artifact hashes and paths, source bundle/policy hashes, effect identity and exact
confirmed publication receipt in job completion so the other schedule can review
the frozen proposal. Retained local specs must match those hashes. Missing frozen
specs or missing independent receipt means WAITING_DATA/REVIEW, not reconstruction
from mutable issue prose. Missing actual model/sandbox proof means WAITING_RESOURCE.
Unknown publication effects require broker reconciliation; never retry a POST or
mark the job successful while its effect remains unresolved. If the current
bridge cannot perform a needed recovery/read-only operation, retain the waiting
state and exact artifacts for the trusted broker.

`status` returns frozen `confirmed_effects` and `ready_code` registry items.
`effect --effect-id ID` returns the confirmed outbox plus frozen issue-spec/hash,
parent input references/hash and original source policy. `reconcile --effect-id ID`
requires no lease and never POSTs: it checks current exact GitHub marker/body,
including closed issues, confirms the effect and performs a scoped wake only for
its WAITING_ACCESS parent after CONFIRMED. Negative visibility remains waiting.
The independent schedule uses exactly the policy-configured principal
`mm2-independent-review-<nativeAutomationId>` only after its native ID/settings
and separate review context match trusted configuration. Verified receipt flags
record actual broker/spec checks; they are not GitHub required approvals.

Always inspect `status.service_health` before treating an empty queue as a quiet
run. It exposes only allowlisted `unit`, `status` (HEALTHY, RUNNING, FAILED or
UNAVAILABLE), safe unavailable reason, active/sub state, result, exit status,
last-started/exited UTC and fingerprint. An inactive oneshot with success and exit
0 is HEALTHY; inactivity alone is not a failure. Fingerprints exclude timestamps:
notify a new actionable failure signature, changed UNAVAILABLE condition or
recovery, then stay quiet while unchanged. Health is a diagnostic signal, not
permission to restart services, access raw logs/environment or alter trading.

Desktop bundle transport returns each `files[member]` as a list of base64 chunks:
at most 96 KiB raw / 128 KiB ASCII per chunk. MAX_RECORD remains unchanged. Before
writing inbox files, the desktop broker verifies the exact frozen manifest SHA,
chunk count, declared member sizes and total decoded payload at most 4 MiB;
after reconstruction it verifies every member hash and the full bundle again.
Transport framing does not relax accounting completeness or input provenance.

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
`ready-code` binds the CONFIRMED source effect, frozen spec, source/current policy,
exact clean baseline, reviewed paths and a real independent broker receipt.
`enqueue-coding` accepts only that registry item under A1; arbitrary issues/labels
grant no coding capability. Patch broker `preview`, `prepare`, `validate` and
`publish-draft` are described with their exact commands in the development
automation prompt. Candidate code runs only inside its pinned offline container;
dirty owner checkout or advanced baseline pauses preparation without stashing or
resetting anyone's work. Current autonomous patch scope excludes executor,
financial permissions, CI, policy and broker self-modification.

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
