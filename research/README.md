# Research controller operations

The research service is separate from `mm2.service`. It owns a local WAL SQLite
registry and verified immutable bundles; the trading engine has no dependency on
it. Initial policy enables A0 collection/analysis/reliability and disposable
secondary cleanup. It grants neither funded trading nor automatic merge.

Use the controller on the live analysis host, never a copied DB as another
writer. Server collection has no GitHub credentials. Publication is a trusted
desktop broker operation; workers receive only sanitized bundle copies/tickets.
The owner desktop is not an enforced A1 coding sandbox.
The available A1 path treats native AI as a patch proposer: the controller's
ready-code registry requires an independently verified exact issue/spec review,
and the trusted patch broker executes candidate code only in locked offline
Docker and publishes a draft PR with automatic merge disabled. Enable that path
only after actual container denial checks succeed.

```sh
python -m tools.research.controller --db /var/lib/mm2-research/jobs.sqlite3 --policy research/policies/autonomy.json enqueue --stage analysis --bundle /var/lib/mm2-research/archive/<bundle-id>
python -m tools.research.controller --db /var/lib/mm2-research/jobs.sqlite3 --policy research/policies/autonomy.json claim --stage analysis --owner desktop --run-id <unique-run-id> --lease-seconds 900
python -m tools.research.controller --db /var/lib/mm2-research/jobs.sqlite3 status
```

Save the returned ticket outside Git. Renew it during long analysis. Enqueue
keys include full input/policy hashes and candidate SHA. For six-hour windows,
freeze the selected bundle set once; changing a bundle list is a new input.
Overlapping claims cannot run one stage twice. Expired workers cannot checkpoint,
complete or dispatch. Attempts are finite and failures/unknown effects retained.

`draft --ticket <file> --spec <file>` verifies the complete issue contract and
every manifest/member evidence hash, then durably prepares the exact GitHub
request. `publish` can run only where the trusted broker controls the registry
and GitHub credentials. For a remote registry, the trusted desktop broker uses
`effect`, `dispatch`, `unknown` and `confirm` over a restricted controller
transport: dispatch must succeed before one POST; response is verified against
repository, marker and exact request/body hash. Never copy credentials to the
server or mount the controller DB in an untrusted worker.

An interrupted/unknown POST remains `DISPATCHING` or `UNKNOWN`. Reconcile both
open and closed remote issues by exact marker/body. Negative listing visibility
does not prove absence and never permits another POST. After verified recovery,
`wake --job-id <id> --prerequisite remote-effect-confirmed` resumes a waiting job.
Same mechanism/epoch fingerprint cannot create duplicate issues. Changed
evidence for an existing fingerprint is held for a reviewed update rather than
silently replacing the original request or opening a new issue.

`ready-code` requires a CONFIRMED source outbox, its immutable validated issue
specification, verified input manifests, exact baseline SHA and allowed file
scope. A trusted independent review binds those fields, original/current policy
hashes and actual selected model settings. `enqueue-coding --ready-id <hash>`
accepts only that registry item under activated A1 policy. GitHub labels or an
arbitrary issue number cannot issue a coding capability. The patch broker's
`preview`, `prepare`, `validate` and `publish-draft` preserve exact patch/head
receipts and a durable PR outbox; uncertain POST results are reconciled, never
blindly repeated. Frozen offline tests complement independent review and do not
establish profitability or authorize a release.

Experiment APIs freeze candidate/baseline SHAs, data/evaluator/emulator/policy,
chronology, embargo, carryover, family trial budget and independent principals.
Holdout is consumed at start; FAIL/INCONCLUSIVE remain registered. Forward is
preregistered before the future window starts and scored only with independently
verified fresh observation bundles. All PASS receipts are exact candidate SHA;
model/self-declared `broker_verified` fields are accepted only from the trusted
broker channel, not an untrusted worker or issue. A PASS does not authorize
promotion or funded effects.

Retention accepts only dedicated `diagnostics`, `secondary-cache`, `build-cache`
or `exports-cache` roots with disposable identity markers. At least thirty days
are preserved. Use `init-secondary-root`, `retention-preview` and exact
`retention-apply --approved-plan-id <hash>` with current pins/lease paths. Plans
and receipts are outside deleted trees; interrupted apply is recoverable.
Financial bundles, manifests, databases, private files and primary journals are
never eligible. Archive financial evidence has no automatic expiry.

Model receipts currently record controller-declared `gpt-6-astra/max` settings.
Confirm actual native run settings separately; these records are not provider
attestation. Missing quota/access/model/sandbox evidence preserves a waiting
state. Notifications are limited to material findings, failures, completion or
required owner action. Detailed daily reports require an explicit user request.
