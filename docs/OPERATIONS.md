# Deployment profiles, recovery and acceptance

The engine always starts paused. Selecting `trading` or `both` selects an execution capability; it does not authorize or submit a funded order. A successful health probe means the process responds. Source readiness, owner/account eligibility, reconciliation and permission to resume are separate checks.

## Choose the startup profile

Run bootstrap once to create private independent engine and console tokens. Preserve the resulting ignored `.env`; do not print `docker compose config` with environment values. The startup wrapper uses `config --quiet` and never edits `.env`.

```sh
sh scripts/bootstrap.sh
python3 scripts/start.py --mode simulation --build
```

On Windows:

```powershell
./scripts/bootstrap.ps1
./scripts/start.ps1 -Mode simulation -Build
```

`start.ps1` accepts `-PythonPath` if Python is not on PATH. `--dry-run` / `-DryRun` prints only the selected config and Docker commands. An already built, reviewed image can be started without `--build`.

| Choice | Packaged config | Execution |
| --- | --- | --- |
| `simulation` | `config/simulation.toml` | Public market observations and local simulated execution. |
| `trading` | `config/trading.toml` | Funded gateway capability, paused until all live gates pass. |
| `both` | `config/both.toml` | One trading account plus an independent simulation lane using shared observations. |
| `shadow` | `config/public.toml` | Public observations and proposals, with no order submission. |
| `demo` | `config/demo.toml` | Offline synthetic emulator for deterministic local checks. |

The selected profile is a process-local `MM2_ENGINE_CONFIG` override. The new simulation/trading/both profiles use separate mode-specific state directories inside the engine data volume. Legacy demo/public both use `/data`; their persisted profile identity prevents silently switching one over the other's journal. `both` additionally retains paper state in `paper/` beneath its mode-specific directory. Trading/both explicitly share `live_state_dir = "/data/account-live"`, so a mode change preserves one account's reservations, spend/loss history and local fence. Do not change the account directory to evade history or run a second copy of the same funded account.

Production uses reviewed immutable image digests, private runtime secrets, a maintained host clock, sufficient disk and external alerting. Keep the owner API on loopback. Put HTTPS and the exact allowed origin/Host boundary in front of the console. Telegram credentials and a Mini App URL need an actual owner-client acceptance test; a dashboard build is not that test. AWS location, network path and eligibility remain host checks.

## Retained execution journal

The command/execution ledger is primary execution state. Console SQLite, Docker output and a monitoring report are secondary and cannot replace it.

The legacy `journal_max_bytes` setting now defines the active segment size. Without an explicit `[journal]` block, total retained capacity is 16 times that size, capped at 1 GiB. Default segment size is 8 MiB, making the default retained limit 128 MiB. `JournalOptions` also bounds sealed segments, owner receipts and execution operation count. Inspect `/v1/state` → `journal` before accepting a long run.

Explicit example:

```toml
[journal]
segment_max_bytes = 8388608
total_max_bytes = 134217728
max_segments = 128
max_commands = 100000
max_execution_operations = 250000
```

The active `commands.jsonl` rotates to `commands-00000000.jsonl`, then successive indexes. `journal-manifest.jsonl` retains an append-only hashed seal for every archived segment. The seal is synced before the active file is renamed, and the directory entry is synced before a new active file can be used. Hash and sequence continuity cover the entire record history. A complete last seal can finish an interrupted rename on startup. Partial records/seals, missing historical archives and mismatched seals refuse startup. An old unsegmented ledger migrates without rewriting its acknowledged contents.

Recovery streams execution transactions in at most 64 KiB per record. The steady-state actor does not retain a duplicate execution tape in RAM. Owner command identities and receipts remain bounded and retained for exact retry behavior. Replay rebuilds execution identity and reservations before another explicit resume.

Rotation does **not** delete or compact audit evidence and does not provide infinite retention. Reaching any retained budget stops further durable work. A separate live ledger has its own configured retained byte bound and full checkpoints; monitor that capacity too. Do not reset a journal to replenish capital, recover capacity or discard uncertain orders. A future compaction scheme needs an independently verifiable state/receipt checkpoint and an archive retention contract first.

Alert at 80% of each budget and forecast consumption from a measured soak. Before the limit, pause, reconcile pending execution, stop the service, take and verify a backup, then adjust the supported capacity deliberately. Retain original data. Live identity binds account/signature/network/protocol/deployment, while validated risk/capacity policy can be adjusted on restart. Do not edit an identity hash or create a new account ledger to bypass reservations, old losses or that deployment protection.

## Stopped-engine backup and restore

`scripts/ledger_ops.py` uses only Python's standard library. It exclusively acquires the same OS fences as the engine: mode-specific `engine.lock`, companion `paper/engine.lock` if present, and the shared account's `live.lock` selected with `--live-state-dir`. A running owner of any selected lane refuses maintenance. Legacy embedded `live/live.lock` backups are also supported. Use an extracted/mounted copy of the correct mode-specific and shared-account directories; console data is a separate secondary volume.

Stop the owning service before exporting a Docker volume (`docker compose stop engine`). Run the tool against the original stopped volume inside the same Linux lock/filesystem domain, or against its stopped exported copy. A Windows lock on an exported path is not a claim that it fences a concurrently running Linux container or another VM. The tool never stops a process on the operator's behalf.

```sh
python3 scripts/ledger_ops.py verify --source /srv/mm2/state
python3 scripts/ledger_ops.py backup --source /srv/mm2/state --destination /srv/mm2/backups/2026-10-05
python3 scripts/ledger_ops.py verify-backup --source /srv/mm2/backups/2026-10-05
python3 scripts/ledger_ops.py restore --source /srv/mm2/backups/2026-10-05 --destination /srv/mm2/restore-test
```

For trading/both, include the shared account sidecar explicitly:

```sh
python3 scripts/ledger_ops.py backup --source /srv/mm2/both \
  --live-state-dir /srv/mm2/account-live --destination /srv/mm2/backups/2026-10-05
python3 scripts/ledger_ops.py verify-backup --source /srv/mm2/backups/2026-10-05
python3 scripts/ledger_ops.py restore --source /srv/mm2/backups/2026-10-05 \
  --destination /srv/mm2/restored-both --live-destination /srv/mm2/restored-account
```

The bundle normalizes shared account objects under `live/` and records `live_layout = "sidecar"` in its v2 backup manifest. Restore requires a separate new `--live-destination`; it does not hide an account ledger inside the mode-specific directory. A profile declaring a shared account location refuses verify/backup without `--live-state-dir`. Embedded and shared account sources cannot be mixed. V1 embedded backups remain readable.

The destination must be new, outside the source and inside an existing resolved parent. Existing files are never replaced. No recursive delete or destructive switch is performed. The tool copies only the recognized command/execution segments, rotation manifest, execution profile, optional companion paper ledger and optional live checkpoint/identity files. Unknown state files refuse backup, requiring the schema to be updated before evidence can be omitted. Lock files are created locally, rather than copied as locks.

Each backup file has a SHA-256 and byte count in `backup-manifest.json`; replay sequence, record hashes, receipt identities and seal hashes are independently checked. A failed backup leaves an `INCOMPLETE` marker and preserves the partial directory for diagnosis. A restore rechecks every copied object against its recorded checksum. An interrupted restore can leave a partial new directory; preserve it and use a different new destination for the next attempt.

The offline validator proves recorded integrity/order and receipt structure. It does not authenticate venue state, validate every ledger-domain invariant or prove the archive was copied off-host. Verify restored directories with the actual Rust engine while paused and isolated. Preserve profile bindings by mounting the new restored copies at the originally configured container paths; do not alter the persisted profile/account identity to make a test pass. Check the original IDs, cumulative fills, cash, inventory and unknown/cancelling reservations; confirm known owner command retries return the original receipts. A live startup still requires venue reconciliation. Restore-testing funded credentials must never resume or submit orders.

Keep at least one verified backup outside the engine host and restrict archive access. Keys belong in a separately controlled secret recovery process and are deliberately absent from ledger backups. Checksum integrity is not an authenticity proof against an attacker who can replace both data and manifests.

## Read-only soak acceptance

`scripts/soak.py` polls only loopback `/healthz` and `/v1/state`. It does not pause, resume, inject faults, change limits or send orders. It refuses redirects, remote URLs and proxy routing. The engine API token is read from a named environment variable, never argv or the report. Do not paste a token into a shell command saved in history.

```sh
python3 scripts/soak.py --seconds 86400 --interval 5 \
  --expect-mode simulation --require-feed-ready \
  --disk-path /srv/mm2 --output /srv/mm2/reports/acceptance-2026-10-05.jsonl
```

Provision `MM2_ENGINE_API_TOKEN` through the host's private environment before running the command. The output file must be new. The monitor bounds duration (at most 7 days), interval, HTTP body, timeout, recent latency samples and output bytes. It records snapshot age, clock skew, disk free space, engine faults, observed restarts and retained-journal budget thresholds. For offline `demo`, use `--expect-mode emulator` and omit `--require-feed-ready` if validating a deliberately paused process. `/healthz` alone is insufficient for market-source acceptance.

Use `--disk-path` on the actual filesystem containing the engine volume. Measuring the repository filesystem cannot prove the data volume has space. In `both`, the monitor additionally checks companion `parallel_simulation.journal` budgets and `live.journal_bytes` / `live.journal_max_bytes` when the engine publishes them; inspect any unavailable lane metric before accepting that lane. Poll latency percentiles describe owner API responsiveness; the core timing percentiles supplied by the engine cover their documented core scope. Neither measures order arrival at the exchange.

Before accepting a deployment:

1. Run the reviewed emulator fixtures, Rust checks, console checks, dashboard build and container integration checks.
2. Exercise backup/restore into a new directory, then compare replayed state and command receipts while paused.
3. Run a bounded emulator acceptance, followed by a public simulation soak on the selected host. Measure retained-byte growth, p95/p99, source disconnects and clock/disk behavior with the console on and off.
4. In separate controlled tests, stop the console, interrupt market feeds, delay acknowledgements, restart the engine and verify every uncertain reservation remains conserved. Record which cases were emulator-injected and which used a real network.
5. Confirm HTTPS/owner Telegram access, external host/process/disk alerts, restart recovery and rollback to a reviewed version that understands the persisted journal schema.
6. For funded operation, confirm protocol/account identity, eligibility, capital and exposure limits, market allowlist, cancel protection and actual venue reconciliation first. A successful simulation soak is not funded-order authorization or profitability evidence.

No selected-host 24-hour soak, real Telegram delivery or funded canary is claimed by merely adding these scripts.
