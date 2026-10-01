# mm2 engineering contract

One owner and one account. Default execution is a paused emulator; public feeds are read-only shadow. Live execution requires a separately validated gateway, eligible owner/host and explicit owner authorization. No profit claims from synthetic fills or incomplete historical data.

The deterministic core has no network, UI, wall-clock or database dependencies. Use exact fixed-point quantities. Preserve order identity, partial fills, cancel-in-flight reservations and unknown outcomes. Cancel-before-post replacements must not acquire free queue priority.

The engine owns execution and durable command/audit state. The console owns Telegram, HTTP frontend, historical statistics and SQLite. Console polling, downtime and storage must not be required for engine decisions. All queues, state histories and ingress must be bounded. Do not claim zero telemetry cost; measure and expose sampling/drop behavior.

Secrets belong only in ignored private runtime files. Never log Telegram initData, bot tokens or signing keys. Reject unsupported live mode. Preserve TLS certificate validation.

Validate with Rust tests and Clippy, Python console tests, dashboard build, real container smoke and browser interaction checks. Keep documented checks separate from unverified provider/region behavior.
