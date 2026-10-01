# Component boundaries

`mm2-core`: exact deterministic order identities, cash reservations, inventory, delayed notifications, cancel-before-post and conservative risk.

`mm2-emulator`: deterministic event-time venue with post/cancel arrival delay, acknowledgement/fill delivery delay, price-time queue approximation, trade-volume conservation, faults and serializable recovery. The engine agent and emulator agent agree their public interface before integrating. No exact venue FIFO or attainable-return claim.

`mm2-engine`: retain and adapt tested owner API/auth/command journal and public feed validation from the previous standalone Rust package. Demo mode drives the emulator and exposes a clearly labelled `simulation` object in `/v1/state`. Public mode remains unsubmitted read-only shadow. Owner controls use durable idempotent CAS receipts. Add execution action audit/recovery and keep telemetry publication bounded/coalesced.

`apps/console`: separate Python stdlib process. Poll private engine state at bounded low frequency, persist secondary SQLite samples/logs with retention, serve the compiled dashboard, owner authentication and proxy commands. Browser never receives the internal engine token. Optional Telegram polling starts only with configured owner and token; owner commands/status use the same API. There must be no external Telegram message during development tests.

Console API: `GET /healthz`, `POST /api/auth/local` with `{token}` (separate console owner token), `POST /api/auth/telegram` with `{init_data}`, authenticated `GET /api/state`, `GET /api/history?limit=120`, `GET /api/logs?limit=100`, `POST /api/control`, `PUT /api/limits`, `POST /api/logout`. Session via HttpOnly SameSite cookie; same-origin unsafe requests require Origin verification. Engine proxy preserves command identity and expected_version. State envelope `{state, observed_at_ms, connected, error}`; history `{samples: [{observed_at_ms, state}]}`; logs `{logs: [{id, observed_at_ms, level, message}]}`. Offline/no state must be explicit, never fake metrics. Static React+Vite UI can use browser polling at 1 Hz; only the console's single sampler polls the engine independently of browser count.

All balances in demo are simulated. Real account fields remain unknown in public shadow. Readiness and mode must be visible. Journal actions are exact audit; sampled console logs and charts are secondary, may lag and are not the trading ledger.
