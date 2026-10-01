# Owner console

The console is a separate Python standard-library process. It samples the private engine state once per second and stores secondary history in its own SQLite volume. Opening more browsers does not add engine samplers. Engine decisions, durable commands and the exchange emulator do not depend on the dashboard or this database.

The dashboard displays engine/API/feed readiness, actual book receive and exchange ages, explicit source or emulator errors, simulated cash/reservations/inventory/orders, limits, receipts and sampled history. Public shadow displays unknown real account funds and inventory. There is no PnL estimate. The chart is sampled simulated available cash, not profit. An emulator rollover with unsettled inventory keeps the original ledger and shows its stop reason.

## Runtime configuration

| Variable | Meaning/default |
| --- | --- |
| `MM2_ENGINE_URL` | Private HTTP(S) engine origin; Compose uses `http://engine:8088`. No credentials, path, query or fragment. |
| `MM2_ENGINE_API_TOKEN` | Required private engine token, 32–256 printable ASCII characters. Never sent to the browser. |
| `MM2_CONSOLE_TOKEN` | Separate owner token with the same length bound. Must differ from the engine token. |
| `MM2_ALLOWED_ORIGINS` | Comma-separated exact console origins. Local defaults: `http://127.0.0.1:8080,http://localhost:8080`. |
| `MM2_CONSOLE_HOST` / `MM2_CONSOLE_PORT` | Default `0.0.0.0` / `8080`; bind/publish locally as configured by Compose. |
| `MM2_CONSOLE_STATE_DIR` | Default `/data`, the console's own persistent volume. |
| `MM2_CONSOLE_STATIC_DIR` | Default `/app/dashboard`, compiled frontend files. |
| `MM2_TELEGRAM_BOT_TOKEN` / `MM2_TELEGRAM_OWNER_ID` | Both absent disables Telegram completely; both supplied enables the allowlisted owner bot. |
| `MM2_MINIAPP_URL` | Optional HTTPS console URL, whose origin must also be explicitly allowlisted. |

If the console owner token is empty, startup generates a random token once and stores it in the private state directory as `console-owner-token`, with restrictive file permissions. Obtain it from that private runtime file; it is never written to logs. Do not place credentials in source, URLs or screenshots. Keep the engine token server-side, including behind a reverse proxy. Configure the public HTTPS console origin explicitly and preserve the matching Host header. Arbitrary API/provider destinations are not accepted.

The host entry point is `python -B -m mm2_console`, with `PYTHONPATH=apps/console`, the runtime environment above and a built dashboard. `npm ci`, `npm test` and `npm run build` run in `apps/dashboard`; the Docker build supplies its static `dist` directory.

## Authentication and control

Local login accepts only the separate console owner token. The authenticated session uses a random HttpOnly cookie, expires after one hour and is held in a bounded in-memory set of 32 sessions. Logout revokes it. Every unsafe HTTP request requires an exact allowlisted Origin and matching Host; cookies use SameSite Strict locally. Telegram HTTPS sessions use Secure/SameSite None to support the Mini App frame. Session cookies and credentials are omitted from access logs.

The console validates Telegram's signed `initData` using the documented HMAC scheme, restricts the signed user ID to the owner, rejects duplicates and replayed initData, and checks a five-minute TTL with at most five seconds of future clock skew. See [Telegram Mini App validation](https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app). The frontend loads the official SDK only for a Telegram Mini App launch. WebView cookie restrictions can depend on the Telegram client; actual Telegram delivery and client embedding require runtime credentials and were not exercised during development.

Browser commands retain the original command ID, expected engine version and exact payload in origin-local `localStorage` **before** dispatch. A reload or closed/reopened Mini App restores an unresolved request. No token or cookie is stored there. An uncertain transport result, timeout, 5xx, mismatched/missing receipt, or 401/403/429 rejection keeps that identity. Reauthenticate or wait for the rate limit, then retry the original request. A matching durable receipt settles it; a definitive engine rejection such as a 409 also settles it and restores controls. Deleting browser storage loses that recovery record, so do not clear it to bypass an unknown outcome. Use one owner control surface at a time.

Pause/Resume/Kill/Reset and limits use the engine's bounded API with unchanged IDs and compare-and-swap versions. Kill and Reset have explicit UI confirmation. The UI labels success only after a matching receipt with a valid applied version and mode. An HTTP response or a button click alone is not a durable acknowledgement. A pending limits request displays its original values as well as its ID/version.

The owner bot accepts private-chat `/start`, `/status`, `/pause`, `/resume`, `/kill`, `/reset` and `/logs`, with sender and chat ID both matching the owner. Controls require a fresh message and a fresh sampled engine publication. The bot persists command identity in SQLite before dispatch and keeps uncertain commands pending across restart; repeating the same command reconciles the same payload. Different controls are blocked while it is unresolved. `/start` includes the configured HTTPS Mini App button. Updates use bounded [Telegram long polling](https://core.telegram.org/bots/api#getupdates). Bot credentials are empty by default; development tests use mock transports and send no external messages.

## API and bounds

| Console route | Behavior |
| --- | --- |
| `GET /healthz` | Process health; does not poll the engine. |
| `POST /api/auth/local` | `{token}` creates the owner cookie. |
| `POST /api/auth/telegram` | `{init_data}` validates the signed owner launch. |
| `GET /api/state` | Cached `{state, observed_at_ms, connected, error, telemetry}`. |
| `GET /api/history?limit=120` | Up to 120 secondary samples. |
| `GET /api/logs?limit=100` | Up to 100 secondary logs. |
| `POST /api/control` | Exact `{id, expected_version, action}` proxy. |
| `PUT /api/limits` | Exact `{id, expected_version, limits}` proxy. |
| `POST /api/logout` | Revoke the session. |

Authenticated reads use cached state or SQLite; they never make private engine requests. The single sampler's internal engine request has a 1.5-second transport timeout. No fresh sampler receipt for 3.5 seconds, an engine publication older than three seconds, or an invalid publication clock makes `connected=false` and disables dashboard controls. The last observed statistics remain visible with a stale/error label. An HTTP 200 from a stalled cached engine state does not establish freshness.

Ingress is bounded to 16 HTTP workers, a three-second socket read timeout, 16 KiB request bodies, 16 KiB accepted headers, 2 KiB paths, and bounded rate-limit tables. Authentication and commands have separate rate limits. Engine transport concurrency is four and response size is capped at 256 KiB; redirects are rejected and HTTPS certificate validation remains enabled. Static files are capped at 2 MiB and must resolve inside the compiled dashboard directory. Security headers include a restrictive Content Security Policy, no-store and nosniff.

SQLite retains at most 900 samples and 1,000 logs, with a 24-hour age policy on writes. Each sample is capped at 64 KiB; log messages at 256 characters. The database page limit is 64 MiB; WAL/checkpoints are separately bounded by short transactions and configured checkpoint/journal limits. Storage failures or oversized samples drop **secondary** writes and increase visible counters. This database is not a replacement for the engine ledger. The state envelope reports history/log drops, sampler overruns, publication lag and storage errors.

## Verification

The Python suite has 16 passing tests for authentication/CSRF/session replay and bounds, secret separation, single sampling, stale publications, retention/restart, transport identity/CAS, and mocked Telegram receipt/pending recovery. Four Node regressions check persisted command identity, uncertain/rejected outcomes, receipt validation and exact limits. TypeScript and the Vite production build pass.

Fixture-only browser checks exercised all four controls, confirmation dialogs, limits, an uncertain command, reload, close/reopen, and a subsequent definitive 409. The integrated Docker console was authenticated against the real local emulator engine with fake QA credentials; its retained account values, source ages and rollover error were verified on screen. Desktop/mobile concepts and final integrated screenshots, plus the visual comparison ledger, are retained in `docs/design` and `DASHBOARD_DESIGN.md`. These checks do not establish live venue execution, real funds, external Telegram delivery or profitability.
