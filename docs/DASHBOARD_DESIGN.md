# Operator console design

The desktop and mobile concepts were generated with the built-in Image Gen tool before implementation and are retained in `design/desktop-concept.png` and `design/mobile-concept.png`. They are specifications, not app backgrounds. All visible controls, text, numbers and charts are HTML/CSS/SVG.

The complete surface is: owner login, header, engine overview and controls, explicit connectivity/readiness, selected market, simulated balances, orders, risk limits, durable command receipt, recent secondary logs, sampled history, and offline/error states. No real balance or PnL is inferred. The concept's $1,000 values illustrate demo layout; the application displays only API values, or unknown placeholders.

Tokens: background `#101411`, surface `#141a16`, text `#e7ede6`, muted `#8a9b8b`, divider `#28352b`, accent `#b7d5a3`, amber `#d8ae59`, danger `#ba785d`. Typography uses a local system sans-serif stack for labels and headings, system monospace for exact numbers, tables and receipts. UI controls are 14px, body 15px, section titles 20px, desktop title 32px, metrics 28px. Four-pixel corners, thin borders, 32px desktop gutters, 20px mobile gutters, no gradients or decorative images.

Desktop has open horizontal metric bands and a 65/35 main split. Orders and activity occupy the left column; limits and receipt occupy the right. Mobile collapses to overview, controls, market, two-column metrics, orders, limits, receipt, activity, history. Tables may scroll inside their own region, never the page. Buttons have at least 44px touch height. Status dots are simple filled semantic circles, not decorative icons.

Locked default copy: mm2; Operator console; Owner session; Sign out; Engine overview; Single sampler · 1 Hz; Resume; Pause; Kill; Reset; LIVE DISABLED; SIMULATED CASH; RESERVED; AVAILABLE; UP / DOWN INVENTORY; Orders; No simulated orders; Risk limits; Initial collateral USD; Max imbalance shares; Lot shares; Half width; Max feed age ms; Apply limits; Command receipt; No command submitted; Recent activity; Sample history; Simulated available cash; Secondary telemetry · not the trading ledger; Engine decisions do not depend on this console. Mode, selected market, feed status, values and receipt text are real API-dependent data.

Required state additions are functional: Owner access/token login, authentication error, engine disconnected/stale state, command acknowledgement/conflict/unknown outcome, confirmation for Kill/Reset, pending retry with the same command ID, exact limit inputs, and telemetry drop/lag indicators. These are part of the authorized owner workflow, rather than new product areas.

Component ownership: `App` composes `Login`, `Overview`, `AccountBand`, `Orders`, `Limits`, `Receipt`, `Activity`, and `History`; `useConsole` owns bounded browser polling and command identity; `api` owns typed same-origin HTTP; `model` maps exact micro-unit state without inventing funds. No engine credential enters the frontend bundle.

## Final verification ledger

Both accepted concepts and both final integrated screenshots were opened with `view_image` in the final comparison pass on 2026-10-01. The final image served CSS `index-SPQf4CNM.css`. Screenshots are `design/desktop-integrated-qa.jpg` and `design/mobile-integrated-qa.jpg`; credentials are absent. Desktop used a 1440 × 1100 viewport and mobile 390 × 844. The document widths were respectively 1425 and 375 CSS pixels after the vertical scrollbar; scroll widths matched them, so neither page had horizontal overflow. The temporary viewport was reset afterward.

| Comparison | Observed result / intentional difference |
| --- | --- |
| Header and hierarchy | The wordmark, quiet operator label, owner/sign-out region and overview hierarchy match the concepts. Mobile removes the redundant owner-session label and keeps Sign out. |
| Desktop composition | Four open metrics, a roughly two-to-one table/limits split, thin column separator and bottom history region match the specified structure. The 12 real log rows make the final page taller than the one-row concept. |
| Mobile order | Overview/status, 2 × 2 controls, market, 2 × 2 metrics, orders, limits/receipt, activity and history follow the mobile concept. Tables scroll within their own region; the final empty-order message stays visible at its left edge. |
| Palette and containers | The measured background is `rgb(16,20,17)` (`#101411`). Sage control borders, muted labels, amber readiness and rust alert/Kill treatment match the tokens. Open bands and tables remain the primary containers. |
| Typography and controls | System sans headings and monospace amounts/tables match the concept. The desktop heading measures 36px at the wide breakpoint. All four mobile control buttons measure 44px high. |
| Fixed copy | Engine overview, Single sampler · 1 Hz, Resume/Pause/Kill/Reset, SIMULATED CASH, Orders, Risk limits, Command receipt and secondary-ledger/footer copy match the design inventory. |
| Dynamic funds/history | The integrated engine reports simulated cash/available `$72.70`, reserved `$0.00` and inventory `35.00 / 35.00`; these replace the concept's illustrative `$1,000` and zero inventory. The actual sampled available-cash series replaces the empty concept history. Neither surface claims profit. |
| Actual state/error | The engine is killed and retains its prior contract ledger after an unsettled-inventory rollover. Its explicit emulator-stop alert, source message and real receive/exchange book ages are visible. Desktop caught a NOT READY sample and mobile a later FRESH sample; these are real independent sampled states. |
| Command recovery | Fixture-only browser actions produced distinct matching receipts for all controls and a limits update. An intentionally lost response retained ID `90668295-aa6f-43da-ab61-11bca0dc1b35` and expected version `0` across reload and tab close/reopen. Retrying the same fixture request received a definitive 409, removed Retry and re-enabled controls. |
| Browser scope | IAB was unavailable to this agent, so its checks used the available Brave browser through CUA. The parent separately verified integrated IAB. Captured Brave errors belonged to a pre-existing wallet extension, not console assets. No console application error was observed. |

`desktop-fixture-qa.jpg` and `mobile-fixture-qa.jpg` preserve the earlier **fixture-only** rendering and do not demonstrate real engine execution. They precede the final mobile empty-label alignment; the integrated images are the final visual result. The fixture server was stopped and its temporary source removed. Telegram remains disabled, and no external Telegram messages were sent.
