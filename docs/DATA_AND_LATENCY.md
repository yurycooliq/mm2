# Bounded historical evidence and network measurements

Research tools are outside the engine image and execution path. Use Python 3.13, Node 22.18+ or 24 and optional pinned dependencies:

```sh
python -m pip install -r tools/data/requirements.txt -r tools/latency/requirements.txt
python -B -m unittest discover -s tools/data -p 'test_*.py'
python -B -m unittest discover -s tools/latency -p 'test_*.py'
node --experimental-transform-types --test tools/reference/execution.test.ts tools/reference/pendulumflow.test.ts
```

Pendulumflow supplies existing V3 data under CC BY 4.0; credit **pendulumflow** and https://archive.pendulumflow.com/. Verified licence source: https://archive.pendulumflow.com/credits. Import only small declared slices, preserving all seven native products, exact decimals, receive microseconds, exchange milliseconds, identities, witnesses, adjacent receive partitions and manifest/range hashes. Each acquisition requires a fresh empty directory **outside this checkout**, finite network/output/event/runtime limits and an explicit development selection.

```sh
python -B tools/data/pendulumflow_probe.py inspect --hour 2026-09-29T20 --max-bytes 4000000 --out /absolute/research/mm2/inspection
python -B tools/data/pendulumflow_import.py --help
python -B tools/data/pendulumflow_import.py --spec /absolute/research/mm2/spec.json --out /absolute/research/mm2/canonical
node --experimental-transform-types tools/reference/archive-quality.ts --directory /absolute/research/mm2/canonical --out /absolute/research/mm2/quality.json --max-events 1000000 --max-bytes 256000000 --max-book-gap-ms 1000
```

The importer reports partial acquisition honestly and refuses to continue beyond budgets. Its supervised worker bounds stalled network and parsing. The standalone quality validator checks canonical hash, identities, initial/terminal snapshots, tick evidence, BBO/depth parity, native duplicate identity, gap policy and ambiguous equal-clock order. It supports compact 5m/15m identities. JSON sorting or a later snapshot does not retrospectively resolve a past trading ambiguity.

The prior September 29 development projection is not committed or marked eligible here. Acquisition success, diagnostic replayability, exact TWAP availability, lossless capture and economic scoring are distinct. Merged witness receive timestamps do not establish the information available to one strategy host. Exact governing TWAP60 access and an untouched eligible holdout remain outstanding. No missing reference is replaced with Binance candles or inferred precision.

Read-only regional capture can be prepared now:

```sh
python -B tools/latency/feed_latency_probe.py --help
python -B tools/latency/feed_latency_compare.py --help
python -B tools/latency/clob-latency-probe.py --help
```

Use identical token subscriptions and Binance symbols concurrently on candidate hosts. Record event hashes, dropped counts, reconnects and clock offset/error bounds. The comparator refuses numeric cross-host arrival differences without those bounds. Neither a cold HTTP request nor a public WebSocket proves authenticated post/cancel latency or account-specific capacity. Keep TLS verification enabled.

The engine's Binance BTCUSDT trade stream supplies a latest spot-price diagnostic,
not an execution tape or the governing TWAP60. It validates each message and keeps
one pending reference per socket, publishing at a 50 ms interval without catch-up
bursts. A full actor queue retains that slot until another attempt; newer prices
replace the pending price. Original exchange and monotonic receive timestamps
remain intact, so queue delay cannot make an old price fresh. CLOB books, trades
and tick updates retain their ordered, whole-frame admission and overflow faults.

`feed_diagnostics.binance` exposes per-boot `reference_received`,
`reference_coalesced`, `reference_delivered`, `reference_queue_full`,
`reference_discarded`, `reference_pending` and `reference_publish_interval_ms`.
Queue-full counts measure publication attempts, not missing trades. These
independently sampled counters are diagnostics, not financial reconciliation.
Ending a socket discards its pending reference; a recoverable Binance Disconnect
waits for queue capacity only until the existing send deadline, preserving the
order of already delivered prices. Closed consumers, invalid input and expired
Disconnect admission remain fatal.

After building the engine image, run `python -B scripts/smoke_public.py --seconds 35` for a bounded, credential-free public readiness check. It creates and removes only its labelled disposable container and state volume. On October 1, 2026 the current local Docker host passed API readiness but public IP eligibility metadata transport failed; shadow readiness was never reached. This result does not identify the transport failure's cause or measure latency on a future server.

The core benchmark and `scripts/scenarios.py` provide offline timing and synthetic sensitivity. Scenario delays are assumptions, not measurements. The latter always reports scoring/live eligibility false and never computes a financial return.
