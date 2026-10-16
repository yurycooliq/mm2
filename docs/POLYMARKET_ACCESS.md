# Polymarket access: observed data and unresolved trading eligibility

Evidence checkpoint: **2026-10-05**. This is an access investigation, not a funded-trading approval or a daily report.

## Current host observations

The service runs in AWS **Ireland, `eu-west-1`**, with static public IPv4 **`34.251.210.30`**. Read-only requests from that host returned:

| Request | Observed response | What it establishes |
| --- | --- | --- |
| `GET https://polymarket.com/api/geoblock` | HTTP 200; `blocked=true`, `country="IE"`, `region="L"`, `ip="34.251.210.30"` | The published eligibility endpoint currently reports this requesting IP as blocked. HTTP 200 is successful delivery of that negative result. |
| `GET https://clob.polymarket.com/version` | HTTP 200; version `2` | The host reaches this public CLOB endpoint and observes protocol V2. It does not establish permission to place orders. |
| `GET https://clob.polymarket.com/time` | HTTP 200 | The public CLOB time endpoint is reachable. |
| `GET https://gamma-api.polymarket.com/markets/slug/btc-updown-5m-1791236700` | HTTP 200; a BTC five-minute market with its 60-second TWAP rule, active/open state and `acceptingOrders` | Public discovery reaches an actual market. `acceptingOrders` describes the market, not this owner's or host's authorization. |

A subsequent stateless feed probe from the same host, in the **2026-10-05T21:50Z** window, verified:

| Request or subscription | Observed response | What it establishes |
| --- | --- | --- |
| `GET https://gamma-api.polymarket.com/markets/slug/btc-updown-5m-1791237000` | The current market's actual two outcome-token identities were used for the following requests. | The probe was tied to a specific current market, not placeholder assets. |
| `GET https://clob.polymarket.com/book?token_id=<actual outcome token>` for each outcome | HTTP 200 for both; one book had 97 bids / 2 asks and the other had 2 bids / 97 asks. | Actual outcome order books were publicly readable from this IP. Their depth counts are a snapshot, not guaranteed executable liquidity. |
| `wss://ws-subscriptions-clob.polymarket.com/ws/market` | TLS certificate validation passed; HTTP 101; both outcome books received, each with a measured exchange age of 20 ms. | The ordinary public market WebSocket delivered fresh book snapshots during this probe. This measures snapshot age, not order acknowledgement or matching latency. |

The follow-up used no authentication and submitted no orders. It establishes public-feed connectivity and freshness at that time; it does not establish full strategy readiness, continuing feed health or funded eligibility.

The market slug and open-state observation are a dated snapshot, not a permanent discovery configuration. Refresh the current market before a feed acceptance run. These probes sent no signed orders, approvals, transfers or funded canaries, and did not create API credentials.

## Official sources do not give a consistent eligibility answer

1. The [API geographic restrictions reference](https://docs.polymarket.com/api-reference/geoblock) categorizes Ireland as frontend close-only with an unrestricted API. It also describes the public geoblock response as order-placement eligibility and instructs integrations to check it before trading.
2. The [Help Center geographic restrictions page](https://help.polymarket.com/en/articles/13364163-geographic-restrictions), dated August 14, 2026, includes Ireland among blocked countries. Its restrictions and the API reference therefore need clarification.
3. The [official Rust SDK](https://github.com/Polymarket/rs-clob-client-v2/blob/main/src/clob/client.rs) implements `check_geoblock()` using `polymarket.com/api/geoblock`. Its `closed_only_mode()` uses authenticated `GET /auth/ban-status/closed-only` for the account's close-only status. The latter is not documented as an alternative IP eligibility check; a negative account-ban result cannot establish geographic permission.
4. The [official API overview](https://docs.polymarket.com/getting-started/api) documents public market data without credentials, including Gamma discovery and the CLOB market WebSocket. Public-data access and trading permission are distinct.

No documented separate API-specific geographic eligibility endpoint or approved exemption for this exact host/account has been established. A possible explanation is that the public endpoint reflects website restrictions, but that remains an inference, not verified behavior. The current evidence also does not establish whether an order from this host would be accepted or legally permitted.

## Service behavior while this is unresolved

Use reachable, ordinary public endpoints for **read-only market observation and synthetic simulation**, subject to actual feed schema, identity, freshness and connection checks. Public request success alone does not prove that the complete feed is ready, nor does it authorize a funded order.

Keep the funded execution gate closed while the published geoblock result is negative or eligibility is unknown. Do not reinterpret `blocked=true` as permission, add a country allowlist override, or use a successful public request as the live gate. Any change to eligibility handling needs an authoritative answer and a reviewed implementation.

Do not use a VPN, proxy, IP rotation, forged location headers or a move to another region to evade a restriction. Do not migrate to London merely because the matching infrastructure is there. Obtain the relevant account/host acceptance before changing funded deployment. Read-only observation is not authority to submit a canary.

## Support question ready to send

The text below is a draft. It has **not** been sent; the owner must authorize external messaging.

> Our personal-account trading service runs in AWS eu-west-1 with static IP 34.251.210.30. Your API reference says Ireland is frontend close-only and API-unrestricted, but GET https://polymarket.com/api/geoblock returns HTTP 200 with blocked=true, country=IE, region=L. Public Gamma and CLOB requests succeed. Does this response prohibit CLOB API order placement from this server? Which authoritative endpoint or account/IP approval should our service use to establish API trading eligibility? Please clarify the difference from the Help Center's Ireland restriction.

## Approved co-location question

The [official infrastructure/co-location section](https://docs.polymarket.com/api-reference/geoblock#server-infrastructure-and-co-location) identifies `eu-west-2` as the primary region and `eu-west-1` as the nearest nonrestricted region. It offers KYC/KYB co-location access to individual own-account traders. Its linked application is the official starting point; admission has not been obtained.

> I operate one personal Polymarket account, with a Rust trading service and a private Telegram Mini App for my own controls. I do not execute trades for other users. How do I apply for approved eu-west-2 co-location as an individual trader? What KYC, account and static-server-IP approval is required, and how should the service verify eligibility after acceptance?

Complete identity verification directly through the official application. Do not put identity documents, bot tokens, private keys, API secrets or signed orders into this repository or a support draft. No response or KYC acceptance currently exists in this checkpoint.
