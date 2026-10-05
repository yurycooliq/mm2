# Telegram owner access and private credentials

The Mini App verifies Telegram's signed `initData` and the complete owner user ID on the server. A typed user ID or the browser's `initDataUnsafe` alone grants no access. The console owns Telegram work; engine decisions never wait for Telegram messages, charts or SQLite.

Do not paste the bot token, wallet private key or seed phrase into chat, a command argument, `.env` in the checkout, browser DevTools or a GitHub issue. The operator enters credentials in a local interactive terminal with echo disabled. The tool sends them only as stdin through SSH over the authenticated AWS SSM tunnel, with the pinned server host key and no password fallback. It stores them atomically in root-owned mode-0600 `/etc/mm2/runtime.env`; it preserves the existing engine and console tokens and does not make a plaintext backup. Python/SSH process memory is not a hardware vault, and the host's root/Docker administrator can access runtime credentials.

The public helper must first be installed, root-owned and not group/world-writable, at `/opt/mm2/tools/provision_private_runtime.py`. The dedicated local SSH configuration and its `known_hosts` file are prepared by the deployment process in ignored `output/deployment-access/`. Keep host-key checking enabled. If AWS authentication expires, run `aws login --profile mm2 --region eu-west-1` yourself before provisioning.

Run in your own PowerShell terminal. This Windows host has Python 3.12.14 in the bundled runtime; the following absolute command works regardless of the current directory or Python's presence in `PATH`:

```powershell
& 'C:\Users\yuryc\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' -B 'C:\Users\yuryc\Code\Bot\mm2\scripts\provision_private_runtime.py' telegram
```

Enter the **full** numeric Telegram user ID, then paste the `@mm2b_bot` token from BotFather into the hidden prompt. The remote helper calls `getMe` over verified HTTPS and refuses a token for another bot. An existing different owner cannot be silently replaced. A successful update prints only the public bot identity and owner ID. Applying Telegram settings requires the deployment operator to recreate **only the console**, preserving engine uptime; this tool performs no restart or messages itself. Open the bot and send `/start` so it can communicate with your private chat.

Configure the controlling MetaMask/external EOA key independently when needed:

```powershell
& 'C:\Users\yuryc\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' -B 'C:\Users\yuryc\Code\Bot\mm2\scripts\provision_private_runtime.py' wallet
```

This accepts one 32-byte hex key, never a seed phrase. OpenSSL derives its public secp256k1 key using stdin, and the helper compares the resulting Ethereum address with `config/trading.toml`'s reviewed `live.signer_address`. The reviewed controlling EOA is **`0xa2300f310b59df6d0dec94576776ce5435bd0b3f`**; the Polymarket DepositWallet **`0x04Ad8Cb08F04b58fbcEa273603d6cd151Ba8aDB4`** is a contract and has no separate exportable private key. A mismatch changes nothing. Storing a matching key does not change the selected mode, authorize trading, create API credentials, send approvals, sign trades, transfer funds or Resume. Exchange eligibility, other private venue/RPC credentials, allowance/identity checks and explicit funded authorization remain separate gates.

For a MetaMask software account, first select the account with the controlling EOA address. Open the account selector, choose the three dots beside that account, then **Account details → Private key**, enter the local MetaMask password and hold to reveal. Copy the account key only into the terminal's hidden prompt; do not share the wallet screen or a screenshot, and clear the copied key afterward. A hardware account may not offer private-key export; do not export or enter its seed as a workaround. See [MetaMask's official account export instructions](https://support.metamask.io/configure/accounts/how-to-export-an-accounts-private-key).

Store **existing CLOB L2 credentials** together:

```powershell
& 'C:\Users\yuryc\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' -B 'C:\Users\yuryc\Code\Bot\mm2\scripts\provision_private_runtime.py' clob
```

The three hidden prompts accept the CLOB API key (UUID), URL-safe base64 secret and passphrase. These are `MM2_POLY_API_KEY`, `MM2_POLY_API_SECRET` and `MM2_POLY_API_PASSPHRASE`. Valid padded or unpadded base64 input is normalized on the server to canonical URL-safe base64 with the padding required by the pinned Rust SDK, preserving the exact decoded HMAC key bytes. This format normalization does not authenticate the credentials. They must belong to the controlling signer. Polymarket CLOB credentials come from the signer's EIP-712 L1 authentication; the documented `GET /auth/derive-api-key` retrieves an existing set for its credential nonce (normally `0`), while `POST /auth/api-key` creates a set. This provisioning command only validates and stores already obtained values: it performs neither operation and never signs anything. If there is no existing credential set, prepare a separately reviewed credential setup operation before proceeding; do not paste a signature or credential response into chat. [Official CLOB authentication flow](https://docs.polymarket.com/getting-started/api#authentication)

Store the owner **Relayer API key** and its public Signer Address:

```powershell
& 'C:\Users\yuryc\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' -B 'C:\Users\yuryc\Code\Bot\mm2\scripts\provision_private_runtime.py' relayer
```

For an existing Polymarket account, open **Settings → API Keys → Relayer API Keys**. The owner creates an API key there and copies the displayed API Key and Signer Address into the two hidden terminal prompts. The helper refuses a Signer Address that differs from the reviewed controlling EOA. It stores `MM2_POLY_RELAYER_API_KEY` and `MM2_POLY_RELAYER_API_KEY_ADDRESS` together. This key authorizes gasless DepositWallet operations and is distinct from the CLOB L2 key and Builder API credentials; a Builder account is not required for this existing-account owner path. The helper does not create, revoke or test the Relayer key. [Official existing-account wallet connection](https://docs.polymarket.com/trading/wallets-auth#connect-your-account)

Store the Polygon **HTTPS RPC endpoint**, including its provider API key when required:

```powershell
& 'C:\Users\yuryc\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' -B 'C:\Users\yuryc\Code\Bot\mm2\scripts\provision_private_runtime.py' rpc
```

Copy the full Polygon mainnet HTTPS endpoint from the RPC provider's dashboard into the hidden prompt. It is stored as `MM2_POLYGON_RPC_URL`, with no URL or provider key in the result. Only a bounded HTTPS URL without whitespace, userinfo, fragment or interpolation is accepted. Format validation does not establish RPC trust, capacity or chain identity. Deployment readiness must separately verify chain ID `137`, canonical and finalized blocks, required historical `eth_call`/receipts and bounded `eth_getLogs`, plus the wallet/deployment/allowance checks described in [LIVE_GATEWAY.md](LIVE_GATEWAY.md).

All five modes preserve existing unrelated runtime fields. Telegram updates require only the console to be recreated by the deployment operator; wallet/CLOB/Relayer/RPC updates do not restart services or activate funded trading. In particular, the reviewed profiles retain `owner_authorized = false`. New trading configuration, selected-host/account eligibility, authenticated venue acceptance and explicit owner authorization are separate from saving credentials. Relayer/RPC/CLOB modes make no network validation requests; the Telegram mode alone calls `getMe`, and wallet mode derives only the public address locally on the server.

The helper requires Linux root-owned mode-0700 `/etc/mm2`, a regular single-link mode-0600 existing runtime file, a safe update lock and a bounded credential request. It rejects symlinks, duplicate JSON fields, duplicate/unrecognized runtime keys, interpolation, concurrent changes and unsafe ownership. It runs the remote Python interpreter with `-B`, so imports never add `__pycache__` to the immutable release. Credentials never appear in tool result JSON, exception text, child environment or the SSH command. On a transport error, re-run the same operation after checking connectivity; the previous environment is retained until the atomic replacement, and exact same credentials are safe to provision again.

The bot offers `/stats` for trade statistics and the deposit-value chart, and `/active` for active positions. Console projections are secondary evidence: labels distinguish simulation from live execution, and unavailable/stale engine data must remain visible. Owner-only command/chat checks precede any response or action. Other Telegram users receive no account data.
