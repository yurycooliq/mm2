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

This accepts one 32-byte hex key, never a seed phrase. OpenSSL derives its public secp256k1 key using stdin, and the helper compares the resulting Ethereum address with `config/trading.toml`'s reviewed `live.signer_address`. That address is the controlling EOA, which can differ from the Polymarket DepositWallet address. A mismatch changes nothing. Storing a matching key does not change the selected mode, authorize trading, create API credentials, send approvals, sign trades, transfer funds or Resume. Exchange eligibility, other private venue/RPC credentials, allowance/identity checks and explicit funded authorization remain separate gates.

The helper requires Linux root-owned mode-0700 `/etc/mm2`, a regular single-link mode-0600 existing runtime file, a safe update lock and a bounded credential request. It rejects symlinks, duplicate JSON fields, duplicate/unrecognized runtime keys, interpolation, concurrent changes and unsafe ownership. It runs the remote Python interpreter with `-B`, so imports never add `__pycache__` to the immutable release. Credentials never appear in tool result JSON, exception text, child environment or the SSH command. On a transport error, re-run the same operation after checking connectivity; the previous environment is retained until the atomic replacement, and exact same credentials are safe to provision again.

The bot offers `/stats` for trade statistics and the deposit-value chart, and `/active` for active positions. Console projections are secondary evidence: labels distinguish simulation from live execution, and unavailable/stale engine data must remain visible. Owner-only command/chat checks precede any response or action. Other Telegram users receive no account data.
