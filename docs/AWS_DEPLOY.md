# AWS single-host deployment package

The deployed host is in **Ireland, `eu-west-1`**. Its public Polymarket requests succeed, but on **2026-10-05** the published geoblock endpoint reports static IP `34.251.210.30` as `blocked=true`, `country=IE`. The official API reference and Help Center give inconsistent Ireland restrictions; funded eligibility remains unresolved. London `eu-west-2` is a separate approved co-location/KYC arrangement. See [Polymarket access evidence and prepared support questions](POLYMARKET_ACCESS.md) before changing eligibility handling or funded deployment. A region name or successful public request is not trading approval.

This package prepares one host for `simulation`, `trading` or `both`, with optional `shadow` and offline `demo`. Every process starts paused. The package never sends Resume, creates an AWS resource, contacts a funded account or supplies trading credentials by itself.

## Foundation and release identity

`infra/aws/host.cloudformation.json` accepts an existing VPC/public subnet, its matching AZ and an **exact reviewed Canonical Ubuntu 24.04 LTS amd64 AMI ID**. The default instance choice is `c7i.large`; confirm availability, account quota and cost in the chosen region before deployment. This is a starting hardware choice, not measured selected-host latency. No NAT gateway, managed database, load balancer or auto-scaling group is created.

The foundation supplies encrypted gp3 root/state volumes, a fixed public IPv4 address, required IMDSv2 and an IPv4 security group permitting public 80/443. Port 80 supports ACME and HTTPS redirects. Port 443 serves the existing authenticated owner console/Telegram Mini App. The engine is bound to host loopback 8088; the console has no public application port. Caddy is on a separate network from the engine. Outbound traffic is permitted for venue/data services, DNS, TLS issuance and host maintenance.

SSM is enabled by default with only instance registration and Session Manager transport actions. It grants no trading-secret, S3, ECR or general AWS API access. Verify that the selected Ubuntu AMI contains a working SSM Agent and that subnet egress reaches the appropriate endpoints. SSH is off by default; enabling it requires an existing key pair and the operator's public IPv4 `/32`. Configure a real operator management path before creating the host.

**Stateful foundation resources are retained on stack deletion/replacement.** This includes the running host, both disks, address and attachment/network/management resources. The optional SSH ingress rule is deliberately not retained: disabling SSH or changing its operator CIDR revokes the previous rule. Deleting the stack is not a shutdown or cleanup operation and does not stop charges. Host/AZ replacement needs an explicit stopped-engine storage/recovery plan; the template does not detach a mounted ledger, erase a disk or move a funded account. CloudFormation updates are infrastructure changes, not application rollback. [AWS volume lifecycle](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-ec2-volume.html)

The template deliberately contains no UserData and no runtime secrets. It boots an OS foundation; bot startup remains a later operator action. Run AWS `validate-template`/a reviewed change set in the intended account before creating anything. Offline linting does not verify AMI provenance, subnet/AZ correspondence, capacity, access or charges.

## Prepare an offline release locally

Build and test the engine/console first, complete review, then commit the source. Select a reviewed Caddy Linux/amd64 image and make it available locally. Resolve/review its registry digest before pulling; do not use an unreviewed moving `latest` tag.

```sh
mkdir -p output/releases
task_revision=$(git rev-parse HEAD)
docker build --build-arg MM2_SOURCE_REVISION="$task_revision" -t mm2-engine:local -f Dockerfile .
docker build --build-arg MM2_SOURCE_REVISION="$task_revision" -t mm2-console:local -f Dockerfile.console .
python3 scripts/deploy_package.py export --destination output/releases/RELEASE \
  --engine-image mm2-engine:local --console-image mm2-console:local \
  --proxy-image REVIEWED_LOCAL_CADDY_REFERENCE
python3 scripts/deploy_package.py verify --source output/releases/RELEASE
```

The exporter refuses a dirty source tree, wrong image architecture, symlinked public inputs and an existing destination. It exports three local images by their immutable `sha256:` IDs, public profiles and an explicit allowlist of deployment scripts/files. It never reads `.env`, keys, live credentials, journals or console SQLite. Windows line endings are normalized for Linux. `release-manifest.json` binds the source commit, Linux/amd64 platform, exact image IDs and SHA-256/byte count of every file; `images.tar` is limited to 8 GiB. Failed output is preserved with `INCOMPLETE` until export completes.

The release pins OCI manifest/index identities from a **containerd image store**. The supported target is a fresh **Docker Engine 29 or later with the containerd image store**; a classic image store can load the archive under different config IDs and cannot satisfy these exact identity pins. A save/load round trip has been verified locally on Docker 29 with containerd; selected-host import must still prove the same IDs, platform and application labels. Docker uses containerd by default on fresh Engine 29+ installations, while upgraded hosts can retain the classic store. [Docker image-store contract](https://docs.docker.com/engine/storage/containerd/)

Both application images must carry `org.opencontainers.image.revision` equal to the exact clean committed source. An older image or a build without that label is refused even if its tag says `local`. Host import and every startup preflight recheck the loaded application labels against the manifest. The Caddy image is an external reviewed dependency and does not claim the mm2 source revision. Labels bind the intended build input; review/build provenance and a trusted export/transfer channel remain required.

`scripts/deploy_verify_proxy.py --proxy-image REVIEWED_LOCAL_CADDY_REFERENCE` provides a local fixture acceptance: verified TLS chain/SNI, the actual console image, owner login, Secure/HttpOnly cookie, origin rejection and HTTP redirect. It uses a Docker `--internal` network and its own temporary CA/tokens, publishes no host ports, and cleans up only resources with its unique ownership label. It also requires the repository's pinned Python 3.13 image locally for the isolated TLS client. It neither requests public certificates nor tests actual AWS/DNS/Telegram delivery. The official Caddy binary's `NET_BIND_SERVICE` file capability must remain in its bounding set even with the container's high listening ports; other capabilities remain dropped.

Verify the manifest/checksum through a trusted channel before transferring the release directory to the host. Checksums detect corruption; an attacker who can replace both the bundle and manifest defeats them. The reviewed source/images and the transfer channel supply provenance. Keep the previous known-compatible release. No registry write is needed: the host loads this bundle, and production Compose uses `pull_policy: never`.

## Prepare the host without starting the bot

Upload the public release directory using the chosen authenticated management path. Do not put private keys, bot tokens or presigned secret links in CloudFormation, UserData, CI logs or a recorded shell command. The package can also be reviewed before AWS billing is solved.

On a **new Ubuntu 24.04 amd64 host**, inspect the exact available versions in Docker's official apt repository, select **Docker Engine 29 or later**, record/review all five compatible component versions and run:

```sh
sudo sh infra/aws/install_runtime.sh \
  EXACT_DOCKER_CE_VERSION EXACT_DOCKER_CLI_VERSION EXACT_CONTAINERD_VERSION \
  EXACT_BUILDX_VERSION EXACT_COMPOSE_VERSION
```

The initial installer rejects an older Engine version before OS/package setup, verifies Docker's repository signing-key fingerprint, installs exactly these versions, holds their automatic upgrades and starts Docker and Chrony. It then requires `DriverStatus` to report `driver-type = io.containerd.snapshotter.v1` before creating private `/etc/mm2`; an unsupported backend refuses completion without rewriting daemon configuration or starting mm2. It refuses an existing Docker installation rather than performing an unreviewed upgrade. Base Ubuntu security dependencies are obtained from the OS's maintained repository. Runtime version/security upgrades are a separate reviewed host operation. Confirm clock synchronization before accepting live feeds. [Docker's supported Ubuntu repository installation](https://docs.docker.com/engine/install/ubuntu/)

Copy the **StateVolumeId** from the reviewed stack output. For a volume just created by that stack, explicitly initialize it:

```sh
sudo sh infra/aws/mount_state.sh vol-EXACT_STATE_ID --initialize-new-volume
```

For retained/recovered state, omit `--initialize-new-volume`. The mount helper finds the exact NVMe EBS serial rather than guessing `nvme1n1` or `/dev/sdf`, refuses the root disk, partitions, existing mounts and unsupported filesystems, and refuses to format a filesystem that already exists. Initialization is an operator assertion that this exact volume is new and empty; absent filesystem signatures alone do not prove no historical raw data. It adds a UUID-bound fstab entry and mounts `/srv/mm2` with `nodev,nosuid,noexec`. Existing state is not recursively changed. [AWS NVMe naming](https://docs.aws.amazon.com/ebs/latest/userguide/nvme-ebs-volumes.html)

Persistent directories are `/srv/mm2/engine` (UID 10001), `/srv/mm2/console` (UID 10002), and `/srv/mm2/caddy` (TLS state). A `.mm2-state-volume` marker binds the chosen EBS ID. A missing/unmounted state disk refuses service startup rather than silently creating a fresh account on the root disk.

Create `/etc/mm2/runtime.env` privately as Linux root with mode **0600**. The bootstrap creates independent random tokens without printing them and preserves an existing file:

```sh
sudo python3 -B scripts/deploy_host.py bootstrap --domain your.exact.public.domain
sudoedit /etc/mm2/runtime.env
```

The host file accepts only literal values for these keys:

```text
MM2_ENGINE_API_TOKEN=PRIVATE_INDEPENDENT_TOKEN
MM2_CONSOLE_TOKEN=PRIVATE_INDEPENDENT_TOKEN
MM2_DOMAIN=your.exact.public.domain
MM2_TELEGRAM_BOT_TOKEN=
MM2_TELEGRAM_OWNER_ID=
MM2_POLY_PRIVATE_KEY=
MM2_POLY_API_KEY=
MM2_POLY_API_SECRET=
MM2_POLY_API_PASSPHRASE=
MM2_POLY_RELAYER_API_KEY=
MM2_POLY_RELAYER_API_KEY_ADDRESS=
MM2_POLYGON_RPC_URL=
```

The private signing key, CLOB L2 key/secret/passphrase and `MM2_POLYGON_RPC_URL` are required by configured funded modes. Signature type 3 also requires the separate Relayer API key/address pair. The key controls the EOA signing address; the DepositWallet is a contract and has no exportable private key. Its owner obtains the Relayer API key in Polymarket Settings → API Keys → Relayer API Keys; copy its signer address into the private address value. This existing-account route does not require a Builder account, but actual key eligibility/quota and authenticated access remain deployment checks. No helper creates these credentials. [Polymarket owner authentication and gasless setup](https://docs.polymarket.com/trading/wallets-auth)

The RPC endpoint is private runtime material: an HTTPS URL can contain a provider credential in its query/path and must not be logged or placed in a public profile. Never paste keys into chat, CloudFormation, reports or recorded shell commands, and never copy these placeholders as actual tokens. Secret values remain only in the ignored/private runtime file and container runtime. Avoid `docker inspect`, full `docker compose config` and host environment dumps: root/Docker access can inspect process environment. Use `config --quiet` for validation. Telegram needs both token and actual owner ID; empty values keep it disabled. Exact HTTPS origin and Mini App URL are derived from `MM2_DOMAIN`.

The AWS console requires Telegram authentication even while bot credentials are empty: token login and unauthenticated API access return 404. Provision the exact `@mm2b_bot` identity and complete owner ID using the hidden-input helper in [TELEGRAM_SETUP.md](TELEGRAM_SETUP.md). Keep Python bytecode disabled (`-B`) when importing or validating immutable release scripts; the systemd unit also sets `PYTHONDONTWRITEBYTECODE=1`.

Caddy overwrites `X-MM2-Client-IP` with its remote socket IP. AWS alone enables the console's explicit trusted-header flag for rate-limit buckets on the unpublished private backend. This avoids placing all internet clients in the proxy's shared login bucket. Arbitrary `X-Forwarded-For` is ignored; this header never authorizes a user. The boundary assumes DNS only/direct Caddy and the existing isolated service networks; review it before adding a CDN or public backend port.

Import verified images, then select a profile while the previous service is stopped:

```sh
sudo python3 -B scripts/deploy_host.py import --source /PRIVATE_UPLOAD_PATH/RELEASE
sudo python3 -B /opt/mm2/releases/COMMIT/scripts/deploy_host.py activate \
  --release /opt/mm2/releases/COMMIT --mode simulation
```

`activate` selects the release and configuration but does **not** start the service. It refuses an active mm2 unit/production container, keeps previous public profiles in `/etc/mm2/profile-history`, writes exact image IDs to `/etc/mm2/release.env`, validates retained storage and runs isolated engine `--check` with no network or credentials. It validates Compose without displaying secret values. A partial failed activation stays stopped and must be inspected before retry. The supervisor repeats the complete preflight before every new systemd process starts the engine.

For `trading` or `both`, supply `--profile /PRIVATE_CONFIG_PATH/reviewed.toml`: packaged templates remain inactive with `owner_authorized = false` and intentionally fail activation. Their public account/funder is `0x04Ad8Cb08F04b58fbcEa273603d6cd151Ba8aDB4`, controlling owner/signing address is `0xa2300f310b59df6d0dec94576776ce5435bd0b3f`, and signature type is 3. Public inspection establishes the native-beacon wallet relationship; it does not verify its private key or authenticated trading permissions. Review exact account/signature/deployment identity, supported protocol, allowlist, authorization and finite risk caps. Both funded profiles must retain `live_state_dir = "/data/account-live"`; mode-specific states are `/data/trading` and `/data/both`. This preserves one account ledger and fence across mode changes. Do not create a fresh data directory to replenish loss/spend history.

Funded closeout needs the exact supported wallet identity, current HTTPS Polygon RPC with historical/finalized reads and bounded logs, and **Conditional Token operator approval** for the reviewed closeout adapter. Direct EOA/proxy/one-owner Safe types 0/1/2 require native signer POL under finite transaction/session gas caps; USD collateral cannot pay that gas. The pinned native-beacon DepositWallet type 3 uses owner-signed gasless relayer batches and does not require owner POL. CLOB order signing uses its canonical ERC7739 wrapper, while batch signing uses plain owner EIP712. Read-only preflight checks the factory, effective per-wallet beacon implementation, owner, domain, predicted wallet, approvals and authenticated relayer nonce. See [LIVE_GATEWAY.md](LIVE_GATEWAY.md) for the pinned identities and exact transport contract.

The inspected account has CTF approvals for two external AutoRedeemer capabilities: `0xa1200000d0002264c9a1698e001292d00e1b00af` and `0xf3cfb6a6ebfeb51876289eb235719eb1c65252b0`. Their operators can redeem positions without wallet nonce progression, so the bot rejects Resume and preparation while either approval remains active. The owner must separately disable them, or wait for a reviewed external-receipt import path; current code does not import their effects. No deploy helper changes approvals, tops up gas, signs a funded operation or changes wallet configuration. No approval was revoked and no funded canary occurred while preparing this package.

For relayer closeout, the complete signed Batch/body/digest/nonce/deadline/prestate is durable before dispatch, and `dispatched = true` is synced before one POST. Automatic HTTP retries and nonce/signature replacement are disabled. The pending batch digest differs from the relayer's actual transaction hash; finalized factory calldata/signature and wallet events plus token/pUSD receipt effects prove success. Lost acknowledgements remain reserved while bounded chain lookup proceeds. A canonical finalized deadline/unchanged nonce/token-prestate proof can establish authorization expiry without inventing a receipt or payout, then leaves execution faulted for investigation/Reset. Restart or a new Resume does not automatically repeat an already dispatched relayer POST.

## Start and accept the selected host

Point the domain's A record at the fixed IPv4 and verify it. Do not add an AAAA record unless IPv6 is deliberately configured and tested. Caddy requires incoming 80/443 and stores certificates on persistent EBS. HTTPS issuance and actual Telegram owner access remain external tests. [Caddy HTTPS requirements](https://caddyserver.com/docs/quick-starts/https)

```sh
sudo systemctl enable --now mm2.service
sudo systemctl status mm2.service
sudo docker compose --project-name mm2-production --project-directory /opt/mm2/current \
  --env-file /etc/mm2/runtime.env -f /opt/mm2/current/infra/aws/compose.yaml ps
```

Systemd is the only engine lifecycle owner. Production Compose uses `restart: "no"` for every service; Docker daemon/host restart cannot silently bypass release and mount checks. Enabling the unit installs both the boot and `docker.service.wants` links: a new Docker daemon activation dispatches the same unit with complete preflight, while a dependency stop alone does not trigger `Restart=on-failure`. `deploy_supervisor.py` runs full preflight, starts the engine detached and checks only its Docker process `Running` flag at a one-second cadence. Engine exit causes unit cleanup to stop the stack; systemd retries after five seconds and repeats full preflight before paused recovery. Six failed starts within five minutes stop automatic retries for operator inspection. A pre-existing running engine refuses startup. No health check, engine HTTP request, console outage or Resume command triggers an engine restart. Actual host reboot/daemon restart and paused recovery remain selected-host acceptance tests. [Docker restart policy](https://docs.docker.com/reference/compose-file/services/#restart), [systemd WantedBy semantics](https://www.freedesktop.org/software/systemd/man/latest/systemd.unit.html#WantedBy=)

Console and proxy recovery uses separate bounded helper processes and `--no-deps`. Each helper verifies the persistent mount before starting only its own secondary service, with five-second retry backoff. Slow/failed helpers do not block engine supervision; timed-out helpers and their Compose children are killed together. Recovery follows process exit: an unhealthy-but-running secondary stays degraded for operator inspection and never triggers engine restart. Duplicate secondary identities or unavailable secondary inventory remain degraded without recovery mutations; duplicate engine identity fails the unit closed. Compose one-off diagnostic containers are excluded. Unit `ExecStopPost` stops owned containers even if startup preflight fails and preserves all volumes. Cleanup uses its own longer bounded inventory attempts plus validated container IDs atomically retained in root-only `/run/mm2`; a short supervision inventory timeout cannot prevent a cached engine stop. Direct `docker start`, manual production Compose startup or changing restart policies bypasses this lifecycle and is not a supported deployment procedure.

Confirm startup is paused, exact image IDs/profile/mode, real/paper account separation, disk/journal budgets, source freshness, clock and restart recovery. Measure this host's public feed and authenticated API path with no order submission; London proximity claims cannot substitute for observed RTT/tail latency. Recheck IP geoblock from this host, account eligibility and authenticated venue reconciliation. Configure BotFather's intended HTTPS Mini App and exercise the actual owner client before calling that integration ready.

Run the existing bounded read-only `scripts/soak.py` against loopback and `/srv/mm2`, and compare console-on/off engine timing, journal growth and feed reconnect behavior. Supply the engine token through a private environment, not argv. A Docker health check and a clean simulation are not profitability evidence or funded authorization. After all owner/account/protocol/host/economic gates, an explicit owner Resume is still required in the dashboard; no deployment script sends it.

## Stop, back up and roll back

Pause and reconcile pending/uncertain orders through the owner controls first. Then `sudo systemctl stop mm2.service` preserves all directories/images. For a maintenance, backup or rollback window, use `sudo systemctl disable --now mm2.service` to remove both activation links and prevent a Docker restart/host boot from starting the engine during the window; explicitly enable it again after inspection. Never use `down --volumes`, prune, delete state, or reformat the retained account volume as a rollback.

Use `scripts/ledger_ops.py` while the engine is stopped, in the same Linux lock domain. For `both`, back up `/srv/mm2/engine/both` plus `--live-state-dir /srv/mm2/engine/account-live`; for `trading`, select its mode directory with the same shared account directory. Verify the bundle and retain an off-host copy. Back up console SQLite and Caddy TLS state separately while their services are stopped; neither replaces the execution journal. Secrets require their own private recovery process and are excluded from the release/ledger archive.

Restore to new directories, verify with the Rust engine while paused and mount the restored copies at the original container paths. Do not edit persisted profile/account identities to force a restore. An EBS snapshot is a useful additional storage backup after a clean stop; it does not prove engine replay or authoritative venue reconciliation.

To roll back application code, keep state mounted, stop the unit, and `activate` a retained **journal-schema-compatible** release/profile before starting it paused. An older release that cannot understand current journal/account/settlement records must refuse recovery; do not discard records to make it start. Inspect positions, unknown reservations and original command retry receipts before another explicit Resume. No helper replaces existing backup/restore destinations or sends funded requests automatically.
