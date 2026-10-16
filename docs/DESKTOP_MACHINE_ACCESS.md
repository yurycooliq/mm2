# Desktop machine access: local enrollment verified, cloud acceptance pending

As of October 8, the owner-authorized dedicated MM2 CA and 90-day worker
certificate are enrolled in `CurrentUser\\My`. Both keys use RSA-3072/SHA256,
Microsoft Software Key Storage Provider and nonexportable CNG policies. Public
chain/constraints, local signatures, helper 1.8.5 checksum/Authenticode, exact
certificate selection and helper-generated public signature verification passed.
This does not establish TPM storage, cloud trust, renewal or unattended runtime
availability. Enrollment receipts and public PEM files stay in ignored output;
no private key was exported. The worker expires January 5, 2027 UTC; the CA key
is bound to this Windows user/device, so device loss requires controlled new-CA
enrollment. Public certificate files cannot restore either private key.

The human AWS session expired on October 8. Both regular and remote login flows
returned browser 400 as reported by the owner; ordinary console login must be
checked before new cloud pre-deployment validation. No prepared CloudFormation
change set has been executed and no machine profile is enabled.

Read-only AWS inspection on 7 October Lisbon found no Roles Anywhere trust anchors
or profiles in eu-west-1. The current human profile `mm2` was reauthenticated, but
it is still root-based and temporary. Do not claim permanent access is installed.

`infra/aws/desktop-access.cloudformation.json` prepares a **disabled** trust anchor,
profile and role. Its permissions allow only SSH SSM sessions to the registered
research instance through AWS-StartSSHSession. No engine instance, S3, IAM mutation,
PassRole, SendCommand, signing secrets or financial requests are granted. Trust
binds exact account/anchor, issuer, worker subject and source identity. Session
cleanup is scoped to `${aws:userid}`; verify that this pattern actually matches
the Roles Anywhere session identifier before activation. Do not broaden it to all
sessions to work around a failed check.

This is infrastructure for named CLI operations, not an inferred boto3 application
policy. Account/region are resolved from deployment; the default research instance
was verified from the existing scoped configuration. Deployment requires a reviewed
CA's **public** PEM and certificate enrollment. Local certificates were created
after the owner selected a dedicated CA; no cloud profile, IAM role or trust
has been created or activated as of this checkpoint.
Validate with cfn-lint, AWS validate-template and `infra/aws/desktop-access.guard`.
Guard checks default-disabled access, short sessions, exact transport scope and
certificate/anchor trust. It does not replace resolved-policy simulation or
certificate/session-name acceptance against the enrolled identity.

## Enrollment and acceptance

1. Enroll a dedicated worker certificate with matching CN/issuer, X509v3,
   DigitalSignature, SHA256-or-stronger and CA=false. The CA must be private with
   CA=true and certificate-sign usage. Store worker private key non-exportably
   using a supported Windows certificate/CNG provider; test TPM support rather
   than assuming it. Protect/restrict the CA signing key and define rotation and
   revocation responsibilities before cloud enrollment.
2. Verify the official credential helper build/digest, certificate selection and
   provider compatibility. Generate a separate `mm2-machine` AWS profile using
   `credential_process`; do not store credentials or overwrite the human login.
   CLI credential JSON goes only to AWS SDK/CLI, never telemetry/tool logs.
3. Deploy disabled, inspect/simulate role boundaries, verify mismatched CN/issuer,
   absent attributes, wrong anchor/account and engine-instance denial. Set up a
   separate SSH config pointing its SSM ProxyCommand to `mm2-machine`, preserving
   checked host keys and existing SSH key. Do not edit the working human transport
   until acceptance succeeds.
4. Enable only after review. Exercise read-only controller polling across multiple
   1800-second credential expirations, clean renewal, session cleanup, certificate
   revocation, outages and restart recovery. Existing temporary sessions may remain
   valid after revocation until expiry; do not claim instant revocation of issued
   STS credentials. Capture redacted broker receipts, not SSM SSH content logs.
5. Point approved workers at the accepted config. Keep administrative human access
   separate and move routine human work away from root. Never Resume execution,
   retry unknown effects or increase autonomy merely because access returned.

Run `python -B scripts/research_access.py --profile mm2-machine --region eu-west-1`
for a fixed-code read-only preflight. It distinguishes expired, denied, unreachable,
unconfigured and unavailable CLI, without reading login caches or printing raw
stderr. Successful identity is not proof of refresh, SSM health or engine health.
Fingerprint excludes time to support quiet-on-unchanged monitoring.

Continuously renewed access still requires a continuously available native worker.
Verify the configured model/runtime/subscription and broker isolation before moving
native scheduled analysis to any new host. No unsupported cloud cron substitute or
fallback model is introduced here.

Sources: [Roles Anywhere setup](https://docs.aws.amazon.com/rolesanywhere/latest/userguide/getting-started.html),
[trust model](https://docs.aws.amazon.com/rolesanywhere/latest/userguide/trust-model.html),
[credential helper](https://docs.aws.amazon.com/rolesanywhere/latest/userguide/credential-helper.html),
[SSM policy examples](https://docs.aws.amazon.com/systems-manager/latest/userguide/getting-started-restrict-access-examples.html).
