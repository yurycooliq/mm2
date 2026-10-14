# Local MM2 automation access

The owner explicitly selected a dedicated IAM user with a long-lived access key for unattended local operations. The concrete policy is [mm2-automation-policy.json](../infra/aws/mm2-automation-policy.json). It is bound to account `276571117378`, region `eu-west-1`, and instance `i-053db5508efc00b4a`; review these bindings before any reuse.

`scripts/mm2_iam_access.py --provision --aws <aws-executable> --receipt <public-receipt-path>` requires an already authenticated administrator using profile `mm2`. It creates `mm2-codex-automation` under `/mm2/`, verifies that it has no console login, groups or other policies, attaches the exact inline policy, and creates one explicitly authorized IAM-user access key. It never creates root access keys. An ambiguous key-creation outcome blocks another creation until inspected.

The key is encrypted with Windows DPAPI for the current Windows user under `%USERPROFILE%\.aws\mm2-automation\key.dpapi`. The AWS CLI invokes the installed credential process and receives credentials through a captured pipe. Do not run `--credential-process` interactively or enable credential/debug logging. The account that owns this Windows login can use/decrypt the key; DPAPI does not protect against compromise of that account or a local administrator.

Before switching profile `mm2`, the helper verifies STS identity and the target's connection status through an isolated configuration. It backs up the existing AWS configuration/credentials outside the repository, removes old login/static credentials only for `mm2`, and preserves unrelated sections. Credential material and backups are never committed. Profiles are switched without restarting MM2 or sending owner commands.

The policy statements allow:

- `OperateOnlyMm2WithExistingRunCommand`: the existing `AWS-RunShellScript` document on the single target.
- `SshOnlyToMm2`: SSH transport through the existing `AWS-StartSSHSession` document on that target.
- `OwnSessionTransportAndCleanup`: opening/resuming/closing only this IAM user's own SSM transport sessions. SSM `ResumeSession` is unrelated to the bot's owner Resume command.
- `ReadMm2ConnectionStatus`: connection status for the target.
- `ReconcileRunCommandReceiptsInIreland`: `ListCommands` and `GetCommandInvocation` in Ireland. AWS does not support instance-level resource scoping for these actions, so this permits reading regional command metadata/results for other instances too. It does not permit executing commands there.

Run Command and the existing SSH/sudo path provide administrative control of the selected operating system, including files readable by root. This is not a read-only bot account, and restricting the instance ARN does not make arbitrary shell execution a narrow application API. No direct IAM administration, infrastructure mutation, `iam:PassRole`, S3, Secrets Manager or billing permissions are granted. SendCommand never passes the optional `ServiceRoleArn`; no role-passing permission is needed for the selected path.

An administrator of the operating system may also use the instance's existing AWS role. At setup, the selected host's role included bounded research-export S3 permissions for `exports/mm2/*` plus SSM transport. These existing host-role permissions were inspected and not changed; the new user's lack of direct S3 permissions does not remove that indirect access.

Access keys do not expire automatically. Review/revoke/rotate them when needed using a separate administrative login; do not send keys in chat or grant this user IAM administration to automate its own privilege changes. Runtime access does not authorize funded trading, changes to risk limits, resets or restarts. Code changes still go through PRs. Independent server-side monitoring continues without a desktop AWS login once the run is actually started.
