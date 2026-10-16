# Integration and branch lifecycle

`main` is the current reviewed source baseline. Experimental deployments and
local runs retain their own exact source/image/journal identities; their existence
does not make an unmerged branch the canonical baseline.

At task start run `python scripts/pr_hygiene.py audit`. The read-only inventory
detects absent/cyclic prerequisites, stacks beyond three open PRs, ready work
still targeting a dependency, experimental work marked ready, and a main branch
older than seven days while work is pending. Ready PRs without an update for
seven days are errors; drafts without an update for fourteen days are warnings.
The PR hygiene workflow runs on relevant PR changes and daily at 08:23 UTC.
PR-event checks focus on that PR; stale-main alerts remain warnings in that check
so the alert cannot block the PR that remedies it. Scheduled/manual audits inspect
the entire inventory. Actions must be operational for hosted checks to execute.

Use one `codex/` branch for one logical change. Finish ready prerequisites before
adding more dependent work. Name every prerequisite in a stacked PR and keep
dependent work Draft. Integrate the prerequisite using a merge commit to retain
ancestry, then retarget its child to current `main` and inspect its incremental
diff again. Do not squash a stack and repeatedly replay its ancestors. Independent
changes can be reviewed and integrated separately.

Before an authorized merge run `python scripts/pr_hygiene.py ready --pr NUMBER`.
It checks the exact head/current main, Draft and hold labels, all five existing CI
jobs, any other reported failed/pending/skipped check, unresolved change requests,
and a review bound to that head and base. It performs no merge or Git mutation.
A READY result is advisory; it is not production or funded authorization. Fetch
and re-read the current base/diff immediately before merging, and use GitHub's
`--match-head-commit` option or the REST merge endpoint's `sha` field to reject a
concurrent head change. Recheck the final main tree and its validation evidence.

Record a separate reviewer's conclusion on the exact reviewed revision. For an
independent agent review, an owner-account COMMENT may contain this receipt:

```text
MM2 independent review
Head: FULL_REVIEWED_HEAD_SHA
Base: FULL_REVIEWED_BASE_SHA
Reviewer: agent:/root/independent_reviewer
Result: PASS

Describe the reviewed boundaries, actual validation and remaining limitations.
```

The authenticated account can be the owner, but the reviewer must really be a
separate contributor/agent. Do not label self-review as independent or a COMMENT
as a human approval. Human APPROVED reviews also record exact Head/Base/Result
bindings. A later source/base change requires refreshed review evidence. A
comment cannot dismiss another contributor's outstanding change request.

Apply `experimental` to strategy, simulator-quality or economic-method changes
whose acceptance evidence remains incomplete. Keep them Draft. Record observed
results, assumptions, disqualifying input gaps, unknowns and the next acceptance
gate. Use `merge-hold` for other known blockers. Remove a hold only after its
evidence gate and independent review have been accepted; this helper never
promotes experimental work automatically.

If GitHub reports that Actions jobs cannot start because of billing, quota or
runner availability, record the infrastructure error separately from executed
test failures. Complete the same relevant checks in isolated pinned containers
on a frozen source snapshot. Record source/tree hashes, actual commands, results,
skips and platform limits. Seek an explicit owner exception for those exact
revisions before merging without hosted CI. Neither old-head green checks nor
an account error is a replacement for that validation. Never increase spending
or change account plans implicitly.

After merge retarget remaining children before deleting their old base. Fetch
remote state, confirm integration by ancestry, and remove only worktrees whose
tracked/untracked files are clean and whose ignored content is disposable or
preserved elsewhere. Leave dirty or active experimental worktrees in place.
Remove the checkout subdirectory, not its enclosing dated evidence directory.
Use normal branch deletion for integrated branches. If GitHub auto-deletes the
remote branch, still inspect local work before removing its checkout or ref.

Server-side protection/rulesets currently return HTTP 403 for this private
repository's plan. Workflow checks and the helper expose debt and unsafe
readiness but cannot technically prohibit administrator/direct pushes. Keep that
limitation explicit. When protection becomes available, require PR review and
the existing checks plus `pr-hygiene`; do not change plan or visibility implicitly.
