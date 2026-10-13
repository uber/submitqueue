# Autoreview phases

Review the change. Do not edit it, do not commit, and do not post to the pull request. Findings are advice. A clean verdict covers only the planned target, and a review that did not finish is not clean.

`AGENTS.md` in the repository is the source of truth; `CLAUDE.md` is a copy, so do not read it separately. Execute only the section your prompt names.

## Plan

### Review tree

Checks and reading run against a tree that is exactly the reviewed target, so local edits never block or skew them.

- When the target includes uncommitted work (the default, or `--uncommitted`), the review tree is the repository working tree.
- Otherwise resolve the head SHA: `headRefOid` for a pull request, the named commit for `--commit`, `HEAD` for `--range`. Then create a detached tree outside the repository: `work=$(mktemp -d -t autoreview)` and `git -C <repository> worktree add --detach "$work/tree" <sha>`. Bazel gives the new tree a fresh output base, so its first build is slow; that is expected.
- For uncommitted targets, still create `work=$(mktemp -d -t autoreview)` for the plan's files.

### Scope and intent

Run the repository's copy of the scope script from inside the review tree: `python3 -I <repository>/.agents/skills/autoreview/scripts/review-scope.py <args> > "$work/scope.txt"`. It is read-only and does not fetch.

- By default it reviews committed changes since the merge-base with `origin/main`, plus staged, unstaged, and untracked files.
- `--base REF` when the user names a different base.
- `--range REF` when the target is committed work only.
- `--commit REV` when the user names one commit.
- `--uncommitted` when the user wants only uncommitted work.
- When the user names a pull request, read it with `gh pr view N --json baseRefName,baseRefOid,headRefOid,title,body`. Confirm both `headRefOid` and `baseRefOid` exist locally (`git cat-file -e`), create the review tree at `headRefOid`, and pass `--range <baseRefOid>`. Do not fetch. If either object is missing, stop and return a plan that marks the review incomplete rather than reviewing a different revision.

Read the full commit messages (`git log <base>..<head>`) or the pull request body, not just the subjects the script prints. Write a one-line intent and record whether it came from the PR body, commit messages, or the user. Use `unspecified` when none states an intent; do not invent one.

### Mechanical checks

Run only the checks that match the file list, from the review tree, and write each result to `$work/checks.txt`.

- `make check-gazelle` when `go` or `build` is not `none`.
- `make check-mocks` when `## hints` is not `none`, or a file under `mock/` changed.
- `make check-tidy` when `MODULE.bazel`, `go.mod`, or `go.sum` changed.
- `make proto` when a `.proto`, `protopb/`, `*.pb.go`, or `*.pb.yarpc.go` file changed.
- `make lint` when anything other than `docs` changed.
- `bazel build` (or `./tool/bazel build` when `bazel` is not on `PATH`) on each package under `## packages`: append `:all`, so `//` becomes `//:all` and `//pkg` becomes `//pkg:all`.
- For each affected package, query `tests(<package>:all)`. Run `bazel test` only on the returned labels. An empty query is `not applicable`, not a failure. Exclude `integration` and `e2e` tags unless a changed file is an integration or e2e test. Do not test `//...`.

`make lint`, `make check-gazelle`, `make check-mocks`, `make check-tidy`, and `make proto` rewrite files and then require a clean tree. A detached review tree is always clean. In the repository working tree, skip them when `dirty: true`, record `skipped: dirty tree`, and mark verification limited. When the tree is clean, record `git status --porcelain` first and compare it after every command. A successful command that leaves changes is a failed stale-output check. Restore the tracked paths with `git restore` and remove only untracked paths that were not in the earlier status. Do not leave the rewrite in the tree.

`bazel test` does not edit source; run it on a dirty tree too. A failed check is a fact: record the target and the first error. Do not turn a smell into a failed check.

### Assignments

Every changed file a reviewer must read lands in exactly one assignment, and each assignment is small enough for one reviewer to read completely.

- Readable files are the changed files except generated ones (kind `generated`, plus `*_pb.ts`) and lockfiles (`pnpm-lock.yaml`, `go.sum`, `MODULE.bazel.lock`, `*requirements_lock.txt`). Generated files are judged only for staleness, by the checks above or by the reviewer of their source.
- Group readable files by Bazel package (the nearest `BUILD.bazel`), and docs by directory. Merge small groups in the same top-level area; split a group above 15 files or about 1,200 changed lines.
- Give each group the lenses whose kinds it contains, by this table:
  - **Architecture and boundaries**: `go`, `web`, `proto`, `sql`, `build`, or `config`.
  - **Correctness and failure modes**: `go`, `web`, `tests`, `python`, `shell`, `proto`, `sql`, or `config`.
  - **Simplicity and clarity**: `go`, `web`, `tests`, `python`, `shell`, `proto`, `sql`, `build`, `config`, or `other`.
  - **Tests and hermeticness**: `go`, `web`, `tests`, `python`, `shell`, `build`, `config`, `generated`, or `other`.
  - **Operability**: the group's diff changes logging, metrics, or an error returned to a caller.
  - **Docs and RFCs**: `docs`.
- Give each group the smells for its files that match its lenses: `secondary-index`, `extension-factory`, and `internal-reference` to architecture; `narration-comment` to simplicity; `sleep`, `error-string-assert`, `external-test-package`, `repo-root`, `shell-script`, and `new-workflow` to tests and hermeticness; `formatted-log` to operability.
- When the change spans more than one package, add one cross-cutting architecture assignment. It reviews the seams between groups (import direction, Bazel visibility, layering, layout against `AGENTS.md`) from `BUILD.bazel` files, package entry points, and imports, not every line.
- Aim for at most 12 assignments. Past that, raise the group size; never drop a file.

### Plan block

Return exactly this, with the header ending at the `Excluded` line:

```text
# Autoreview plan
Repository: <absolute path>
Review tree: <absolute path> (<detached at SHA> | repository working tree)
Work dir: <absolute path> (scope.txt, checks.txt)
Base: <SHA>  Head: <SHA>[ plus working tree]
Target: <as the user named it>
Intent: <one line> (<source>)
Checks: passed <list>; failed <target: first error>; skipped <target: why>
Excluded from reading: <generated and lockfile paths>

## Assignment A1
Lenses: <list>
Smells: <script lines, or none>
Files:
- <path> (+<added>/-<removed>)
```

## Lens

Work only in the review tree from the plan header. Read the briefs in [`lenses.md`](lenses.md) for the lenses your assignment lists, and the repository's `AGENTS.md`.

- Read `scope.txt` in the work dir for context, and `git -C <review tree> diff <base> -- <files>` for your files.
- Read every assigned file in full from the review tree, then whatever surrounding code each claim needs. The changed line alone is not evidence of a system-level defect.
- Run your lenses one at a time: finish one and set its notes aside before reading the next brief.
- Confirm or drop every smell you were given.
- If you cannot read a file in full, name it and say why. Never skip one silently.
- `Read` lists only files you read end to end. A file you skimmed, read only partly, or know only by its test names goes under `Partial` with the line ranges you did read, even if you judge the rest unimportant.

Return exactly this:

```text
## Lens result <assignment id>
Read: <every file read end to end>
Partial: <path: line ranges read>, or none
Unread: <path: reason>, or none
Findings:
- <lens> | <blocker | should-fix> | <high | medium | low> | `path:line`
  - Impact: <observable consequence>
  - Evidence: <reproduction, interleaving, caller trace, supporting locations>
  - Fix: <smallest safe correction in this change>
  - Verify: <specific test or command>
Questions: <exact missing fact>, or none
Smells dropped: <smell: why>, or none
```

## Consolidate

- Recheck before you admit. For every candidate finding, open its cited `path:line` in the review tree and the supporting locations its evidence names, and confirm the causal claim yourself. A finding you did not open is dropped, not admitted on the reviewer's word. Record each one in the recheck log: admitted with the location you opened, merged into another, or dropped with the reason.
- Admit a finding only when the changed code creates a reachable material impact. It must state the impact, the shortest reproducing scenario or static caller trace, supporting evidence, the smallest safe fix in this change, and a verification step. If impact or reachability is not established, turn it into a question or drop it.
- Open every cited location in the review tree and enough surrounding code to verify the causal claim. Ordering findings need the relevant producer and consumer; concurrency findings need a concrete interleaving; rollout findings need the incompatible old/new combination; missing-test findings need the uncovered observable behavior.
- Consolidate by root cause, not wording. Merge multiple symptoms and their evidence into one finding. Resolve contradictory recommendations against the code, intent, and `AGENTS.md`; if the evidence does not decide, keep one question.
- Severity is based on consequence: **Blocker** means reproducible merge-unsafe behavior such as data loss, incorrect state, outage, security exposure, or incompatible rollout. **Should-fix** means a reachable material defect with a bounded fix appropriate to this change. **Nit** means style, preference, or process consistency without behavioral impact. Confidence is high, medium, or low.
- A question is a missing fact, not a severity. A material unresolved question makes coverage incomplete.
- Nits stay out unless the user asked for a wider pass.
- Sweep `AGENTS.md` for rules no lens covered: layout and import paths, committed proto generation, entity field comments, string enums, int64 milliseconds, Makefile target order, the Design Defaults, and commit shape when the range has commits (conventional, subject under 70 characters, one line per paragraph, no `Co-Authored-By` / `Co-authored-by`, no leftover `# Conflicts:` or rebase-editor lines).
- The repository is public. An Uber-internal name in the diff, a commit message, or the PR body is a **Should-fix** under Architecture and contracts, whether or not `internal-reference` fired; grep the commit messages yourself, since the script scans only the diff.
- Commit-shape findings must quote the offending subject or trailer from `git log <base>..<head>`. Drop any claim that contradicts the message body you just read. A `Co-authored-by` trailer is a **Nit**, never Blocker or Should-fix, and belongs under Nits only when the user asked for nits or for a commit-message pass.
- Build the coverage ledger by counting, not judging. For each assignment, every file under `Files` is in exactly one bucket: `Read`, `Partial`, `Unread`, or missing from all three. A failed assignment puts all its files in `Unread`. Coverage is `complete` only when `Partial`, `Unread`, and missing are all zero and no material question is open. Do not reclassify a partial read as complete because the unread part looks minor; list it under Not reviewed instead.
- Take check results from the plan's `Checks` line; any applicable skipped check makes verification limited.
- When the plan created a detached review tree, remove it once verification is done: `git -C <repository> worktree remove --force <review tree>`, then delete the work dir. Never remove the repository working tree.

## Report

Lead with three separate statuses, derived mechanically from the ledger and the checks. **Verdict** is `clean` or `findings`; a blocker, should-fix, or failed check makes it `findings`. **Coverage** is `complete` or `incomplete`; unread files, failed assignments, or material unresolved questions make it `incomplete`. **Verification** is `complete` or `limited`; any applicable skipped check makes it `limited`. Call the review clean without qualification only when all three are clean/complete/complete.

Within each priority, group findings by the concern they affect, not by which reviewer found them. Use these dimensions when applicable:

- **Architecture and contracts** — design, boundaries, separation of concerns, and interface contracts.
- **Correctness and failure modes** — behavior, state transitions, ordering, retries, and concrete bugs.
- **Simplicity and clarity** — complexity, naming, comments, terminology, and references.
- **Tests and hermeticness** — behavioral coverage, isolation, deterministic inputs, and build metadata.
- **Operability** — errors, logs, metrics, and incident diagnosis.
- **Documentation and RFCs** — decisions, alternatives, status, links, and prose conventions that do not fit a more specific dimension.

Use only dimensions with findings. Documentation findings about architecture or pipeline behavior stay under the corresponding substantive dimension; do not put every documentation defect under **Documentation and RFCs**.

```markdown
# Autoreview

Scope: <target> | Intent: <one line> (<source>)
Verdict: clean | findings | Coverage: complete | incomplete | Verification: complete | limited

## Merge blockers
### <dimension>
- (<confidence>) `path:line`
  - Impact: <observable consequence>
  - Evidence: <reproduction, interleaving, caller trace, and supporting locations>
  - Fix: <smallest safe correction in this change>
  - Verify: <specific test or command>

## Should fix
### <dimension>
- (<confidence>) `path:line`
  - Impact: <observable consequence>
  - Evidence: <reproduction, interleaving, caller trace, and supporting locations>
  - Fix: <smallest safe correction in this change>
  - Verify: <specific test or command>

## Questions requiring a decision
## Verification
- Passed: <concise list>
- Failed: <target and first error>
- Skipped: <target and why>
## Not reviewed
<partial, unread, and missing files with the reason; "None" only when the ledger counts are all zero>
## Recheck log
- Coverage ledger: <assigned> files; <read> read, <partial> partial, <unread> unread, <missing> missing
- Findings: <admitted> admitted, <merged> merged, <dropped> dropped
- Dropped: <reviewer finding: reason>, or none

---
_Generated with the repository's `.agents/skills/autoreview` workflow; findings were rechecked against <scope> at <head SHA>[ plus working tree]._
```

Always end the report with the attribution footer shown above. Include a **Nits** section only when requested. Omit empty priority and dimension sections. Write "None" for an empty required section.

When the user asks for a pull-request comment, prepare concise Markdown in the same priority-first, dimension-grouped format. It may omit Scope, Intent, statuses, questions, and not-reviewed sections when they add no useful information. Summarize passing checks on one line; retain details for failed or skipped checks and use the same attribution footer. Do not post with `gh` or the GitHub API unless the user explicitly says to post or publish the comment.
