---
name: autoreview
description: >-
  Review a SubmitQueue change from independent lenses after mechanical checks.
  Use when the user asks for an autoreview, a review of the current changes or
  diff, a pull request review, or a review against AGENTS.md.
---

# Autoreview

Review the change. Do not edit it, do not commit, and do not post to the pull request. Findings are advice. A clean verdict covers only the target below, and a review that did not finish is not clean.

Run the four stages in order. `AGENTS.md` is the source of truth; `CLAUDE.md` is a copy, so do not read it separately.

## 1. Scope and intent

Run `bazel run //.agents/skills/autoreview/scripts:review-scope --` from the repository, followed by any arguments below. The tool is read-only and does not fetch. Its source is [`scripts/review-scope.py`](scripts/review-scope.py).

- By default it reviews committed changes since the merge-base with `origin/main`, plus staged, unstaged, and untracked files.
- `--base REF` when the user names a different base.
- `--range REF` when the target is committed work only; dirty work is excluded.
- `--commit REV` when the user names one commit. Dirty work is excluded.
- `--uncommitted` when the user wants only uncommitted work.
- When the user names a pull request, read it with `gh pr view N --json baseRefName,baseRefOid,headRefOid,title,body`. Verify local `HEAD` equals `headRefOid` and `baseRefOid` exists locally, then pass `--range <baseRefOid>` so unrelated dirty work is excluded. Do not fetch. If either check fails, stop and report the review as incomplete rather than reviewing a different revision.

Read the full commit messages (`git log <base>..HEAD`) or the pull request body, not just the subjects the script prints. Write a one-line intent and record whether it came from the PR body, commit messages, or the user. Use `unspecified` when none states an intent; do not invent one. Hold the change to that intent, including changes that do not belong in it.

The script's `## smells` lines are leads. A hit is not a finding until a lens confirms it in the code. `## hints` means an interface declaration or mock-generation source changed.

Read context from the reviewed snapshot. Branch and uncommitted targets use the working tree. A range target uses `git show HEAD:<path>` and its merge-base for before-state; a commit target uses `git show <commit>:<path>` and its parent. Do not open the working-tree copy for a committed-only target when dirty work is excluded or the named commit is not `HEAD`. If required context cannot be read from the Git objects, mark coverage incomplete.

## 2. Mechanical checks

Run only the checks that match the file list.

- Run checks only when the filesystem materializes the reviewed target. A branch or uncommitted review uses the current tree. For `--range`, any dirty work means all checks are skipped. For `--commit`, all checks are skipped unless the named commit equals `HEAD` and the tree is clean. Record `skipped: reviewed target is not materialized` and mark verification limited; never report results from a different tree.
- `make check-gazelle` when `go` or `build` is not `none`.
- `make check-mocks` when `## hints` is not `none`, or a file under `mock/` changed.
- `make check-tidy` when `MODULE.bazel`, `go.mod`, or `go.sum` changed.
- `make proto` when a `.proto`, `protopb/`, `*.pb.go`, or `*.pb.yarpc.go` file changed.
- `make lint` when anything other than `docs` changed.
- `bazel build` (or `./tool/bazel build` when `bazel` is not on `PATH`) on each package under `## packages`: append `:all`, so `//` becomes `//:all` and `//pkg` becomes `//pkg:all`.
- For each affected package, query `tests(<package>:all)`. Run `bazel test` only on the returned labels. An empty query is `not applicable`, not a failure. Exclude `integration` and `e2e` tags unless a changed file is an integration or e2e test. Do not test `//...`.

`make lint`, `make check-gazelle`, `make check-mocks`, `make check-tidy`, and `make proto` rewrite files and then require a clean tree. Skip them when `dirty: true`, record `skipped: dirty tree`, and mark verification limited. When the tree is clean, record `git status --porcelain` first and compare it after every command. A successful command that leaves changes is a failed stale-output check. Restore the tracked paths with `git restore` and remove only untracked paths that were not in the earlier status, whether the command failed or merely left edits. Do not leave the rewrite in the worktree.

`bazel test` does not edit source. Run it on a dirty tree too.

A failed check is a fact. Report the target and the first error. Do not turn a smell into a failed check.

## 3. Independent lenses

Read [`lenses.md`](lenses.md). Run a lens only when its files changed:

- **Architecture and boundaries** — `go`, `proto`, `sql`, `build`, or `config` is not `none`.
- **Correctness and failure modes** — `go`, `tests`, `python`, `shell`, `proto`, `sql`, or `config` is not `none`.
- **Simplicity and clarity** — `go`, `tests`, `python`, `shell`, `proto`, `sql`, `build`, `config`, or `other` is not `none`.
- **Tests and hermeticness** — `go`, `tests`, `python`, `shell`, `build`, `config`, `generated`, or `other` is not `none`.
- **Operability** — the diff changes logging, metrics, or an error returned to a caller.
- **Docs and RFCs** — `docs` is not `none`.

Generated files (`protopb/`, `mock/`, `*_mock.go`, `*.pb.go`) are not a lens of their own. Each lens checks whether they were regenerated, and does not review them line by line.

When you can start subagents, start one per applicable lens in parallel. Give each the lens brief, the normalized intent and its source, the `review-scope` output, the diff, and nothing from the conversation that wrote the change. When you cannot, finish one lens and set it aside before reading the next brief.

Pass a lens the smells that match it: `secondary-index` and `extension-factory` to architecture; `narration-comment` to simplicity; `sleep`, `error-string-assert`, `external-test-package`, `repo-root`, and `shell-script` to tests and hermeticness; `formatted-log` to operability. The lens confirms or drops each one.

If the diff is too large to pass, give the reviewer the file list and have it read the files. Anything unread is listed in the report, and the verdict is incomplete.

## 4. Verify and consolidate

- Admit a finding only when the changed code creates a reachable material impact. It must state the impact, the shortest reproducing scenario or static caller trace, supporting evidence, the smallest safe fix in this change, and a verification step. If impact or reachability is not established, turn it into a question or drop it.
- Open every cited location and enough surrounding code to verify the causal claim. Ordering findings need the relevant producer and consumer; concurrency findings need a concrete interleaving; rollout findings need the incompatible old/new combination; missing-test findings need the uncovered observable behavior.
- Consolidate by root cause, not wording. Merge multiple symptoms and their evidence into one finding. Resolve contradictory recommendations against the code, intent, and `AGENTS.md`; if the evidence does not decide, keep one question.
- Severity is based on consequence: **Blocker** means reproducible merge-unsafe behavior such as data loss, incorrect state, outage, security exposure, or incompatible rollout. **Should-fix** means a reachable material defect with a bounded fix appropriate to this change. **Nit** means style, preference, or process consistency without behavioral impact. Confidence is high, medium, or low.
- A question is a missing fact, not a severity. A material unresolved question makes coverage incomplete.
- Nits stay out unless the user asked for a wider pass.
- Sweep `AGENTS.md` for rules no lens covered: layout and import paths, committed proto generation, entity field comments, string enums, int64 milliseconds, Makefile target order, and commit shape when the range has commits (conventional, subject under 70 characters, one line per paragraph, no `Co-Authored-By` / `Co-authored-by`).
- Commit-shape findings must quote the offending subject or trailer from `git log <base>..HEAD` (or the named commit). Drop any claim that contradicts the message body you just read. A `Co-authored-by` trailer is a **Nit** (process/policy), never Blocker or Should-fix, and belongs under Nits only when the user asked for nits or for a commit-message pass.
- List files in scope that were not read, checks that were skipped, and lenses that did not finish.

## Report

Lead with three separate statuses. **Verdict** is `clean` or `findings`; a blocker, should-fix, or failed check makes it `findings`. **Coverage** is `complete` or `incomplete`; unread files, unfinished lenses, or material unresolved questions make it `incomplete`. **Verification** is `complete` or `limited`; any applicable skipped check makes it `limited`. Call the review clean without qualification only when all three are clean/complete/complete.

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

---
_Generated with the repository's `.agents/skills/autoreview` workflow; findings were rechecked against <scope> at <head SHA>[ plus working tree]._
```

Always end the report with the attribution footer shown above. Include a **Nits** section only when requested. Omit empty priority and dimension sections. Write "None" for an empty required section.

When the user asks for a pull-request comment, prepare concise Markdown in the same priority-first, dimension-grouped format. It may omit Scope, Intent, statuses, questions, and not-reviewed sections when they add no useful information. Summarize passing checks on one line; retain details for failed or skipped checks and use the same attribution footer. Do not post with `gh` or the GitHub API unless the user explicitly says to post or publish the comment.
