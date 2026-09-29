---
name: autoreview
description: >-
  Review a SubmitQueue change from independent lenses after mechanical checks.
  Use when the user asks for an autoreview, a review of the current changes or
  diff, a pull request review, or a review against AGENTS.md.
---

# Autoreview dispatcher

Always run the review in a dedicated subagent. The parent context only dispatches the work and returns the result.

## Dispatch

1. Launch exactly one `generalPurpose` subagent with `run_in_background: false` and the inherited model.
2. Give it only:
   - the absolute repository path,
   - the target exactly as the user named it, or `current branch changes including uncommitted work` when no target was named,
   - the user's review instruction.
3. Use this worker prompt:

```text
You are the autoreview worker. Execute the review yourself; do not delegate the whole review.

Repository: <absolute repository path>
Target: <target>
Request: <user's review instruction>

Read and execute `.agents/skills/autoreview/review.md` completely. Read `lenses.md` only as directed there. Do not use conversation context about how the change was authored. Return only the final autoreview report.
```

4. Do not inspect the diff, run mechanical checks, open changed files, or produce findings in the parent context.
5. Return the worker's final report unchanged. Do not weaken, expand, or reinterpret its findings.

If a subagent cannot be launched, report the review as incomplete and explain that isolated execution was unavailable. Do not fall back to reviewing in the parent context.
