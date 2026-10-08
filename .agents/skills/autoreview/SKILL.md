---
name: autoreview
description: >-
  Review a SubmitQueue change from independent lenses after mechanical checks.
  Use when the user asks for an autoreview, a review of the current changes or
  diff, a pull request review, or a review against AGENTS.md.
---

# Autoreview dispatcher

The review runs in subagents. Subagents cannot start subagents of their own, so this parent context coordinates three phases and relays their outputs verbatim. It never reads the diff, runs checks, opens changed files, or writes findings itself.

Every subagent is a general-purpose subagent on the inherited model, run in the foreground. Give each one only what its prompt below names: nothing from the conversation that wrote the change.

## 1. Plan

Launch one planner:

```text
You are the autoreview planner.

Repository: <absolute repository path>
Target: <the target exactly as the user named it, or `current branch changes including uncommitted work`>
Request: <user's review instruction>

Read `<repository>/.agents/skills/autoreview/review.md` and execute its "Plan" section only. Return only the plan block it specifies.
```

## 2. Lenses

For every assignment in the plan, launch one lens reviewer. Launch them all in one message so they run in parallel:

```text
You are an autoreview lens reviewer.

Plan header:
<the plan's header lines, verbatim>

Assignment:
<one assignment block, verbatim>

Read `<repository>/.agents/skills/autoreview/review.md` and execute its "Lens" section for this assignment only. Return only the lens result it specifies.
```

## 3. Consolidate

Launch one consolidator:

```text
You are the autoreview consolidator.

Plan:
<the whole plan, verbatim>

Lens results:
<every lens result, verbatim, each under its assignment id>

Read `<repository>/.agents/skills/autoreview/review.md` and execute its "Consolidate" and "Report" sections. Return only the final report.
```

Return the consolidator's report unchanged. Do not weaken, expand, or reinterpret its findings.

If a subagent cannot be launched or fails, do not review in the parent context and do not drop its part silently. Pass the failure to the consolidator as that assignment's result, so it lands under Not reviewed. If the planner or consolidator fails, report the review as incomplete and name the phase.
