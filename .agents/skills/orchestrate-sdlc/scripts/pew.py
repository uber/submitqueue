#!/usr/bin/env python3
"""Render the PEW dashboard as Markdown from the campaign ledger and GitHub.

The ledger fields read here are documented in references/watch.md. The script
never writes the ledger; PR state and required checks come from `gh`. Zone data
comes from the system tzdata through `zoneinfo`; no tzdata package is bundled.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from worktree_control import sdlc_root

ROLE_ICONS = {"investigator": "🔎", "implementer": "🧑‍💻", "reviewer": "🧐"}
WORKING_ICON = "⚙️"
AUTHOR_ICON = "🙋"
UNKNOWN = "unknown"
CI_MARKERS = {"pass": "🟢", "pending": "🟡", "fail": "🔴"}
CHECK_BUCKETS = {"pass", "fail", "pending", "skipping", "cancel"}
HEADER = "| Agent / issue-work | PR / artifacts | PR state / CI | Next step / last action |\n|---|---|---|---|"

PRFetcher = Callable[[str], dict]


def parse_timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError(f"timestamp {value!r} must include a timezone")
    return result


def format_row_time(value: str | None, now: datetime, zone: ZoneInfo) -> str:
    if not value:
        return UNKNOWN
    local = parse_timestamp(value).astimezone(zone)
    return local.strftime("%H:%M" if local.date() == now.astimezone(zone).date() else "%m-%d %H:%M")


def cell_text(value) -> str:
    if value is None or value == "":
        return UNKNOWN
    return str(value).replace("\n", " ").replace("|", "\\|")


def watch_summary(watch: dict | None, now: datetime, zone: ZoneInfo) -> str:
    if not watch:
        return "watch off"
    deadline = parse_timestamp(watch["deadline_at"])
    if watch.get("phase") in ("starting", "watching") and now < deadline:
        return "watch on until " + deadline.astimezone(zone).strftime("%m-%d %H:%M")
    if watch.get("phase") == "stopping":
        return "watch stopping"
    return "watch off"


def run_gh(cwd: Path, *args: str) -> str:
    # `gh pr checks` exits non-zero while checks are pending or failing but
    # still prints the JSON, so the exit status is only fatal without output.
    completed = subprocess.run(["gh", *args], cwd=cwd, text=True, capture_output=True)
    if completed.stdout.strip():
        return completed.stdout
    if completed.returncode == 0 or re.search(r"\bno\b.*\bchecks\b", completed.stderr, re.IGNORECASE):
        return ""
    raise RuntimeError(completed.stderr.strip() or f"gh {' '.join(args)} exited {completed.returncode}")


def gh_pr_fetcher(cwd: Path) -> PRFetcher:
    def fetch(number: str) -> dict:
        pr = json.loads(run_gh(cwd, "pr", "view", number, "--json", "url,state,isDraft,reviewDecision"))
        pr["required_checks"] = json.loads(run_gh(cwd, "pr", "checks", number, "--required", "--json", "name,bucket") or "[]")
        pr["checks"] = json.loads(run_gh(cwd, "pr", "checks", number, "--json", "name,bucket") or "[]")
        return pr
    return fetch


def pr_marker(pr: dict | None) -> str:
    if pr is None:
        return "⚪ Unknown"
    if pr.get("state") == "OPEN":
        return "🟡 Draft" if pr.get("isDraft") else "🟢 Ready"
    return {"MERGED": "🟣 Merged", "CLOSED": "⚫ Closed"}.get(pr.get("state"), "⚪ Unknown")


def ci_verdict(pr: dict | None) -> str:
    if pr is None:
        return UNKNOWN
    buckets = {check["bucket"] for check in pr["required_checks"]}
    if not buckets:
        return "absent"
    if buckets & {"fail", "cancel"}:
        return "fail"
    if buckets - CHECK_BUCKETS:
        return UNKNOWN
    if "pending" in buckets:
        return "pending"
    if buckets == {"pass"}:
        return "pass"
    return "skipped" if buckets <= {"pass", "skipping"} else UNKNOWN


def ci_marker(pr: dict | None) -> str:
    verdict = ci_verdict(pr)
    return f"{CI_MARKERS.get(verdict, '⚪')} {verdict}"


def pr_footnotes(number: str, pr: dict | None, error: str | None) -> list[str]:
    if pr is None:
        return [f"#{number}: GitHub unavailable: {cell_text(error)}"]
    if pr.get("state") != "OPEN":
        return []
    notes = [f"#{number}: {pr['reviewDecision'].lower().replace('_', ' ')}"] if pr.get("reviewDecision") else []
    required = {check["name"] for check in pr["required_checks"]}
    for check in pr["checks"]:
        if check["name"] not in required and check["bucket"] in ("pending", "fail", "cancel"):
            notes.append(f"#{number}: {cell_text(check['name'])} {check['bucket']}")
    return notes


def render_agent_row(name: str, agent: dict, prs: dict, now: datetime, zone: ZoneInfo) -> list[str]:
    role = ROLE_ICONS.get(agent.get("role"), UNKNOWN)
    model = cell_text(agent.get("model")) if agent.get("model") else "model?"
    state = {"working": f" {WORKING_ICON}", "idle": ""}.get(agent.get("state"), " state?")
    first = f"{role} {cell_text(name)} ({model}){state} {cell_text(agent.get('issue'))}"

    numbers = [str(number) for number in agent.get("prs") or []]
    links = [f"[#{n}]({prs[n][0]['url']})" if prs[n][0] else f"#{n}" for n in numbers]
    for kind, artifact in sorted((agent.get("artifacts") or {}).items()):
        if not isinstance(artifact, dict) or not Path(artifact.get("path") or "").is_absolute():
            raise ValueError(f"agent {name}: artifact {kind} must be an object with an absolute path")
        path = Path(artifact["path"])
        links.append(f"[{cell_text(kind)}]({path.as_uri()})")
    second = " · ".join(links) or "—"

    entries = [f"{pr_marker(prs[n][0])} · {ci_marker(prs[n][0])}" for n in numbers]
    if len(numbers) > 1:
        entries = [f"#{n} {entry}" for n, entry in zip(numbers, entries)]
    third = "; ".join(entries) or "—"

    author = f"{AUTHOR_ICON} " if agent.get("author_action") else ""
    fourth = (
        f"{author}{cell_text(agent.get('next_step'))} · {cell_text(agent.get('last_action'))} "
        f"{format_row_time(agent.get('last_update'), now, zone)}"
    )

    return [first, second, third, fourth]


def validate_ledger(state: dict) -> None:
    agents = state.get("agents")
    if agents is not None and not isinstance(agents, dict):
        raise ValueError("ledger agents must be an object keyed by agent name")
    for name, agent in (agents or {}).items():
        if not isinstance(agent, dict):
            raise ValueError(f"agent {name} must be an object")
        if not isinstance(agent.get("prs", []), list):
            raise ValueError(f"agent {name}: prs must be a list")
        if not isinstance(agent.get("artifacts") or {}, dict):
            raise ValueError(f"agent {name}: artifacts must be an object keyed by kind")
    watch = state.get("watch")
    if watch is not None and not (isinstance(watch, dict) and watch.get("phase") and watch.get("deadline_at")):
        raise ValueError("ledger watch must be null or an object with phase and deadline_at")


def render_pew(state: dict, now: datetime, fetch_pr: PRFetcher) -> str:
    validate_ledger(state)
    zone_name = (state.get("parameters") or {}).get("display_timezone")
    if not zone_name:
        raise ValueError("ledger parameters.display_timezone is not set")
    try:
        zone = ZoneInfo(zone_name)
    except (ZoneInfoNotFoundError, ValueError) as error:
        raise ValueError(f"display_timezone {zone_name!r} is not a known IANA zone in the system tzdata") from error
    agents = state.get("agents") or {}
    active = {name: agent for name, agent in agents.items() if not agent.get("archived")}

    prs: dict[str, tuple[dict | None, str | None]] = {}
    for agent in active.values():
        for number in agent.get("prs") or []:
            number = str(number)
            if number not in prs:
                try:
                    prs[number] = (fetch_pr(number), None)
                except (RuntimeError, OSError, ValueError) as error:
                    prs[number] = (None, str(error))

    def last_update_key(name: str) -> datetime:
        value = active[name].get("last_update")
        return parse_timestamp(value) if value else datetime.min.replace(tzinfo=timezone.utc)

    order = sorted(sorted(active), key=last_update_key, reverse=True)

    local_now = now.astimezone(zone)
    lines = [f"PEW · {local_now.strftime('%Y-%m-%d %H:%M %Z')} · {watch_summary(state.get('watch'), now, zone)}", "", HEADER]
    lines += ["| " + " | ".join(render_agent_row(name, active[name], prs, now, zone)) + " |" for name in order]

    notes = [f"{AUTHOR_ICON} {name}: {cell_text(active[name]['author_action'])}" for name in order if active[name].get("author_action")]
    for number, (pr, error) in prs.items():
        notes += pr_footnotes(number, pr, error)
    archived = len(agents) - len(active)
    if archived:
        notes.append(f"{archived} archived agent{'s' if archived != 1 else ''}")
    if notes:
        lines += [""] + [f"- {note}" for note in notes]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None, fetcher_factory: Callable[[Path], PRFetcher] = gh_pr_fetcher) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, help="campaign ledger path; defaults to <main checkout>/.sdlc/campaign.json")
    parser.add_argument("--now", help="ISO-8601 time to render at, with a timezone")
    args = parser.parse_args(argv)
    cwd = Path(os.environ.get("BUILD_WORKING_DIRECTORY", os.getcwd()))
    try:
        path = sdlc_root(cwd) / "campaign.json" if args.state is None else cwd / args.state
        state = json.loads(path.read_text())
        if not isinstance(state, dict):
            raise ValueError("campaign ledger must be a JSON object")
        now = parse_timestamp(args.now) if args.now else datetime.now(timezone.utc)
        sys.stdout.write(render_pew(state, now, fetcher_factory(cwd)))
    except (OSError, ValueError, KeyError) as error:
        print(f"pew: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
