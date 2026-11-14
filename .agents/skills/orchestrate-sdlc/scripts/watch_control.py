#!/usr/bin/env python3
"""Persist watch intent and fence stale actions; this does not run a watcher."""

from __future__ import annotations

import argparse
import copy
from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import uuid


def parse_duration(value: str) -> timedelta:
    match = re.fullmatch(r"([1-9][0-9]*)([smhd])", value)
    if not match:
        raise ValueError("maximum must be a positive whole duration, e.g. 30m or 48h")
    try:
        return timedelta(seconds=int(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match[2]])
    except OverflowError as error:
        raise ValueError("maximum is outside the supported time range") from error


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("watch timestamps must include a timezone")
    return result


def control_watch(state: dict, command: str, now: datetime, maximum: str | None = None, run_id: str | None = None) -> dict:
    result = copy.deepcopy(state)
    watch = result.get("watch")
    if command == "STARTWATCH":
        duration = parse_duration(maximum or "48h")
        try:
            deadline = now + duration
        except OverflowError as error:
            raise ValueError("maximum is outside the supported time range") from error
        if watch and watch["phase"] != "stopped":
            if watch["phase"] not in ("starting", "watching") or now >= parse_timestamp(watch["deadline_at"]):
                raise ValueError("stop and confirm the previous run is quiescent before starting another")
            if maximum is not None:
                watch["deadline_at"] = format_timestamp(min(parse_timestamp(watch["deadline_at"]), deadline))
            return result
        if watch:
            result.setdefault("watch_history", []).append(watch)
        result["watch"] = {
            "run_id": str(uuid.uuid4()),
            "phase": "starting",
            "started_at": format_timestamp(now),
            "deadline_at": format_timestamp(deadline),
        }
        return result

    if command == "STOPWATCH" and watch is None:
        return result
    if not watch:
        raise ValueError("no watch run exists")
    if run_id is not None and run_id != watch["run_id"]:
        raise ValueError("stale watch run ID")
    if command == "STOPWATCH":
        if watch["phase"] != "stopped":
            watch["phase"] = "stopping"
            watch.setdefault("stop_requested_at", format_timestamp(now))
        return result

    if run_id is None:
        raise ValueError(f"{command} requires --run-id")
    if command == "CONFIRMSTOP":
        if watch["phase"] not in ("stopping", "stopped"):
            raise ValueError("request stop and verify owned workers are quiescent first")
        watch["phase"] = "stopped"
        watch.setdefault("stopped_at", format_timestamp(now))
        return result
    if now >= parse_timestamp(watch["deadline_at"]):
        raise ValueError("watch deadline expired")
    if command == "ARM":
        if watch["phase"] not in ("starting", "watching"):
            raise ValueError("a stopped run cannot be armed")
        watch["phase"] = "watching"
        watch.setdefault("armed_at", format_timestamp(now))
        return result
    if command == "CHECK":
        if watch["phase"] != "watching":
            raise ValueError("watch is not armed or stop has been requested")
        return result
    raise ValueError("unknown watch command")


def write_ledger(path: Path, state: dict) -> None:
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as output:
            json.dump(state, output, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True, help="absolute campaign ledger path")
    parser.add_argument("--run-id", help="required for internal ARM, CHECK, CONFIRMSTOP")
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("STARTWATCH")
    start.add_argument("maximum", nargs="?")
    for command in ("STOPWATCH", "ARM", "CHECK", "CONFIRMSTOP"):
        commands.add_parser(command)
    args = parser.parse_args(argv)
    if not args.state.is_absolute():
        parser.error("--state must be absolute")
    try:
        args.state.parent.mkdir(parents=True, exist_ok=True)
        with args.state.with_name(args.state.name + ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            state = json.loads(args.state.read_text()) if args.state.exists() else {}
            if not isinstance(state, dict):
                raise ValueError("campaign ledger must be a JSON object")
            updated = control_watch(state, args.command, datetime.now(timezone.utc), getattr(args, "maximum", None), args.run_id)
            if args.command != "CHECK" and updated != state:
                write_ledger(args.state, updated)
        print(json.dumps({"command": args.command, "watch": updated.get("watch")}, indent=2))
        return 0
    except (ValueError, KeyError, TypeError, OSError) as error:
        print(f"watch control: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
