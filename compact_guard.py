#!/usr/bin/env python3
"""Watch Codex threads for remote compaction failures and retry on a fallback model."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

COMPACT_FAILURE_RE = re.compile(r"Error running remote compact task", re.IGNORECASE)
COMPACT_URL_RE = re.compile(r"responses/compact", re.IGNORECASE)
STREAM_FAILURE_RE = re.compile(r"stream disconnected before completion|error sending request", re.IGNORECASE)
SUCCESS_RE = re.compile(r"context_compacted|compacted history|compaction complete", re.IGNORECASE)
DEFAULT_PROMPT = "continue"


@dataclass(frozen=True)
class ThreadRow:
    id: str
    title: str
    model: str
    updated_at_ms: int
    rollout_path: Path


def now_ms() -> int:
    return int(time.time() * 1000)


def codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME", "~/.codex")).expanduser()


def default_state_db(home: Path) -> Path:
    for name in ("state_5.sqlite", "state.db", "threads.db"):
        path = home / name
        if path.exists():
            return path
    raise FileNotFoundError(f"no Codex state DB found under {home}")


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"handled": {}, "restores": {}}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
        f.write("\n")
    tmp.replace(path)


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("pragma busy_timeout=5000")
    return conn


def list_threads(
    conn: sqlite3.Connection,
    thread_id: str | None,
    active_hours: float,
) -> list[ThreadRow]:
    if thread_id:
        rows = conn.execute(
            """
            select id, title, coalesce(model, '') as model, updated_at_ms, rollout_path
            from threads
            where id = ?
            """,
            (thread_id,),
        ).fetchall()
    else:
        cutoff = now_ms() - int(active_hours * 3600 * 1000)
        rows = conn.execute(
            """
            select id, title, coalesce(model, '') as model, updated_at_ms, rollout_path
            from threads
            where archived = 0 and updated_at_ms >= ?
            order by updated_at_ms desc
            """,
            (cutoff,),
        ).fetchall()

    return [
        ThreadRow(
            id=row["id"],
            title=row["title"] or "",
            model=row["model"] or "",
            updated_at_ms=int(row["updated_at_ms"] or 0),
            rollout_path=Path(row["rollout_path"]),
        )
        for row in rows
        if row["rollout_path"]
    ]


def tail_text(path: Path, max_bytes: int) -> str:
    if not path.exists():
        return ""
    with path.open("rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        offset = max(0, size - max_bytes)
        f.seek(offset)
        data = f.read()
    if offset > 0 and b"\n" in data:
        data = data.split(b"\n", 1)[1]
    return data.decode("utf-8", errors="replace")


def latest_event_ts(text: str) -> str:
    latest = ""
    for line in text.splitlines():
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = str(obj.get("timestamp") or "")
        if ts:
            latest = ts
    return latest


def has_failure_since(text: str, handled_ts: str) -> tuple[bool, str]:
    found_ts = ""
    for line in text.splitlines():
        compact_error = COMPACT_FAILURE_RE.search(line)
        compact_stream_error = COMPACT_URL_RE.search(line) and STREAM_FAILURE_RE.search(line)
        if not (compact_error or compact_stream_error):
            continue
        if not is_runtime_error_line(line):
            continue
        ts = ""
        try:
            ts = str(json.loads(line).get("timestamp") or "")
        except json.JSONDecodeError:
            pass
        if not handled_ts or not ts or ts > handled_ts:
            found_ts = ts or latest_event_ts(text)
    return bool(found_ts), found_ts


def is_runtime_error_line(line: str) -> bool:
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return False
    typ = obj.get("type")
    payload = obj.get("payload") or {}
    payload_type = payload.get("type")
    if typ == "event_msg" and payload_type in {
        "error",
        "turn_error",
        "task_error",
        "compact_error",
        "stream_error",
    }:
        return True
    if typ == "response_item" and payload_type in {"error", "failure"}:
        return True
    return False


def has_success_after(text: str, failure_ts: str) -> bool:
    for line in text.splitlines():
        if not SUCCESS_RE.search(line):
            continue
        try:
            ts = str(json.loads(line).get("timestamp") or "")
        except json.JSONDecodeError:
            ts = ""
        if not failure_ts or not ts or ts > failure_ts:
            return True
    return False


def backup_db(db_path: Path) -> Path:
    stamp = time.strftime("%Y%m%dT%H%M%S")
    backup = db_path.with_name(f"{db_path.name}.{stamp}.bak")
    shutil.copy2(db_path, backup)
    return backup


def set_thread_model(
    conn: sqlite3.Connection,
    thread_id: str,
    model: str,
    apply: bool,
) -> None:
    if not apply:
        return
    conn.execute("update threads set model = ? where id = ?", (model, thread_id))
    conn.commit()


def read_thread_model(conn: sqlite3.Connection, thread_id: str) -> str:
    row = conn.execute("select coalesce(model, '') as model from threads where id = ?", (thread_id,)).fetchone()
    return str(row["model"] if row else "")


def run_resume(
    codex_bin: str,
    thread_id: str,
    model: str,
    prompt: str,
    timeout: int,
    apply: bool,
) -> str:
    cmd = [
        codex_bin,
        "exec",
        "resume",
        "--all",
        "--skip-git-repo-check",
        "-m",
        model,
        thread_id,
        prompt,
    ]
    print("$ " + " ".join(quote_arg(part) for part in cmd))
    if not apply:
        return "dry-run"
    subprocess.Popen(cmd)
    return "started"


def quote_arg(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_./:=@+-]+", value):
        return value
    return "'" + value.replace("'", "'\\''") + "'"


def handle_thread(
    conn: sqlite3.Connection,
    args: argparse.Namespace,
    state: dict,
    thread: ThreadRow,
) -> bool:
    text = tail_text(thread.rollout_path, args.tail_bytes)
    handled_ts = state["handled"].get(thread.id, "")
    if args.force:
        failed, failure_ts = True, latest_event_ts(text) or time.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        failed, failure_ts = has_failure_since(text, handled_ts)
    if not failed:
        if args.apply:
            maybe_restore(conn, args, state, thread, text)
        return False

    original_model = thread.model or args.primary_model
    print(f"[detect] {thread.id} {thread.title!r} compact failure at {failure_ts}")
    print(f"[switch] {original_model} -> {args.fallback_model}")

    if args.apply and not args.no_backup:
        backup = backup_db(args.state_db)
        print(f"[backup] {backup}")

    set_thread_model(conn, thread.id, args.fallback_model, args.apply)
    if args.apply:
        actual_model = read_thread_model(conn, thread.id)
        if actual_model != args.fallback_model:
            print(
                f"[switch-error] {thread.id} expected {args.fallback_model}, got {actual_model}",
                file=sys.stderr,
            )
            return False
        print(f"[switch-confirmed] {thread.id} model={actual_model}")
    if args.apply:
        state["handled"][thread.id] = failure_ts
        state["restores"][thread.id] = {
            "original_model": original_model,
            "failure_ts": failure_ts,
            "restore_after": time.time() + args.restore_delay,
        }

    if args.run_trigger:
        trigger_status = run_resume(
            args.codex_bin,
            thread.id,
            args.fallback_model,
            args.trigger_prompt,
            args.trigger_timeout,
            args.apply,
        )
        if trigger_status == "dry-run":
            print(f"[trigger] dry-run for {thread.id}")
        else:
            print(f"[trigger] started for {thread.id}")

    if args.apply:
        maybe_restore(conn, args, state, thread, tail_text(thread.rollout_path, args.tail_bytes))
    return True


def maybe_restore(
    conn: sqlite3.Connection,
    args: argparse.Namespace,
    state: dict,
    thread: ThreadRow,
    text: str,
) -> None:
    item = state["restores"].get(thread.id)
    if not item:
        return
    ready_by_time = time.time() >= float(item.get("restore_after", 0))
    ready_by_success = has_success_after(text, str(item.get("failure_ts") or ""))
    if not (ready_by_time or ready_by_success):
        return

    model = str(item.get("original_model") or args.primary_model)
    print(f"[restore] {thread.id} -> {model}")
    set_thread_model(conn, thread.id, model, args.apply)
    state["restores"].pop(thread.id, None)


def iter_once(args: argparse.Namespace, state: dict) -> bool:
    changed = False
    with connect(args.state_db) as conn:
        threads = list_threads(conn, args.thread, args.active_hours)
        if not threads:
            print("[scan] no matching threads")
        for thread in threads:
            changed = handle_thread(conn, args, state, thread) or changed
    return changed


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    home = codex_home()
    parser = argparse.ArgumentParser(
        description="Retry failed Codex remote compaction with a fallback model.",
    )
    parser.add_argument("--state-db", type=Path, default=default_state_db(home))
    parser.add_argument("--state-file", type=Path, default=home / "compact-supervisor.json")
    parser.add_argument("--thread", help="single Codex thread id; omit to scan active threads")
    parser.add_argument("--active-hours", type=float, default=24)
    parser.add_argument("--primary-model", default="gpt-5.5")
    parser.add_argument("--fallback-model", default="gpt-5.4-mini")
    parser.add_argument("--tail-bytes", type=int, default=512_000)
    parser.add_argument("--restore-delay", type=int, default=120)
    parser.add_argument("--interval", type=int, default=20)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--force", action="store_true", help="treat the selected thread as failed")
    parser.add_argument("--apply", action="store_true", help="write SQLite changes and run triggers")
    parser.add_argument("--no-backup", action="store_true")
    parser.add_argument("--run-trigger", action="store_true")
    parser.add_argument("--codex-bin", default="/Applications/Codex.app/Contents/Resources/codex")
    parser.add_argument("--trigger-timeout", type=int, default=600)
    parser.add_argument("--trigger-prompt", default=DEFAULT_PROMPT)
    return parser.parse_args(list(argv))


def main(argv: Iterable[str] = sys.argv[1:]) -> int:
    args = parse_args(argv)
    state = load_state(args.state_file) if args.apply else {"handled": {}, "restores": {}}
    print(f"[mode] {'apply' if args.apply else 'dry-run'}")
    print(f"[db] {args.state_db}")
    try:
        while True:
            iter_once(args, state)
            if args.apply:
                save_state(args.state_file, state)
            if not args.watch:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        if args.apply:
            save_state(args.state_file, state)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
