# codex-compact-guard

> Keep long Codex sessions usable when compaction pressure rises.

`codex-compact-guard` is a small Python guard for Codex workflows. It watches for moments where compaction is likely to damage continuity, then helps switch models or preserve context before the session loses the thread.

## Why
Long agent sessions fail quietly when context gets compressed at the wrong moment. The fix is not more prompts; it is earlier detection and a cleaner handoff.

## What it does
- Adds a guardrail around Codex compact/continuation risk.
- Keeps the implementation small enough to inspect and adapt.
- Gives you one command to run before a long local session.

## Quick start
```bash
python compact_guard.py
```

## Example output
````text
context pressure rising -> switch/preserve -> continue with less loss
````

## Proof
- Single-file Python tool, easy to audit.
- Source: https://github.com/dangoZhang/codex-compact-guard
