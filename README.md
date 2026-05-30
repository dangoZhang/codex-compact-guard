# Codex Compact Guard

中文 | [English](#english)

## 问题

Codex 长线程有时会 remote compact 失败，线程卡在 `gpt-5.5` 下继续触发同一个失败。

## 方案

监督器扫描 Codex 活跃线程。发现 compact 失败后，把线程 model 临时写成 `gpt-5.4-mini`，发送一次 `continue` 让 Codex 真正进入新模型轮次，压缩后再恢复为 `gpt-5.5`。

## 效果

单线程可用 `--thread` 修复；全局可用 `--watch` 监听最近活跃线程。默认 dry-run，只有 `--apply` 会写入 `~/.codex/state_5.sqlite`，写前会备份。

## 用法

```bash
python3 compact_guard.py --thread THREAD_ID
python3 compact_guard.py --thread THREAD_ID --force --apply --run-trigger
python3 compact_guard.py --watch --active-hours 24 --apply --run-trigger
```

## English

## Problem

Long Codex threads can fail remote compaction and keep retrying the same failure under `gpt-5.5`.

## Fix

The supervisor scans active Codex threads. On compact failure, it temporarily writes the thread model to `gpt-5.4-mini`, sends `continue` so Codex actually enters a new-model turn, then restores `gpt-5.5` after compaction.

## Result

Use `--thread` for one thread, or `--watch` for recently active threads. It is dry-run by default; `--apply` writes `~/.codex/state_5.sqlite` and creates a backup first.

## Usage

```bash
python3 compact_guard.py --thread THREAD_ID
python3 compact_guard.py --thread THREAD_ID --force --apply --run-trigger
python3 compact_guard.py --watch --active-hours 24 --apply --run-trigger
```
