"""Persistent usage logging — append-only JSONL, read/aggregate for /insights."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_USAGE_DIR = Path("usage")
_LOG_FILE = "log.jsonl"


def _log_path(workspace: Path) -> Path:
    return workspace / _USAGE_DIR / _LOG_FILE


def record_usage(
    workspace: Path,
    *,
    model: str,
    provider: str,
    prompt_tokens: int,
    completion_tokens: int,
    tools_used: int,
) -> None:
    """Append one usage record to the JSONL log (fire-and-forget, best-effort)."""
    try:
        path = _log_path(workspace)
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "m": model,
            "p": provider,
            "pt": prompt_tokens,
            "ct": completion_tokens,
            "tu": tools_used,
        }
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError:
        pass  # non-critical; never block the agent loop


def _parse_ts(ts_str: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts_str)
    except (ValueError, TypeError):
        return None


def query_usage(workspace: Path, *, days: int | None = None) -> dict[str, Any]:
    """
    Aggregate usage from the log file.

    Args:
        workspace: nanobot workspace path.
        days: number of recent days to include, or None for all history.

    Returns:
        dict with keys: turns, prompt, completion, tools, models.
    """
    path = _log_path(workspace)
    if not path.exists():
        return {"turns": 0, "prompt": 0, "completion": 0, "tools": 0, "models": {}}

    cutoff: datetime | None = None
    if days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    turns = 0
    prompt = 0
    completion = 0
    tools = 0
    models: dict[str, int] = {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Filter by date if needed
                if cutoff is not None:
                    ts = _parse_ts(rec.get("ts", ""))
                    if ts is None or ts < cutoff:
                        continue

                turns += 1
                prompt += int(rec.get("pt", 0))
                completion += int(rec.get("ct", 0))
                tools += int(rec.get("tu", 0))
                model = rec.get("m", "unknown")
                models[model] = models.get(model, 0) + 1
    except OSError:
        pass

    return {"turns": turns, "prompt": prompt, "completion": completion, "tools": tools, "models": models}


def format_insights(stats: dict[str, Any], days: int | None = None) -> str:
    """Render the /insights response text."""
    if stats["turns"] == 0:
        label = "all time" if days is None else f"last {days} day{'s' if days != 1 else ''}"
        return f"No usage data in the last {label}."

    total_tokens = stats["prompt"] + stats["completion"]
    period = "all time" if days is None else f"last {days} day{'s' if days != 1 else ''}"
    lines = [
        f"📊 Usage — {period}",
        "",
        f"Turns:       {stats['turns']}",
        f"Tokens:      {total_tokens:,}",
        f"  Prompt:    {stats['prompt']:,}",
        f"  Output:    {stats['completion']:,}",
        f"Tool calls:  {stats['tools']}",
    ]
    if stats["models"]:
        lines.append("")
        lines.append("Models:")
        for model, count in sorted(stats["models"].items(), key=lambda x: -x[1]):
            lines.append(f"  {model}: {count} turns")
    return "\n".join(lines)
