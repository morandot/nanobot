"""Tests for persistent usage logging (utils/usage_log.py)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from nanobot.bus.events import InboundMessage
from nanobot.command.builtin import cmd_insights
from nanobot.command.router import CommandContext
from nanobot.utils.usage_log import format_insights, query_usage, record_usage


@pytest.fixture()
def workspace(tmp_path: Path) -> Path:
    return tmp_path


def _read_records(workspace: Path) -> list[dict]:
    path = workspace / "usage" / "log.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# record_usage
# ---------------------------------------------------------------------------


class TestRecordUsage:
    def test_creates_log_file_and_directory(self, workspace: Path):
        record_usage(workspace, model="gpt-4o", provider="openai", prompt_tokens=100, completion_tokens=50, tools_used=2)
        assert (workspace / "usage" / "log.jsonl").exists()

    def test_appends_jsonl_record(self, workspace: Path):
        record_usage(workspace, model="gpt-4o", provider="openai", prompt_tokens=100, completion_tokens=50, tools_used=2)
        record_usage(workspace, model="claude-sonnet-4-20250514", provider="anthropic", prompt_tokens=200, completion_tokens=80, tools_used=1)
        records = _read_records(workspace)
        assert len(records) == 2
        assert records[0]["m"] == "gpt-4o"
        assert records[1]["m"] == "claude-sonnet-4-20250514"

    def test_record_fields(self, workspace: Path):
        record_usage(workspace, model="deepseek-v3", provider="openrouter", prompt_tokens=1000, completion_tokens=300, tools_used=5)
        rec = _read_records(workspace)[0]
        assert rec["m"] == "deepseek-v3"
        assert rec["p"] == "openrouter"
        assert rec["pt"] == 1000
        assert rec["ct"] == 300
        assert rec["tu"] == 5
        assert "ts" in rec
        # ts should be valid ISO format
        datetime.fromisoformat(rec["ts"])

    def test_no_error_on_unwritable_path(self, workspace: Path):
        # record_usage is best-effort; should not raise
        bad = workspace / "nonexistent" / "deeply" / "nested"
        record_usage(bad, model="x", provider="y", prompt_tokens=0, completion_tokens=0, tools_used=0)


# ---------------------------------------------------------------------------
# query_usage
# ---------------------------------------------------------------------------


class TestQueryUsage:
    def test_empty_log(self, workspace: Path):
        stats = query_usage(workspace)
        assert stats == {"turns": 0, "prompt": 0, "completion": 0, "tools": 0, "models": {}}

    def test_aggregate_all(self, workspace: Path):
        for i in range(5):
            record_usage(workspace, model="gpt-4o", provider="openai", prompt_tokens=100, completion_tokens=50, tools_used=1)
        stats = query_usage(workspace)
        assert stats["turns"] == 5
        assert stats["prompt"] == 500
        assert stats["completion"] == 250
        assert stats["tools"] == 5
        assert stats["models"] == {"gpt-4o": 5}

    def test_aggregate_multi_model(self, workspace: Path):
        record_usage(workspace, model="gpt-4o", provider="openai", prompt_tokens=100, completion_tokens=50, tools_used=1)
        record_usage(workspace, model="gpt-4o", provider="openai", prompt_tokens=100, completion_tokens=50, tools_used=1)
        record_usage(workspace, model="claude-sonnet-4-20250514", provider="anthropic", prompt_tokens=200, completion_tokens=80, tools_used=2)
        stats = query_usage(workspace)
        assert stats["turns"] == 3
        assert stats["models"] == {"gpt-4o": 2, "claude-sonnet-4-20250514": 1}

    def test_days_filter(self, workspace: Path):
        now = datetime.now(timezone.utc)
        # Old record (10 days ago)
        old_ts = (now - timedelta(days=10)).isoformat()
        log_path = workspace / "usage" / "log.jsonl"
        log_path.parent.mkdir(parents=True)
        log_path.write_text(json.dumps({"ts": old_ts, "m": "old", "p": "x", "pt": 100, "ct": 50, "tu": 1}) + "\n")
        # Recent record
        record_usage(workspace, model="new", provider="y", prompt_tokens=200, completion_tokens=80, tools_used=2)

        stats_all = query_usage(workspace)
        assert stats_all["turns"] == 2

        stats_7d = query_usage(workspace, days=7)
        assert stats_7d["turns"] == 1
        assert stats_7d["models"] == {"new": 1}

    def test_days_filter_none_shows_all(self, workspace: Path):
        record_usage(workspace, model="x", provider="y", prompt_tokens=10, completion_tokens=5, tools_used=0)
        stats = query_usage(workspace, days=None)
        assert stats["turns"] == 1

    def test_corrupt_lines_skipped(self, workspace: Path):
        log_path = workspace / "usage" / "log.jsonl"
        log_path.parent.mkdir(parents=True)
        log_path.write_text(
            "not json\n"
            '{"ts": "2026-01-01T00:00:00+00:00", "m": "ok", "p": "x", "pt": 10, "ct": 5, "tu": 0}\n'
            '{"broken\n'
        )
        stats = query_usage(workspace)
        assert stats["turns"] == 1
        assert stats["prompt"] == 10

    def test_missing_log_file(self, workspace: Path):
        stats = query_usage(workspace / "nonexistent")
        assert stats["turns"] == 0


# ---------------------------------------------------------------------------
# format_insights
# ---------------------------------------------------------------------------


class TestFormatInsights:
    def test_empty_with_days(self):
        stats = {"turns": 0, "prompt": 0, "completion": 0, "tools": 0, "models": {}}
        text = format_insights(stats, days=7)
        assert "7 days" in text

    def test_empty_singular_day(self):
        stats = {"turns": 0, "prompt": 0, "completion": 0, "tools": 0, "models": {}}
        text = format_insights(stats, days=1)
        assert "1 day" in text
        assert "1 days" not in text

    def test_empty_all_time(self):
        stats = {"turns": 0, "prompt": 0, "completion": 0, "tools": 0, "models": {}}
        text = format_insights(stats, days=None)
        assert "all time" in text

    def test_full_output(self):
        stats = {
            "turns": 142,
            "prompt": 1_021_200,
            "completion": 262_250,
            "tools": 89,
            "models": {"deepseek-v3": 98, "mimo-v2.5-pro": 44},
        }
        text = format_insights(stats, days=7)
        assert "7 days" in text
        assert "142" in text
        assert "1,283,450" in text
        assert "1,021,200" in text
        assert "262,250" in text
        assert "89" in text
        assert "deepseek-v3: 98 turns" in text
        assert "mimo-v2.5-pro: 44 turns" in text
        # Sorted by count descending
        assert text.index("deepseek") < text.index("mimo")

    def test_all_time_label(self):
        stats = {"turns": 1, "prompt": 100, "completion": 50, "tools": 0, "models": {"x": 1}}
        text = format_insights(stats, days=None)
        assert "all time" in text


# ---------------------------------------------------------------------------
# cmd_insights (builtin command handler)
# ---------------------------------------------------------------------------


def _make_insights_ctx(raw: str, workspace: Path, *, args: str = "") -> CommandContext:
    msg = InboundMessage(channel="cli", sender_id="u1", chat_id="direct", content=raw)
    loop = SimpleNamespace(workspace=workspace)
    return CommandContext(msg=msg, session=None, key=msg.session_key, raw=raw, args=args, loop=loop)


class TestCmdInsights:
    @pytest.mark.asyncio
    async def test_default_7_days(self, workspace: Path):
        # Seed a record from 10 days ago and one from now
        now = datetime.now(timezone.utc)
        log_path = workspace / "usage" / "log.jsonl"
        log_path.parent.mkdir(parents=True)
        log_path.write_text(json.dumps({
            "ts": (now - timedelta(days=10)).isoformat(),
            "m": "old", "p": "x", "pt": 100, "ct": 50, "tu": 1,
        }) + "\n")
        record_usage(workspace, model="new", provider="y", prompt_tokens=200, completion_tokens=80, tools_used=2)

        out = await cmd_insights(_make_insights_ctx("/insights", workspace, args=""))
        assert "7 days" in out.content
        assert "new" in out.content
        assert "old" not in out.content

    @pytest.mark.asyncio
    async def test_explicit_days(self, workspace: Path):
        record_usage(workspace, model="gpt-4o", provider="openai", prompt_tokens=100, completion_tokens=50, tools_used=1)
        out = await cmd_insights(_make_insights_ctx("/insights 30", workspace, args="30"))
        assert "30 days" in out.content
        assert "gpt-4o" in out.content

    @pytest.mark.asyncio
    async def test_all_keyword(self, workspace: Path):
        now = datetime.now(timezone.utc)
        log_path = workspace / "usage" / "log.jsonl"
        log_path.parent.mkdir(parents=True)
        log_path.write_text(json.dumps({
            "ts": (now - timedelta(days=30)).isoformat(),
            "m": "old", "p": "x", "pt": 100, "ct": 50, "tu": 0,
        }) + "\n")

        out = await cmd_insights(_make_insights_ctx("/insights all", workspace, args="all"))
        assert "all time" in out.content
        assert "old" in out.content

    @pytest.mark.asyncio
    async def test_invalid_arg_returns_usage_hint(self, workspace: Path):
        out = await cmd_insights(_make_insights_ctx("/insights abc", workspace, args="abc"))
        assert "Usage" in out.content

    @pytest.mark.asyncio
    async def test_no_data(self, workspace: Path):
        out = await cmd_insights(_make_insights_ctx("/insights", workspace, args=""))
        assert "No usage data" in out.content
