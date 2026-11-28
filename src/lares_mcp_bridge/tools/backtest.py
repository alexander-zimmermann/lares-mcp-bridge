"""Back-testing a candidate fault: the engine's own back-test, run on the read role.

The engine is a dependency at its deployed tag, so a candidate is measured
here by the very schema, kind registry and fold the detect-faults job runs —
not by a copy that could drift from it. The engine takes one synchronous
connection and nothing it could write or publish through; the bridge hands
it one on the read role, opened READ ONLY, in a worker thread.

The engine measures a room and the plant by what the site file declares, so
the bridge loads the file the detect-faults job runs on, once at start.

Whatever the engine cannot measure it refuses with a sentence naming what to
fix, and that sentence is the tool's error.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from lares_diagnostics_engine import Backtest
from lares_diagnostics_engine import backtest_fault as engine_backtest_fault
from lares_diagnostics_engine.site import Site

from .. import db

if TYPE_CHECKING:
    from ..config import Settings

_site: Site | None = None


def init(settings: Settings) -> None:
    """Load the site file; a missing or invalid one stops the start."""
    global _site
    _site = Site.load(Path(settings.site_file))


async def backtest_fault(candidate: dict[str, Any], weeks: int, limit: int) -> dict[str, Any]:
    """The episodes ``candidate`` would have produced over the last ``weeks``, oldest first."""
    limit = db.row_cap(limit)
    site = _site
    if site is None:
        raise RuntimeError("the site file is not loaded")
    try:
        result = await db.blocking_read(
            "backtest_fault",
            "backtest",
            lambda conn: engine_backtest_fault(conn, candidate, weeks=weeks, site=site),
        )
    except ValueError as exc:
        raise ValueError(f"backtest_refused: {exc}") from exc
    return _reported(result, weeks, limit)


def _reported(result: Backtest, weeks: int, limit: int) -> dict[str, Any]:
    episodes = [
        {
            "subject": episode.subject,
            "label": episode.label,
            "started_at": episode.started_at.isoformat(),
            "ended_at": episode.ended_at.isoformat() if episode.ended_at else None,
            "severity": episode.severity,
            "peak_score": episode.peak_score,
            "observations": episode.observations,
        }
        for episode in result.episodes[:limit]
    ]
    return {
        "fault": result.fault,
        "kind": result.kind.value,
        "weeks": weeks,
        "window_start": result.window_start.isoformat(),
        "frontier": result.frontier.isoformat(),
        "measured": dict(result.measured),
        "episode_count": len(result.episodes),
        "episodes": episodes,
        "truncated": len(result.episodes) > limit,
    }
