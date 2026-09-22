"""The verdict loop: read the episodes the detection chain holds, and attach
a binary verdict to one of them.

The mails Basalte sends are the prompt to review, not the archive — the
engine already knows every situation it caused, so the review happens
against this list. A verdict says only whether the situation was real or
nonsense, and it hangs on the individual episode rather than the fault, so
it stays possible to see *when* a fault is wrong: only at night, only in
summer, only while the laundry runs.

Nothing acts on a verdict. They are counted per fault on the dashboard and
thresholds are moved by a person with those numbers in view — which is what
turns tuning from an opinion into a measurement. ``set_episode_verdict`` is
the server's only write, and it goes through the separate write pool.
"""

from __future__ import annotations

from typing import Any, Literal, get_args

from psycopg import sql

from .. import db

# Binary by design — nobody sustains a richer scale, and for the only
# question that matters (how often does this fault get it wrong) it is enough.
VERDICTS = ("real", "nonsense")

EpisodeState = Literal["all", "open", "ended"]

_MAX_WINDOW_DAYS = 365 * 5

# The subject carries the group address it was observed on; the catalog turns
# that into the name and room a person recognises. Falls back to the bracketed
# label, then to the subject itself, for subjects that carry no address.
_CATALOG_JOIN = sql.SQL(
    """
    LEFT JOIN ga_catalog c ON c.ga = substring(e.subject from '[0-9]+/[0-9]+/[0-9]+')
    """
)


async def list_episodes(
    *,
    state: EpisodeState = "all",
    episode_id: int | None = None,
    fault: str | None = None,
    days: int = 7,
    only_unjudged: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    """Episodes overlapping the last ``days``, newest first, each with the
    verdict it already carries.

    The window is an overlap, not a start filter: an episode that began
    weeks ago and is still open is exactly what a review needs to see. Pass
    ``episode_id`` to read one episode back regardless of how old it is.
    """
    if state not in get_args(EpisodeState):
        raise ValueError(
            f"invalid_state: {state!r}; must be one of {', '.join(get_args(EpisodeState))}"
        )
    if days <= 0:
        raise ValueError(f"invalid_days: {days}")
    days = min(days, _MAX_WINDOW_DAYS)

    # A named episode is answered whatever its age — the window is for
    # browsing, not for hiding a verdict somebody just wrote.
    where_parts: list[sql.Composable] = []
    params: list[Any] = []
    if episode_id is not None:
        where_parts.append(sql.SQL("e.id = %s"))
        params.append(episode_id)
    else:
        where_parts.append(
            sql.SQL("COALESCE(e.ended_at, now()) >= now() - make_interval(days => %s)")
        )
        params.append(days)
    if state == "open":
        where_parts.append(sql.SQL("e.ended_at IS NULL"))
    elif state == "ended":
        where_parts.append(sql.SQL("e.ended_at IS NOT NULL"))
    if fault is not None:
        where_parts.append(sql.SQL("e.fault = %s"))
        params.append(fault)
    if only_unjudged:
        where_parts.append(sql.SQL("v.verdict IS NULL"))

    stmt = sql.SQL(
        """
        SELECT e.id AS episode_id, e.fault, e.subject,
               COALESCE(c.name, substring(e.subject from '\\[([^]]+)\\]'), e.subject)
                   AS affected,
               c.room, e.severity, e.started_at, e.last_seen_at, e.ended_at,
               e.peak_score, e.folded, e.externally_delivered,
               v.verdict, v.decided_at
        FROM episodes e
        {catalog}
        LEFT JOIN episode_verdicts v ON v.episode_id = e.id
        WHERE {where}
        ORDER BY e.started_at DESC
        """
    ).format(catalog=_CATALOG_JOIN, where=sql.SQL(" AND ").join(where_parts))
    result = await db.read(
        "list_episodes", "episodes", stmt, params, limit=limit, overflow="truncate"
    )

    return {
        "state": state,
        "days": None if episode_id is not None else days,
        "filters": {"episode_id": episode_id, "fault": fault, "only_unjudged": only_unjudged},
        "limit": result.limit,
        "row_count": len(result.rows),
        "truncated": result.truncated,
        "episodes": result.rows,
    }


async def set_episode_verdict(*, episode_id: int, verdict: str) -> dict[str, Any]:
    """Record ``verdict`` on one episode, overwriting any earlier one.

    One row per episode by primary key, so a second thought replaces the
    first instead of stacking beside it.
    """
    if verdict not in VERDICTS:
        raise ValueError(f"invalid_verdict: {verdict!r}; must be one of {', '.join(VERDICTS)}")

    found = await db.lookup(
        "set_episode_verdict",
        "episodes",
        "SELECT fault, subject FROM episodes WHERE id = %s",
        (episode_id,),
    )
    if not found:
        raise ValueError(f"unknown_episode: {episode_id}; call list_episodes to find a valid id")
    episode = found[0]

    written = await db.write(
        "set_episode_verdict",
        "episode_verdicts",
        """
        INSERT INTO episode_verdicts (episode_id, verdict)
        VALUES (%s, %s)
        ON CONFLICT (episode_id)
        DO UPDATE SET verdict = EXCLUDED.verdict, decided_at = now()
        RETURNING episode_id, verdict, decided_at
        """,
        (episode_id, verdict),
    )
    return {**written[0], "fault": episode["fault"], "subject": episode["subject"]}
