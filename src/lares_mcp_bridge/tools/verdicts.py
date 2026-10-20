"""The server's only write: a person's binary judgement on one subject.

A verdict says whether a thing was any good, and it is the one measurement
the platform does not give itself. On an episode it judges the detection —
was the fault real or nonsense — and it hangs on the individual episode
rather than on the fault, so it stays possible to see *when* a fault is
wrong: only at night, only in summer, only while the laundry runs. On a run
it judges the output — was the answer helpful or useless.

A run is judged only where the caller may read it — their own runs and the
machines' — so a verdict never confirms that another person's run exists.

Binary by design in both cases: nobody sustains a richer scale, and for the
only questions that matter (how often does this fault get it wrong, how often
does this use case earn its tokens) it is enough. One verdict per subject; a
second thought overwrites the first instead of stacking beside it.

Nothing acts on a verdict. They are counted per fault and per use case on the
dashboard and thresholds are moved by a person with those numbers in view —
which is what turns tuning from an opinion into a measurement. Both writes go
through the separate write pool, whose role may touch the verdict table and
the two verdict columns of the ledger and nothing else.
"""

from __future__ import annotations

from typing import Any, Literal, get_args

from psycopg import sql

from .. import db
from .runs import SUBJECT_KEY_MATCH, SubjectKind, require_subject_kind, visible_to

VerdictTarget = Literal["episode", "run"]
Verdict = Literal["real", "nonsense", "helpful", "useless"]

# Which word pair belongs to which target — the words are not interchangeable,
# "helpful" says nothing about a fault and "real" nothing about an answer.
_VERDICTS: dict[str, tuple[str, ...]] = {
    "episode": ("real", "nonsense"),
    "run": ("helpful", "useless"),
}

_RUN_IDENTITY = sql.SQL(
    "SELECT r.id AS run_id, r.use_case, r.subject_kind, r.subject_key FROM agent_runs r"
)


async def set_verdict(
    *,
    enduser_id: str | None,
    target: VerdictTarget,
    verdict: Verdict,
    episode_id: int | None = None,
    run_id: int | None = None,
    subject_kind: SubjectKind | None = None,
    subject_key: str | None = None,
) -> dict[str, Any]:
    """Record ``verdict`` on one episode or one run, overwriting any earlier one.

    An episode is named by ``episode_id``. A run is named by ``run_id``, or
    by ``subject_kind`` and ``subject_key`` together for the newest run on
    that subject, or by nothing at all for the caller's newest chat run — the
    answer just given in the chat. Every way of naming a run finds only the
    runs the caller ``enduser_id`` may read.
    """
    if target not in get_args(VerdictTarget):
        raise ValueError(
            f"invalid_target: {target!r}; must be one of {', '.join(get_args(VerdictTarget))}"
        )
    allowed = _VERDICTS[target]
    if verdict not in allowed:
        raise ValueError(
            f"invalid_verdict: {verdict!r}; on a {target} it must be one of {', '.join(allowed)}"
        )

    if target == "episode":
        if run_id is not None or subject_kind is not None or subject_key is not None:
            raise ValueError(
                'misplaced_address: run_id, subject_kind and subject_key belong to target "run"'
            )
        if episode_id is None:
            raise ValueError("missing_episode_id: name the episode this verdict is about")
        return await _judge_episode(episode_id=episode_id, verdict=verdict)

    if episode_id is not None:
        raise ValueError(
            'misplaced_address: episode_id belongs to target "episode"; a run about an episode'
            ' is addressed with subject_kind "episode" and the episode id as subject_key'
        )
    return await _judge_run(
        enduser_id=enduser_id,
        verdict=verdict,
        run_id=run_id,
        subject_kind=subject_kind,
        subject_key=subject_key,
    )


async def _judge_episode(*, episode_id: int, verdict: str) -> dict[str, Any]:
    """One row per episode by primary key, so a second thought replaces the first."""
    found = await db.lookup(
        "set_verdict",
        "episodes",
        "SELECT fault, subject FROM episodes WHERE id = %s",
        (episode_id,),
    )
    if not found:
        raise ValueError(f"unknown_episode: {episode_id}; call list_episodes to find a valid id")

    written = await db.write(
        "set_verdict",
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
    return {"target": "episode", **written[0], **found[0]}


async def _judge_run(
    *,
    enduser_id: str | None,
    verdict: str,
    run_id: int | None,
    subject_kind: SubjectKind | None,
    subject_key: str | None,
) -> dict[str, Any]:
    """Two columns on the ledger row, so a second thought replaces the first.

    The UPDATE touches nothing but those two, which is exactly the column
    grant the verdict role holds: a judgement can never rewrite what it judges.
    """
    run = await _resolve_run(
        enduser_id=enduser_id, run_id=run_id, subject_kind=subject_kind, subject_key=subject_key
    )

    written = await db.write(
        "set_verdict",
        "agent_runs",
        """
        UPDATE agent_runs SET verdict = %s, verdict_at = now() WHERE id = %s
        RETURNING id AS run_id, use_case, subject_kind, subject_key, tldr, verdict, verdict_at
        """,
        (verdict, run["run_id"]),
    )
    return {"target": "run", **written[0]}


async def _resolve_run(
    *,
    enduser_id: str | None,
    run_id: int | None,
    subject_kind: SubjectKind | None,
    subject_key: str | None,
) -> dict[str, Any]:
    """The run that the three ways of naming one point at, among those ``enduser_id`` may read."""
    if run_id is not None and (subject_kind is not None or subject_key is not None):
        raise ValueError(
            "ambiguous_run_address: name a run by run_id, or by subject_kind and subject_key,"
            " or by nothing for your newest chat run"
        )
    if (subject_kind is None) != (subject_key is None):
        raise ValueError(
            "incomplete_subject_address: subject_kind and subject_key name a subject together"
        )

    visible, visible_params = visible_to(enduser_id)
    if run_id is not None:
        found = await db.lookup(
            "set_verdict",
            "agent_runs",
            sql.SQL("{identity} WHERE r.id = {run_id} AND {visible}").format(
                identity=_RUN_IDENTITY, run_id=sql.Placeholder(), visible=visible
            ),
            (run_id, *visible_params),
        )
        if not found:
            raise ValueError(f"unknown_run: {run_id}; call list_runs to find a valid id")
        return found[0]

    if subject_kind is not None and subject_key is not None:
        require_subject_kind(subject_kind)
        found = await db.lookup(
            "set_verdict",
            "agent_runs",
            sql.SQL(
                "{identity} WHERE r.subject_kind = {kind} AND {key_match} AND {visible}"
                " ORDER BY r.created_at DESC LIMIT 1"
            ).format(
                identity=_RUN_IDENTITY,
                kind=sql.Placeholder(),
                key_match=SUBJECT_KEY_MATCH.format(key=sql.Placeholder()),
                visible=visible,
            ),
            (subject_kind, subject_key, subject_key, *visible_params),
        )
        if not found:
            raise ValueError(
                f"no_run_on_subject: {subject_kind}/{subject_key};"
                " call list_runs to see which runs exist"
            )
        return found[0]

    # The caller's own chat: their rows, or the rows without a person when they have none.
    found = await db.lookup(
        "set_verdict",
        "agent_runs",
        sql.SQL(
            "{identity} WHERE r.subject_kind = 'chat' AND r.enduser_id IS NOT DISTINCT FROM {who}"
            " ORDER BY r.created_at DESC LIMIT 1"
        ).format(identity=_RUN_IDENTITY, who=sql.Placeholder()),
        (enduser_id,),
    )
    if not found:
        raise ValueError(
            "no_run_on_subject: chat; there is no chat run of yours to judge yet —"
            " name a run_id or a subject instead"
        )
    return found[0]
