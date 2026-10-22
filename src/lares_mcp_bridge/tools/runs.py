"""The ledger: one row per run of every use case, whatever started it.

Every run the platform makes — an explanation on an episode event, a
scheduled proposal, a chat turn — is written by the trigger service before it
starts and closed when it ends. Reading that table is how "what did the
platform do, what did it cost, and was it any good" has one answer instead of
three.

The list is the browsing read and deliberately leaves the output text out: a
week of explanations would cost more tokens than the question is worth. A run
named by its id comes back whole, because judging an output means reading it.

A person reads their own runs and the machines' runs, never another
person's: a caller with a handle sees the rows with that ``enduser_id`` and
the rows without one, a caller without a handle only the latter. A run
outside those rows answers as if it did not exist.

This module owns the ledger's vocabulary — the subject kinds, the statuses
and the shape of a subject key — for every other module that reads the table.
Verdicts are written in :mod:`.verdicts`; nothing here writes.
"""

from __future__ import annotations

from typing import Any, Literal, get_args

from psycopg import sql

from .. import db

SubjectKind = Literal["episode", "alert_group", "chat", "none"]
RunStatus = Literal["queued", "running", "completed", "failed", "capped"]

# A run's subject key is the subject itself, or the subject and the event kind
# behind a colon: the trigger's dedupe key is one run per episode event, so the
# key has to tell `appeared` from `escalated` on the same episode. Either shape
# is found by the subject alone. Bind the key expression twice.
SUBJECT_KEY_MATCH = sql.SQL("(r.subject_key = {key} OR split_part(r.subject_key, ':', 1) = {key})")

# Cost is NUMERIC and duration an INTERVAL; both are cast here so the result
# carries plain numbers a model can compare without parsing anything.
_RUN_COLUMNS = sql.SQL(
    """
    r.id AS run_id, r.use_case, r.trigger, r.subject_kind, r.subject_key,
    r.session_id, r.enduser_id, r.surface, r.status, r.attempt, r.error,
    r.tldr, r.language, r.output_ref, r.output_state, r.model_source, r.model,
    r.tokens_in, r.tokens_out,
    r.cost::double precision AS cost,
    EXTRACT(EPOCH FROM r.duration)::double precision AS duration_seconds,
    r.verdict, r.verdict_at, r.created_at, r.finished_at
    """
)


def visible_to(enduser_id: str | None) -> tuple[sql.Composable, tuple[str, ...]]:
    """The rows the caller ``enduser_id`` may read, as a condition on ``r`` and its parameters."""
    if enduser_id is None:
        return sql.SQL("r.enduser_id IS NULL"), ()
    return sql.SQL("(r.enduser_id = %s OR r.enduser_id IS NULL)"), (enduser_id,)


def require_subject_kind(subject_kind: str) -> None:
    """Refuse a subject kind the ledger has no rows for, naming the ones it has."""
    if subject_kind not in get_args(SubjectKind):
        kinds = ", ".join(get_args(SubjectKind))
        raise ValueError(f"invalid_subject_kind: {subject_kind!r}; must be one of {kinds}")


async def list_runs(
    *,
    enduser_id: str | None,
    run_id: int | None = None,
    use_case: str | None = None,
    subject_kind: SubjectKind | None = None,
    subject_key: str | None = None,
    status: RunStatus | None = None,
    only_unjudged: bool = False,
    days: int = 7,
    limit: int = 100,
) -> dict[str, Any]:
    """Ledger rows from the last ``days`` that the caller ``enduser_id`` may read, newest first.

    Pass ``run_id`` to read one run back regardless of how old it is; that
    read alone carries the output ``text``.
    """
    if subject_kind is not None:
        require_subject_kind(subject_kind)
    if status is not None and status not in get_args(RunStatus):
        statuses = ", ".join(get_args(RunStatus))
        raise ValueError(f"invalid_status: {status!r}; must be one of {statuses}")
    days = db.window_days(days)

    visible, visible_params = visible_to(enduser_id)
    where_parts: list[sql.Composable] = [visible]
    params: list[Any] = [*visible_params]
    if run_id is not None:
        where_parts.append(sql.SQL("r.id = %s"))
        params.append(run_id)
    else:
        where_parts.append(sql.SQL("r.created_at >= now() - make_interval(days => %s)"))
        params.append(days)
    if use_case is not None:
        where_parts.append(sql.SQL("r.use_case = %s"))
        params.append(use_case)
    if subject_kind is not None:
        where_parts.append(sql.SQL("r.subject_kind = %s"))
        params.append(subject_kind)
    if subject_key is not None:
        where_parts.append(sql.SQL("r.subject_key = %s"))
        params.append(subject_key)
    if status is not None:
        where_parts.append(sql.SQL("r.status = %s"))
        params.append(status)
    if only_unjudged:
        where_parts.append(sql.SQL("r.verdict IS NULL"))

    stmt = sql.SQL(
        """
        SELECT {columns}{text}
        FROM agent_runs r
        WHERE {where}
        ORDER BY r.created_at DESC
        """
    ).format(
        columns=_RUN_COLUMNS,
        text=sql.SQL(", r.text") if run_id is not None else sql.SQL(""),
        where=sql.SQL(" AND ").join(where_parts),
    )
    result = await db.read(
        "list_runs", "agent_runs", stmt, params, limit=limit, overflow="truncate"
    )

    return {
        "days": None if run_id is not None else days,
        "filters": {
            "run_id": run_id,
            "use_case": use_case,
            "subject_kind": subject_kind,
            "subject_key": subject_key,
            "status": status,
            "only_unjudged": only_unjudged,
        },
        "limit": result.limit,
        "row_count": len(result.rows),
        "truncated": result.truncated,
        "runs": result.rows,
    }
