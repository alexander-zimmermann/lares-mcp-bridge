"""Live tools — current state read straight from NATS JetStream.

``get_current_state`` returns the last retained message per subject (microsecond
fresh, no projector/cache); ``subscribe_nats`` tails a subject for a short
window. Both translate the homelab's NATS subject layout into LLM-friendly
fields and fall back to the last TSDB row only when JetStream never saw a subject.

Freshness is domain-aware: cyclic sensors (heating/dhw/solar publish every few
seconds) carry a ``stale`` flag once they go quiet past the stale threshold;
event-driven domains (knx switch state, wallbox charger state publish only on
change) are never "stale" — the last value is still the truth, so they report
the raw ``freshness_seconds`` age and let the caller judge.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from psycopg import sql

from .. import db
from .. import metrics as metrics_module
from .. import nats as nats_module
from ..logging_setup import get_logger

log = get_logger(__name__)

# Domain → JetStream stream. The subject(s) and field mapping per domain live in
# the dispatch functions below.
_STREAMS = {
    "knx": "KNX",
    "heating": "EMS_ESP",
    "dhw": "EMS_ESP",
    "solar": "SOLAREDGE",
    "wallbox": "WARP",
}

# Domains whose sources publish cyclically (every few seconds), so silence past
# the threshold genuinely means "sensor stopped" → worth a ``stale`` flag. The
# others (knx, wallbox) are event-driven: an old value is still the current one.
_CYCLIC_DOMAINS = frozenset({"heating", "dhw", "solar"})

# subscribe_nats: subjects must start with a known top-level stream prefix and
# carry at least two concrete tokens before any wildcard (no `knx.>` firehose).
_ALLOWED_PREFIXES = ("knx.", "ems-esp.", "solaredge-1.", "solaredge-2.", "warp.")
_SUBSCRIBE_MAX_MESSAGES = 200

# Wallbox subjects (confirmed against the live cluster):
#  - charger state is event-driven on warp.evse.state (publishes only on change);
#  - active power is cyclic on the charger's own meter, warp.meters.0.values
#    (a V/A/W-per-phase array; indices 6..8 are the three phase powers).
_WALLBOX_STATE_SUBJECT = "warp.evse.state"
_WALLBOX_POWER_SUBJECT = "warp.meters.0.values"
# get_current_knx resolves one JetStream read per matching GA; the semaphore
# keeps a broad filter (up to query_row_limit GAs) from opening thousands of
# concurrent requests against the NATS server at once.
_LAST_VALUE_CONCURRENCY = 20

_WALLBOX_STATE_FIELDS = (
    "charger_state",
    "iec61851_state",
    "allowed_charging_current",
    "error_state",
    "contactor_state",
    "lock_state",
)

_GA_RE = re.compile(r"^(\d{1,2})[/.](\d{1,2})[/.](\d{1,4})$")


# =====================================================================
# get_current_state
# =====================================================================


async def get_current_state(
    domain: str,
    identifier: str | None = None,
    *,
    stale_after_seconds: int,
) -> dict[str, Any]:
    """Current state of a homelab domain, read live from NATS JetStream.

    ``domain`` is one of ``knx`` | ``heating`` | ``dhw`` | ``solar`` |
    ``wallbox``. ``knx`` requires an ``identifier`` group address (``"1/2/3"``);
    ``solar`` accepts an optional inverter id (``"1"`` | ``"2"``).

    When JetStream holds a message for the domain → ``status: "ok"`` with the
    live ``state``, an ``as_of`` timestamp and a ``freshness_seconds`` age. For
    cyclic domains (heating/dhw/solar) a ``stale: true`` flag is added once the
    age exceeds ``stale_after_seconds`` (the sensor went quiet). Event-driven
    domains (knx/wallbox) omit ``stale`` — an old value is still current.

    When the subject was never seen on NATS → ``status: "unknown"`` with
    ``last_known_in_tsdb`` (the most recent matching TimescaleDB row).
    """
    key = domain.strip().lower()
    if key not in _STREAMS:
        raise ValueError(f"unknown_domain: {domain!r}; valid: {sorted(_STREAMS)}")

    m = metrics_module.get()
    try:
        state, newest = await _DISPATCH[key](identifier)
    except ValueError:
        raise  # validation errors are the caller's fault, not a fetch failure
    except Exception:
        m.nats_fetches.labels(domain=key, result="error").inc()
        raise

    if newest is None:
        m.nats_fetches.labels(domain=key, result="miss").inc()
        return {
            "status": "unknown",
            "domain": key,
            "reason": "no message retained on NATS",
            "last_known_in_tsdb": await _tsdb_fallback(key, identifier),
        }

    freshness = _age_seconds(newest)
    m.nats_state_age.labels(domain=key).observe(freshness)
    m.nats_fetches.labels(domain=key, result="ok").inc()
    resp: dict[str, Any] = {
        "status": "ok",
        "domain": key,
        "as_of": newest.isoformat(),
        "freshness_seconds": round(freshness, 3),
        "state": state,
    }
    if key in _CYCLIC_DOMAINS:
        resp["stale"] = freshness > stale_after_seconds
    return resp


# =====================================================================
# Per-domain collectors → (state fields, newest timestamp)
# =====================================================================


async def _knx(identifier: str | None) -> tuple[dict[str, Any], datetime | None]:
    if not identifier:
        raise ValueError("knx_requires_identifier: pass a group address like '1/2/3'")
    ga = _normalize_ga(identifier)
    msg = await nats_module.last_msg("KNX", "knx." + ga.replace("/", "."))
    if msg is None:
        return {}, None
    p = _payload(msg)
    state = {
        "ga": p.get("ga", ga),
        "name": p.get("name"),
        "dpt": p.get("dpt"),
        "value": p.get("value"),
    }
    return state, msg.ts


async def _heating(_identifier: str | None) -> tuple[dict[str, Any], datetime | None]:
    msg = await nats_module.last_msg("EMS_ESP", "ems-esp.boiler_data")
    if msg is None:
        return {}, None
    p = _payload(msg)
    state = {
        "flow_temp": p.get("curflowtemp"),
        "return_temp": p.get("rettemp"),
        "burner_power": p.get("curburnpow"),
        "heating_active": p.get("heatingactive"),
        "heating_pump": p.get("heatingpump"),
        "pump_status": p.get("pumpstatus"),
        "target_flow_temp": _first(p, "selflowtemp", "setflowtemp"),
    }
    return state, msg.ts


async def _dhw(_identifier: str | None) -> tuple[dict[str, Any], datetime | None]:
    # DHW fields are split across two subjects (confirmed in-cluster): storage
    # temp + setpoint live on ems-esp.boiler_data_dhw, the tap-water-active flag
    # on ems-esp.boiler_data. Read both and merge.
    dhw = await nats_module.last_msg("EMS_ESP", "ems-esp.boiler_data_dhw")
    boiler = await nats_module.last_msg("EMS_ESP", "ems-esp.boiler_data")
    newest = _max_ts(dhw, boiler)
    if newest is None:
        return {}, None
    dp = _payload(dhw)
    bp = _payload(boiler)
    state = {
        "dhw_storage_temp": dp.get("storagetemp1"),
        "dhw_setpoint": dp.get("seltemp"),
        "dhw_comfort_temp": dp.get("comforttemp"),
        "dhw_active": bp.get("tapwateractive"),
        "circulation": bp.get("circ"),
        "charging": bp.get("charging"),
    }
    return state, newest


async def _solar(identifier: str | None) -> tuple[dict[str, Any], datetime | None]:
    ids = [_inverter_id(identifier)] if identifier else [1, 2]
    inverters: dict[str, Any] = {}
    pv_total = 0.0
    pv_seen = False
    newest: datetime | None = None
    for inverter in ids:
        msg = await nats_module.last_msg("SOLAREDGE", f"solaredge-{inverter}.powerflow")
        if msg is None:
            continue
        p = _payload(msg)
        pv = p.get("pv_production")
        inverters[str(inverter)] = {
            "pv_power": pv,
            "grid_power": _nested(p, "grid", "power"),
            "battery_power": _nested(p, "battery", "power"),
            "consumer_house": _nested(p, "consumer", "house"),
            "consumer_evcharger": _nested(p, "consumer", "evcharger"),
        }
        if isinstance(pv, int | float):
            pv_total += float(pv)
            pv_seen = True
        newest = msg.ts if newest is None else max(newest, msg.ts)
    if newest is None:
        return {}, None
    return {"pv_power": pv_total if pv_seen else None, "inverters": inverters}, newest


async def _wallbox(_identifier: str | None) -> tuple[dict[str, Any], datetime | None]:
    # Charger state (event-driven) and active power (cyclic) come from different
    # subjects with very different update rates — keep their ages separate so the
    # caller can tell "idle since yesterday, drawing 0 W now" from stale data.
    evse = await nats_module.last_msg("WARP", _WALLBOX_STATE_SUBJECT)
    meter = await nats_module.last_msg("WARP", _WALLBOX_POWER_SUBJECT)
    newest = _max_ts(evse, meter)
    if newest is None:
        return {}, None
    ep = _payload(evse)
    state: dict[str, Any] = {field: ep.get(field) for field in _WALLBOX_STATE_FIELDS}
    state["state_age_seconds"] = round(_age_seconds(evse.ts), 1) if evse else None

    arr = _payload_list(meter)
    state["power_w"] = _sum_numeric(arr[6:9]) if arr is not None and len(arr) >= 9 else None
    state["power_age_seconds"] = round(_age_seconds(meter.ts), 1) if meter else None
    return state, newest


_DISPATCH: dict[str, Callable[[str | None], Awaitable[tuple[dict[str, Any], datetime | None]]]] = {
    "knx": _knx,
    "heating": _heating,
    "dhw": _dhw,
    "solar": _solar,
    "wallbox": _wallbox,
}


# =====================================================================
# TSDB fallback (last-known when JetStream never saw the subject)
# =====================================================================


async def _tsdb_fallback(domain: str, identifier: str | None) -> dict[str, Any] | None:
    stmt: db.Statement
    params: tuple[Any, ...]
    if domain == "heating":
        stmt, params, table = (
            "SELECT * FROM ems_esp WHERE topic = 'boiler_data' ORDER BY time DESC LIMIT 1",
            (),
            "ems_esp",
        )
    elif domain == "dhw":
        stmt, params, table = (
            "SELECT * FROM ems_esp WHERE topic = 'boiler_data_dhw' ORDER BY time DESC LIMIT 1",
            (),
            "ems_esp",
        )
    elif domain == "solar" and identifier:
        stmt, params, table = (
            "SELECT * FROM solaredge_powerflow WHERE inverter_id = %s ORDER BY time DESC LIMIT 1",
            (_inverter_id(identifier),),
            "solaredge_powerflow",
        )
    elif domain == "solar":
        stmt, params, table = (
            "SELECT * FROM solaredge_powerflow ORDER BY time DESC LIMIT 1",
            (),
            "solaredge_powerflow",
        )
    elif domain == "wallbox":
        stmt, params, table = (
            "SELECT * FROM warp_meter ORDER BY time DESC LIMIT 1",
            (),
            "warp_meter",
        )
    elif domain == "knx" and identifier:
        stmt, params, table = (
            "SELECT time, ga, dpt, value FROM knx WHERE ga = %s ORDER BY time DESC LIMIT 1",
            (_normalize_ga(identifier),),
            "knx",
        )
    else:
        return None

    try:
        newest = await db.lookup("get_current_state", table, stmt, params)
    except Exception as exc:  # fallback is best-effort
        log.warning("tsdb_fallback_failed", domain=domain, error=str(exc))
        return None
    return newest[0] if newest else None


# =====================================================================
# subscribe_nats
# =====================================================================


async def subscribe_nats(
    subject: str,
    duration_seconds: int = 10,
    *,
    max_duration_seconds: int,
) -> dict[str, Any]:
    """Tail a NATS subject for a short window and return the collected messages.

    Token-expensive — meant for "watch this for a moment" requests. ``subject``
    must start with a known stream prefix (``knx.`` | ``ems-esp.`` |
    ``solaredge-1.`` | ``solaredge-2.`` | ``warp.``) and carry at least two
    concrete tokens before any wildcard, so top-level firehoses like ``knx.>``
    are rejected. ``duration_seconds`` is hard-capped at ``max_duration_seconds``.
    """
    subject = subject.strip()
    _validate_subscribe_subject(subject)
    if duration_seconds <= 0:
        raise ValueError(f"invalid_duration: {duration_seconds}; must be > 0")
    if duration_seconds > max_duration_seconds:
        raise ValueError(f"duration_too_long: {duration_seconds} > {max_duration_seconds}s cap")

    msgs = await nats_module.tail(subject, float(duration_seconds), _SUBSCRIBE_MAX_MESSAGES)
    prefix = subject.split(".", 1)[0]
    metrics_module.get().nats_subscribe_messages.labels(prefix=prefix).inc(len(msgs))
    return {
        "subject": subject,
        "duration_seconds": duration_seconds,
        "message_count": len(msgs),
        "messages": [
            {"subject": msg.subject, "ts": msg.ts.isoformat(), "data": _decode(msg)} for msg in msgs
        ],
    }


def _validate_subscribe_subject(subject: str) -> None:
    if not any(subject.startswith(prefix) for prefix in _ALLOWED_PREFIXES):
        raise ValueError(
            f"subject_not_allowed: {subject!r}; must start with one of {_ALLOWED_PREFIXES}"
        )
    for index, token in enumerate(subject.split(".")):
        if token in (">", "*"):
            if index < 2:
                raise ValueError(
                    f"wildcard_too_broad: {subject!r}; need >=2 concrete tokens before a wildcard"
                )
            break


# =====================================================================
# get_current_knx — "what's on right now?" across many group addresses
# =====================================================================


async def get_current_knx(
    room: str | None = None,
    function: str | None = None,
    name: str | None = None,
    only_active: bool = False,
    limit: int = 200,
) -> dict[str, Any]:
    """Current value of KNX group addresses matching a filter, read live from NATS.

    Answers "is the living-room light on right now?" / "which lights are on?" —
    use this instead of ``query_knx_events`` for *current* state. It reads the
    last retained value per group address from JetStream, so it works even for
    GAs that only publish on change (lights, switches) and would be missing from
    a recent event-log window.

    Filters (all optional, AND-combined) resolve the relevant GAs via the
    catalog: ``room`` (exact), ``function`` (exact, e.g. ``"Beleuchtung"``),
    ``name`` (substring, ``ILIKE``). GAs with no retained NATS message are
    omitted.

    Every state carries its ``role``: ``status`` for a ``-Status`` datapoint,
    ``command`` for the datapoint it reports on (``Ein/Aus`` next to
    ``Ein/Aus-Status``, ``Dimmen-Absolut`` next to ``Dimmen-Status``), whose
    retained value is the last order sent and not the device now, and
    ``reading`` for everything else (sensors, meters, diagnostics).
    ``only_active=True`` keeps only GAs whose current value is "on" (boolean
    true or a number > 0) and that report a state, never a command.
    """
    where: list[sql.Composable] = []
    params: list[Any] = []
    if room is not None:
        where.append(sql.SQL("room = %s"))
        params.append(room)
    if function is not None:
        where.append(sql.SQL("function = %s"))
        params.append(function)
    if name is not None:
        where.append(sql.SQL("name ILIKE %s"))
        params.append(f"%{name}%")
    clause = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where) if where else sql.SQL("")
    catalog_sql = sql.SQL(
        "SELECT ga, name AS ga_name, room, function, dpt FROM ga_catalog{clause} ORDER BY ga"
    ).format(clause=clause)
    matched = await db.read(
        "get_current_knx", "ga_catalog", catalog_sql, params, limit=limit, overflow="truncate"
    )
    catalog = matched.rows

    filter_used = {"room": room, "function": function, "name": name, "only_active": only_active}
    if not catalog:
        return {
            "count": 0,
            "filter": filter_used,
            "states": [],
            "note": "no group addresses match the filter; check room/function/name first",
        }

    roles = await _ga_roles([row["ga_name"] for row in catalog])
    by_ga = await _knx_last_values([row["ga"] for row in catalog])
    metrics_module.get().nats_fetches.labels(domain="knx", result="ok").inc()

    states: list[dict[str, Any]] = []
    for row in catalog:
        current = by_ga.get(row["ga"])
        if current is None:
            continue  # never retained on NATS
        value, ts = current
        role = roles[row["ga_name"]]
        if only_active and (role == "command" or not _is_on(value)):
            continue
        states.append(
            {
                "ga": row["ga"],
                "name": row["ga_name"],
                "room": row["room"],
                "function": row["function"],
                "role": role,
                "value": value,
                "age_seconds": round(_age_seconds(ts), 1),
            }
        )

    return {
        "count": len(states),
        "filter": filter_used,
        "states": states,
        "note": (
            "current value per GA from NATS (last message); GAs without one are omitted; "
            "a command's value is the last order sent, the device's state is its -Status sibling"
        ),
    }


_STATUS_SUFFIX = "-Status"
_ANOMALY_SUFFIX = "-Anomalie"
_DEVICE_DATAPOINTS_SQL = "SELECT name FROM ga_catalog WHERE name LIKE ANY(%s)"


async def _ga_roles(names: list[str]) -> dict[str, str]:
    """Role per catalog name from the house convention ``Function.Device.Datapoint``.

    A ``-Status`` datapoint is the state of its device; a sibling it reports on
    (the same datapoint without the suffix, or one that extends it with a dash:
    ``Dimmen-Absolut`` for ``Dimmen-Status``) is a command; everything else is a
    reading. Siblings come from the whole catalog, not the caller's filter, so a
    lone ``Dimmen-Absolut`` match still knows it is a command.
    """
    devices = {name.rpartition(".")[0] for name in names}
    rows = await db.lookup(
        "get_current_knx",
        "ga_catalog",
        _DEVICE_DATAPOINTS_SQL,
        ([f"{device}.%" for device in devices],),
    )
    datapoints: dict[str, set[str]] = {device: set() for device in devices}
    for row in rows:
        device, _, datapoint = row["name"].rpartition(".")
        if device in datapoints:
            datapoints[device].add(datapoint)

    roles: dict[str, str] = {}
    for name in names:
        device, _, datapoint = name.rpartition(".")
        bases = [
            point.removesuffix(_STATUS_SUFFIX)
            for point in datapoints[device]
            if point.endswith(_STATUS_SUFFIX)
        ]
        if datapoint.endswith(_STATUS_SUFFIX):
            roles[name] = "status"
        elif datapoint.endswith(_ANOMALY_SUFFIX):
            roles[name] = "reading"
        elif any(datapoint == base or datapoint.startswith(base + "-") for base in bases):
            roles[name] = "command"
        else:
            roles[name] = "reading"
    return roles


async def _knx_last_values(gas: list[str]) -> dict[str, tuple[Any, datetime]]:
    """Map each ga (`m/h/s`) → (current value, timestamp) via per-GA get_last_msg.

    One direct JetStream read per GA (no consumer, no ack), run concurrently
    but bounded by ``_LAST_VALUE_CONCURRENCY`` — works with the MCP server's
    subscribe-only nkey ($JS.API + _INBOX, no $JS.ACK).
    """
    sem = asyncio.Semaphore(_LAST_VALUE_CONCURRENCY)

    async def _one(ga: str) -> tuple[str, nats_module.StateMsg | None]:
        async with sem:
            return ga, await nats_module.last_msg("KNX", "knx." + ga.replace("/", "."))

    results = await asyncio.gather(*[_one(ga) for ga in gas])
    out: dict[str, tuple[Any, datetime]] = {}
    for ga, msg in results:
        if msg is not None:
            out[ga] = (_payload(msg).get("value"), msg.ts)
    return out


def _is_on(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value > 0
    return False


# =====================================================================
# Payload + timestamp helpers
# =====================================================================


def _payload(msg: nats_module.StateMsg | None) -> dict[str, Any]:
    if msg is None:
        return {}
    try:
        data = json.loads(msg.data)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _payload_list(msg: nats_module.StateMsg | None) -> list[Any] | None:
    if msg is None:
        return None
    try:
        data = json.loads(msg.data)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, list) else None


def _decode(msg: nats_module.StateMsg) -> Any:
    try:
        return json.loads(msg.data)
    except json.JSONDecodeError:
        return msg.data.decode("utf-8", errors="replace")


def _nested(payload: dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _first(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return value
    return None


def _sum_numeric(values: list[Any]) -> float | None:
    numbers = [float(v) for v in values if isinstance(v, int | float)]
    return sum(numbers) if numbers else None


def _max_ts(*msgs: nats_module.StateMsg | None) -> datetime | None:
    times = [m.ts for m in msgs if m is not None]
    return max(times) if times else None


def _age_seconds(ts: datetime) -> float:
    return (datetime.now(UTC) - ts).total_seconds()


def _normalize_ga(identifier: str) -> str:
    match = _GA_RE.match(identifier.strip())
    if not match:
        raise ValueError(f"invalid_ga: {identifier!r}; expected 'main/middle/sub' like '1/2/3'")
    return "/".join(match.groups())


def _inverter_id(identifier: str | None) -> int:
    try:
        value = int(str(identifier).strip())
    except ValueError:
        raise ValueError(f"invalid_inverter: {identifier!r}; expected 1 or 2") from None
    if value not in (1, 2):
        raise ValueError(f"invalid_inverter: {identifier!r}; expected 1 or 2")
    return value
