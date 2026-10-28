"""Shared fixtures: a seeded TimescaleDB testcontainer, settings, and the DB pool."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import LiteralString

import psycopg
import pytest
import pytest_asyncio
import respx
import structlog
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from testcontainers.postgres import PostgresContainer

from lares_mcp_bridge import db, tracing
from lares_mcp_bridge.config import Settings
from lares_mcp_bridge.tools import sources, trigger

from .support import GATEWAY_KEY, TRIGGER_KEY, TRIGGER_URL, connect

# One recording provider for the whole session: the global can only be set once.
_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_exporter))
trace.set_tracer_provider(_provider)
tracing.instrument()


@pytest.fixture
def clean_context() -> Iterator[None]:
    """No caller bound before the test, none left behind after it."""
    structlog.contextvars.clear_contextvars()
    try:
        yield
    finally:
        structlog.contextvars.clear_contextvars()


@pytest.fixture
def spans() -> Iterator[InMemorySpanExporter]:
    """The spans finished in this test, every one sampled."""
    _exporter.clear()
    yield _exporter
    _exporter.clear()


@pytest.fixture(autouse=True)
def _fresh_sources_cache() -> None:
    """Drop the data-source catalog TTL cache so tests stay order-independent."""
    sources.invalidate_cache()


# TimescaleDB image with the extension preinstalled.
TIMESCALEDB_IMAGE = "timescale/timescaledb:latest-pg17"


@pytest.fixture(scope="session")
def timescaledb_container() -> Iterator[PostgresContainer]:
    container = PostgresContainer(
        TIMESCALEDB_IMAGE, username="test", password="test", dbname="homelab"
    )
    container.start()
    try:
        # 1) Extension + tables + seed (regular DDL/DML, autocommit-safe).
        conn = connect(container)
        try:
            conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            _seed(conn)
        finally:
            conn.close()

        # 2) Continuous aggregates. `CALL refresh_continuous_aggregate()` must
        # run outside any transaction block — fresh connection per CAGG keeps
        # psycopg from wrapping them in an implicit BEGIN.
        for cagg_sql, refresh_sql in _CAGGS:
            conn = connect(container)
            try:
                conn.execute(cagg_sql)
                conn.execute(refresh_sql)
            finally:
                conn.close()

        yield container
    finally:
        container.stop()


def _seed(conn: psycopg.Connection) -> None:
    """Set up the hypertables, lookup tables, and views the tools query."""
    # KNX hypertable + catalog table + view that joins them.
    conn.execute(
        """
        CREATE TABLE knx (
            time TIMESTAMPTZ NOT NULL,
            ga   TEXT NOT NULL,
            knx_main   SMALLINT,
            knx_middle SMALLINT,
            knx_sub    SMALLINT,
            dpt   TEXT,
            value DOUBLE PRECISION,
            raw   JSONB,
            knx_name TEXT
        )
        """
    )
    conn.execute("SELECT create_hypertable('knx', by_range('time'))")

    conn.execute(
        """
        CREATE TABLE ga_catalog (
            ga          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            room        TEXT,
            function    TEXT,
            description TEXT,
            dpt         TEXT NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW ga_catalog_view AS
        SELECT k.time, k.ga, k.knx_main, k.knx_middle, k.knx_sub, k.dpt, k.value,
               n.name AS ga_name, n.room, n.function, n.description
        FROM knx k LEFT JOIN ga_catalog n USING (ga)
        """
    )

    # ems_esp boiler hypertable. Tools key off `topic = 'boiler_data'` and the
    # typed `curburnpow` column.
    conn.execute(
        """
        CREATE TABLE ems_esp (
            time       TIMESTAMPTZ NOT NULL,
            topic      TEXT NOT NULL,
            curburnpow DOUBLE PRECISION,
            curtemp    DOUBLE PRECISION,
            raw        JSONB NOT NULL
        )
        """
    )
    conn.execute("SELECT create_hypertable('ems_esp', by_range('time'))")

    # solaredge powerflow hypertable.
    conn.execute(
        """
        CREATE TABLE solaredge_powerflow (
            time             TIMESTAMPTZ NOT NULL,
            inverter_id      SMALLINT    NOT NULL,
            pv_production    DOUBLE PRECISION,
            grid_power       DOUBLE PRECISION,
            grid_consumption DOUBLE PRECISION,
            grid_delivery    DOUBLE PRECISION,
            consumer_total   DOUBLE PRECISION,
            battery_charge   DOUBLE PRECISION,
            battery_discharge DOUBLE PRECISION,
            consumer_used_pv_production      DOUBLE PRECISION,
            consumer_used_battery_production DOUBLE PRECISION,
            PRIMARY KEY (time, inverter_id)
        )
        """
    )
    conn.execute("SELECT create_hypertable('solaredge_powerflow', by_range('time'))")

    # WARP meter hypertable.
    conn.execute(
        """
        CREATE TABLE warp_meter (
            time      TIMESTAMPTZ NOT NULL,
            sub_topic TEXT        NOT NULL,
            meter_id  SMALLINT,
            power_l1  DOUBLE PRECISION,
            power_l2  DOUBLE PRECISION,
            power_l3  DOUBLE PRECISION,
            voltage_l1 DOUBLE PRECISION,
            current_l1 DOUBLE PRECISION,
            voltage_l2 DOUBLE PRECISION,
            voltage_l3 DOUBLE PRECISION,
            current_l2 DOUBLE PRECISION,
            current_l3 DOUBLE PRECISION
        )
        """
    )
    conn.execute("SELECT create_hypertable('warp_meter', by_range('time'))")

    # Catalog seed — 12 GAs covering the categories the tools filter on; the
    # kitchen light is one device with command and -Status datapoint pairs.
    conn.execute(
        """
        INSERT INTO ga_catalog (ga, name, room, function, dpt, description) VALUES
            ('1/2/0',  'Sensors.1F.Bedroom.Temperature',   'Bedroom',    'Sensors',  '9.001', NULL),
            ('1/2/1',  'Sensors.1F.Bedroom.Humidity',      'Bedroom',    'Climate',  '9.007', NULL),
            ('1/2/2',  'Lighting.1F.Bedroom.Ceiling',      'Bedroom',    'Lighting', '1.001', NULL),
            ('1/2/3',  'Sensors.GF.LivingRoom.Temperature','LivingRoom', 'Sensors',  '9.001', NULL),
            ('1/2/4',  'Sensors.GF.LivingRoom.Humidity',   'LivingRoom', 'Climate',  '9.007', NULL),
            ('1/2/100','Lighting.GF.LivingRoom.Ceiling',   'LivingRoom', 'Lighting', '1.001', NULL),
            ('1/2/200','Security.GF.LivingRoom.Motion',    'LivingRoom', 'Motion',   '1.018', NULL),
            ('1/2/300','General.Central.Presence',         NULL,         NULL,       '1.011', NULL),
            ('1/3/0',  'Lighting.GF.Kitchen.Switch',       'Kitchen',    'Lighting', '1.001', NULL),
            ('1/3/1',  'Lighting.GF.Kitchen.Switch-Status','Kitchen',    'Lighting', '1.011', NULL),
            ('1/3/2',  'Lighting.GF.Kitchen.Dim-Absolute', 'Kitchen',    'Lighting', '5.001', NULL),
            ('1/3/3',  'Lighting.GF.Kitchen.Dim-Status',   'Kitchen',    'Lighting', '5.001', NULL)
        """
    )

    # One room on two floors, named the way production names them: the room
    # column carries the ETS space id, so these are two rooms that merely
    # share a word. `Flur` exists three times in the real catalog, one per
    # storey, and the sibling query must not mix them.
    conn.execute(
        """
        INSERT INTO ga_catalog (ga, name, room, function, dpt, description) VALUES
            ('1/4/0', 'Lighting.KG.Flur.Ceiling', 'Flur (K1)', 'Lighting', '1.001', NULL),
            ('1/4/1', 'Sensors.KG.Flur.Motion',   'Flur (K1)', 'Motion',   '1.018', NULL),
            ('1/4/2', 'Lighting.EG.Flur.Ceiling', 'Flur (E1)', 'Lighting', '1.001', NULL),
            ('1/4/3', 'Sensors.EG.Flur.Motion',   'Flur (E1)', 'Motion',   '1.018', NULL),
            ('1/4/9', 'General.EG.Flur.Scenes',   'Flur (E1)', 'Allgemein','17.001', NULL)
        """
    )

    # A second device in the kitchen: an appliance whose power reading an
    # episode is measured on, with a switch command and its status.
    conn.execute(
        """
        INSERT INTO ga_catalog (ga, name, room, function, dpt) VALUES
            ('1/3/10', 'Appliance.GF.Kitchen.Freezer.Power', 'Kitchen', 'Appliance', '7.012'),
            ('1/3/11', 'Appliance.GF.Kitchen.Freezer.Switch', 'Kitchen', 'Appliance', '1.001'),
            ('1/3/12', 'Appliance.GF.Kitchen.Freezer.Switch-Status', 'Kitchen', 'Appliance',
             '1.011')
        """
    )

    # Presence: one datapoint per person, named and typed as production has
    # them, written only on change. Anna and Ben carry a state from before
    # the test window; Dora's first change falls inside it, Cleo never wrote
    # one. Anna's stream title is a Person datapoint that is not presence.
    conn.execute(
        """
        INSERT INTO ga_catalog (ga, name, room, function, dpt, description) VALUES
            ('18/1/0', 'Person.Anna.Präsenz',             'Anwesen', 'Person', '1.011',  NULL),
            ('18/1/9', 'Person.Anna.Stream.Titel-Status', 'Anwesen', 'Person', '16.001', NULL),
            ('18/2/0', 'Person.Ben.Präsenz',              'Anwesen', 'Person', '1.011',  NULL),
            ('18/3/0', 'Person.Cleo.Präsenz',             'Anwesen', 'Person', '1.011',  NULL),
            ('18/4/0', 'Person.Dora.Präsenz',             'Anwesen', 'Person', '1.011',  NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO knx (time, ga, knx_main, knx_middle, knx_sub, dpt, value) VALUES
            ('2026-08-31T20:00:00+00:00', '18/1/0', 18, 1, 0, '1.011', 1),
            ('2026-09-01T07:00:00+00:00', '18/1/0', 18, 1, 0, '1.011', 0),
            ('2026-09-01T07:00:30+00:00', '18/1/0', 18, 1, 0, '1.011', 0),
            ('2026-09-01T17:00:00+00:00', '18/1/0', 18, 1, 0, '1.011', 1),
            ('2026-09-01T08:00:00+00:00', '18/1/9', 18, 1, 9, '16.001', 0),
            ('2026-08-31T22:00:00+00:00', '18/2/0', 18, 2, 0, '1.011', 1),
            ('2026-09-01T09:00:00+00:00', '18/2/0', 18, 2, 0, '1.011', 0),
            ('2026-09-01T12:00:00+00:00', '18/2/0', 18, 2, 0, '1.011', 1),
            ('2026-09-01T10:00:00+00:00', '18/4/0', 18, 4, 0, '1.011', 0),
            ('2026-09-01T11:00:00+00:00', '18/4/0', 18, 4, 0, '1.011', 1)
        """
    )

    # KNX seed — 200 readings spread across the catalog GAs (i % 5 picks 0..4
    # which maps to 1/2/0..1/2/4 — temp/humidity in two rooms).
    conn.execute(
        """
        INSERT INTO knx
        SELECT
            NOW() - (i || ' minutes')::interval,
            '1/2/' || (i % 5),
            1, 2, (i % 5),
            '9.001',
            20 + (i % 10),
            jsonb_build_object('value', 20 + (i % 10), 'unit', 'celsius')
        FROM generate_series(0, 200) AS i
        """
    )

    # ems_esp seed — every 3rd row burns at 50 % modulation, otherwise 0. That gives
    # ~67 distinct ON/OFF cycles in 200 rows for heating_cycles to detect.
    conn.execute(
        """
        INSERT INTO ems_esp (time, topic, curburnpow, curtemp, raw)
        SELECT
            NOW() - (i || ' minutes')::interval,
            'boiler_data',
            CASE WHEN i % 3 = 0 THEN 50 ELSE 0 END,
            35 + (i % 20),
            jsonb_build_object(
                'flow_temp', 35 + (i % 20),
                'return_temp', 30 + (i % 15),
                'burner_power', CASE WHEN i % 3 = 0 THEN 50 ELSE 0 END
            )
        FROM generate_series(0, 200) AS i
        """
    )

    # solaredge_powerflow seed — daily cycle of PV production peaking around hour
    # 12, plus matching grid/consumer/battery flows. 168 hours = 7 days.
    conn.execute(
        """
        INSERT INTO solaredge_powerflow
        SELECT
            NOW() - (i || ' hours')::interval,
            1::smallint,
            GREATEST(0, 5000 * sin(radians(((i % 24) - 6) * 15))),
            -1500 + (i % 2000),
            500 + (i % 800),
            500 + (i % 800),
            2000 + (i % 1500),
            500 + (i % 1000),
            300 + (i % 800)
        FROM generate_series(0, 168) AS i
        """
    )

    # warp_meter seed — wallbox power follows a similar diurnal pattern but
    # offset, so correlate_events has something to find.
    conn.execute(
        """
        INSERT INTO warp_meter
        SELECT
            NOW() - (i || ' hours')::interval,
            'warp.meters.0.values',
            0::smallint,
            GREATEST(0, 1500 * sin(radians(((i % 24) - 8) * 15))),
            GREATEST(0, 1500 * sin(radians(((i % 24) - 8) * 15))),
            GREATEST(0, 1500 * sin(radians(((i % 24) - 8) * 15))),
            230,
            GREATEST(0, 6 * sin(radians(((i % 24) - 8) * 15)))
        FROM generate_series(0, 168) AS i
        """
    )

    # A fixed day for exact energy sums: two inverters at a steady 1000 W and
    # 500 W all day, the wallbox's meter 0 at 3600 W for its first ten hours,
    # and meter 1 reporting the same hours without phase values.
    conn.execute(
        """
        INSERT INTO solaredge_powerflow (time, inverter_id, pv_production, grid_power,
                                         consumer_total, battery_charge,
                                         consumer_used_battery_production)
        SELECT '2026-09-02T00:30:00+00:00'::timestamptz + (h || ' hours')::interval,
               inv.id, inv.watts, 0, inv.watts - 100, 0, 0
        FROM generate_series(0, 23) AS h,
             (VALUES (1::smallint, 1000), (2::smallint, 500)) AS inv (id, watts)
        """
    )
    conn.execute(
        """
        INSERT INTO warp_meter (time, sub_topic, meter_id, power_l1, power_l2, power_l3)
        SELECT '2026-09-02T00:30:00+00:00'::timestamptz + (h || ' hours')::interval,
               m.topic, m.id, m.watts, m.watts, m.watts
        FROM generate_series(0, 9) AS h,
             (VALUES ('warp.meters.0.values', 0::smallint, 1200),
                     ('warp.meters.1.update', 1::smallint, NULL)) AS m (topic, id, watts)
        """
    )

    # unifi_events hypertable — Security-Archive of UniFi Protect Alarm Manager
    # triggers. Columns mirror prod (see lares bootstrap.sql).
    conn.execute(
        """
        CREATE TABLE unifi_events (
            time           TIMESTAMPTZ NOT NULL,
            camera         TEXT        NOT NULL,
            detection_type TEXT        NOT NULL,
            score          SMALLINT,
            event_type     TEXT,
            value          TEXT,
            event_id       TEXT,
            event_link     TEXT,
            raw            JSONB
        )
        """
    )
    conn.execute("SELECT create_hypertable('unifi_events', by_range('time'))")

    # Seed: mix cameras, detection_types, scores so filters can be exercised
    # in isolation. 40 rows over the last 40 minutes.
    conn.execute(
        """
        INSERT INTO unifi_events
        SELECT
            NOW() - (i || ' minutes')::interval,
            (ARRAY['fassade','eingang','terrasse_wohnzimmer','terrasse_esszimmer'])[1 + (i % 4)],
            (ARRAY['person','motion','vehicle','face_known'])[1 + (i % 4)],
            ((i * 7) % 101)::smallint,
            (ARRAY['smartDetectZone','smartDetectLine','motion','smartDetectZone'])[1 + (i % 4)],
            CASE WHEN i % 4 = 3 THEN 'Alexander Zimmermann' ELSE NULL END,
            'evt-' || i,
            'https://192.168.1.1/protect/events/event/evt-' || i,
            jsonb_build_object('seed', true, 'idx', i)
        FROM generate_series(0, 39) AS i
        """
    )

    # mcp_forecasts — the batch jobs' write target. Schema mirrors the
    # production bootstrap.sql.
    conn.execute(
        """
        CREATE TABLE mcp_forecasts (
            forecast_for   TIMESTAMPTZ      NOT NULL,
            created_at     TIMESTAMPTZ      NOT NULL DEFAULT now(),
            source         TEXT             NOT NULL,
            metric         TEXT             NOT NULL,
            model          TEXT             NOT NULL,
            forecast_value DOUBLE PRECISION,
            forecast_lower DOUBLE PRECISION,
            forecast_upper DOUBLE PRECISION,
            PRIMARY KEY (forecast_for, source, metric, model)
        )
        """
    )
    conn.execute("SELECT create_hypertable('mcp_forecasts', by_range('forecast_for'))")

    # Episode tables — the detection chain's episode layer, mirroring the
    # production bootstrap.sql. `episode_verdicts` keys on episode_id, so a
    # second verdict on the same episode overwrites rather than duplicates.
    conn.execute(
        """
        CREATE TABLE episodes (
            id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            fault        TEXT             NOT NULL,
            subject      TEXT             NOT NULL,
            entity_kind  TEXT             CHECK (entity_kind IN ('channel', 'room', 'plant')),
            entity_ref   TEXT,
            started_at   TIMESTAMPTZ      NOT NULL,
            last_seen_at TIMESTAMPTZ      NOT NULL,
            ended_at     TIMESTAMPTZ,
            severity     SMALLINT         NOT NULL CHECK (severity BETWEEN 1 AND 3),
            peak_score   DOUBLE PRECISION NOT NULL,
            folded       BOOLEAN          NOT NULL DEFAULT false,
            externally_delivered BOOLEAN  NOT NULL DEFAULT false,
            fingerprint  TEXT,
            created_at   TIMESTAMPTZ      NOT NULL DEFAULT now()
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE episode_verdicts (
            episode_id BIGINT      PRIMARY KEY REFERENCES episodes (id),
            verdict    TEXT        NOT NULL CHECK (verdict IN ('real', 'nonsense')),
            decided_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )

    # Verbatim from bootstrap.sql: the readers go through it, so the mirror
    # has to resolve a subject exactly as production does.
    conn.execute(
        r"""
CREATE OR REPLACE VIEW episode_view AS
WITH resolved AS (
    SELECT e.*,
           COALESCE(
               e.entity_kind,
               CASE WHEN e.subject ~ '[0-9]+/[0-9]+/[0-9]+' THEN 'channel' END
           ) AS kind,
           COALESCE(
               e.entity_ref, substring(e.subject from '[0-9]+/[0-9]+/[0-9]+')
           ) AS ref
    FROM episodes e
)
SELECT r.id, r.fault, r.subject, r.kind AS entity_kind, r.ref AS entity_ref,
       -- Only a channel fault was measured on the channel the ref names;
       -- for the others the ref merely locates the room.
       CASE WHEN r.kind = 'channel' THEN c.ga END   AS channel_ga,
       CASE WHEN r.kind = 'channel' THEN c.name END AS channel_name,
       c.room,
       -- Falls back the way it always did when the catalog does not know
       -- the address: the bracketed label, then the subject itself.
       COALESCE(
           CASE r.kind WHEN 'channel' THEN c.name WHEN 'room' THEN c.room END,
           substring(r.subject from '\[([^]]+)\]'),
           r.subject
       ) AS affected,
       r.severity, r.started_at, r.last_seen_at, r.ended_at,
       r.peak_score, r.folded, r.externally_delivered, r.fingerprint,
       v.verdict, v.decided_at
FROM resolved r
LEFT JOIN ga_catalog c ON c.ga = r.ref
LEFT JOIN episode_verdicts v ON v.episode_id = r.id;
        """
    )

    # Episode seed — four episodes across two faults. `1/2/2` and `1/2/3`
    # are catalog GAs so the subject resolves to a name and a room; the
    # fourth subject deliberately carries no GA at all.
    conn.execute(
        """
        INSERT INTO episodes (fault, subject, started_at, last_seen_at, ended_at,
                              severity, peak_score, folded)
        VALUES
            ('silence',   'knx [1/2/2]', NOW() - INTERVAL '5 hours',
             NOW() - INTERVAL '1 hour',  NULL,                       3, 9.5,  false),
            ('silence',   'knx [1/2/3]', NOW() - INTERVAL '3 days',
             NOW() - INTERVAL '3 days',  NOW() - INTERVAL '2 days',  1, 1.25, false),
            ('constancy', 'knx [1/2/2]', NOW() - INTERVAL '9 days',
             NOW() - INTERVAL '9 days',  NOW() - INTERVAL '8 days',  2, 4.0,  false),
            ('constancy', 'ems boiler',  NOW() - INTERVAL '40 days',
             NOW() - INTERVAL '39 days', NOW() - INTERVAL '39 days', 2, 3.5,  true),
            -- Subject as the engine writes it in production: the bare group
            -- address, here on the floor-ambiguous room.
            ('channel_silence', '1/4/2', NOW() - INTERVAL '60 days',
             NOW() - INTERVAL '60 days', NOW() - INTERVAL '59 days', 2, 5.0, false)
        """
    )

    # A fault measured on a room, the way the engine writes it: the subject
    # is its own slug, and the entity columns name the room and an address
    # the catalog resolves to it.
    conn.execute(
        """
        INSERT INTO episodes (fault, subject, entity_kind, entity_ref,
                              started_at, last_seen_at, ended_at,
                              severity, peak_score, folded)
        VALUES ('fbh_cold', 'eg-buero', 'room', '1/2/0',
                NOW() - INTERVAL '2 days', NOW() - INTERVAL '2 days',
                NOW() - INTERVAL '2 days', 1, 1.2, false)
        """
    )

    # The episode's trajectory and its notification events. Only the two
    # `silence` episodes carry them, so a bundle with nothing underneath it
    # stays testable on the `constancy` ones.
    conn.execute(
        """
        CREATE TABLE episode_observations (
            episode_id BIGINT           NOT NULL REFERENCES episodes (id),
            time       TIMESTAMPTZ      NOT NULL,
            score      DOUBLE PRECISION NOT NULL,
            severity   SMALLINT         NOT NULL CHECK (severity BETWEEN 1 AND 3),
            value      DOUBLE PRECISION,
            PRIMARY KEY (episode_id, time)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE episode_events (
            episode_id BIGINT      NOT NULL REFERENCES episodes (id),
            kind       TEXT        NOT NULL CHECK (kind IN ('appeared', 'escalated', 'ended')),
            time       TIMESTAMPTZ NOT NULL,
            severity   SMALLINT    NOT NULL CHECK (severity BETWEEN 0 AND 3),
            PRIMARY KEY (episode_id, kind)
        )
        """
    )

    # Five hourly observations on the open episode, the score climbing into
    # the escalation, plus one on the ended one.
    conn.execute(
        """
        INSERT INTO episode_observations (episode_id, time, score, severity, value)
        SELECT e.id,
               NOW() - ((5 - i) || ' hours')::interval,
               2.0 + i * 1.875,
               CASE WHEN i >= 2 THEN 3 ELSE 2 END,
               21.5 + i
        FROM episodes e, generate_series(0, 4) AS i
        WHERE e.fault = 'silence' AND e.ended_at IS NULL
        """
    )
    conn.execute(
        """
        INSERT INTO episode_observations (episode_id, time, score, severity, value)
        SELECT e.id, e.started_at, e.peak_score, e.severity, 19.0
        FROM episodes e
        WHERE e.fault = 'silence' AND e.subject = 'knx [1/2/3]'
        """
    )

    # A long episode on the kitchen freezer, older than every list window the
    # other tests use: 60 hourly observations, more than the bundle carries.
    conn.execute(
        """
        INSERT INTO episodes (fault, subject, started_at, last_seen_at, ended_at,
                              severity, peak_score, folded)
        VALUES ('freezer_icing', '1/3/10', NOW() - INTERVAL '400 days',
                NOW() - INTERVAL '397 days', NOW() - INTERVAL '397 days', 2, 2.7, false)
        """
    )
    conn.execute(
        """
        INSERT INTO episode_observations (episode_id, time, score, severity, value)
        SELECT e.id, e.started_at + (i || ' hours')::interval, 2.0 + i / 100.0, 2, 38.0
        FROM episodes e, generate_series(0, 59) AS i
        WHERE e.fault = 'freezer_icing'
        """
    )
    conn.execute(
        """
        INSERT INTO episode_events (episode_id, kind, time, severity)
        SELECT e.id, 'appeared', e.started_at, 2
        FROM episodes e WHERE e.fault = 'silence' AND e.ended_at IS NULL
        UNION ALL
        SELECT e.id, 'escalated', NOW() - INTERVAL '3 hours', 3
        FROM episodes e WHERE e.fault = 'silence' AND e.ended_at IS NULL
        UNION ALL
        SELECT e.id, 'appeared', e.started_at, 1
        FROM episodes e WHERE e.fault = 'silence' AND e.subject = 'knx [1/2/3]'
        UNION ALL
        SELECT e.id, 'ended', e.ended_at, 0
        FROM episodes e WHERE e.fault = 'silence' AND e.subject = 'knx [1/2/3]'
        """
    )

    # Ledger and memory — the platform's run table, written by the trigger
    # service, read here and column-updated by the verdict role.
    conn.execute(
        """
        CREATE TABLE agent_runs (
            id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            use_case       TEXT           NOT NULL,
            trigger        TEXT           NOT NULL
                CHECK (trigger IN ('event', 'schedule', 'message', 'manual')),
            subject_kind   TEXT           NOT NULL
                CHECK (subject_kind IN ('episode', 'alert_group', 'chat', 'none')),
            subject_key    TEXT,
            session_id     TEXT,
            enduser_id     TEXT,
            surface        TEXT           CHECK (surface IN ('discord', 'web')),
            harness_run_id TEXT,
            trace_id       TEXT,
            status         TEXT           NOT NULL
                CHECK (status IN ('queued', 'running', 'completed', 'failed', 'capped')),
            attempt        SMALLINT       NOT NULL DEFAULT 1,
            error          TEXT,
            tldr           TEXT,
            text           TEXT,
            language       TEXT,
            output_ref     TEXT[]         NOT NULL DEFAULT '{}',
            output_state   TEXT[]         NOT NULL DEFAULT '{}',
            model_source   TEXT,
            model          TEXT,
            tokens_in      INTEGER,
            tokens_out     INTEGER,
            cost           NUMERIC(10, 6),
            duration       INTERVAL,
            tool_trace     JSONB,
            verdict        TEXT           CHECK (verdict IN ('helpful', 'useless')),
            verdict_at     TIMESTAMPTZ,
            created_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),
            finished_at    TIMESTAMPTZ,
            CONSTRAINT agent_runs_output_positions
                CHECK (cardinality(output_ref) = cardinality(output_state)),
            CONSTRAINT agent_runs_output_state_values
                CHECK (array_remove(output_state, NULL) <@ ARRAY['open', 'merged', 'closed'])
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX agent_runs_subject_idx
            ON agent_runs (use_case, subject_kind, subject_key)
        """
    )
    conn.execute(
        """
        CREATE INDEX agent_runs_use_case_created_at_idx
            ON agent_runs (use_case, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX agent_runs_enduser_id_created_at_idx
            ON agent_runs (enduser_id, created_at DESC)
        """
    )

    # Verbatim from bootstrap.sql, like episode_view: the bridge and the
    # dashboard pick an episode's explanation through it.
    conn.execute(
        """
CREATE OR REPLACE VIEW episode_explanation_view AS
SELECT DISTINCT ON (episode_id)
       -- Inside CASE, so a filter pushed into the view never casts a chat key.
       CASE WHEN r.subject_kind = 'episode'
            THEN split_part(r.subject_key, ':', 1)::bigint
       END AS episode_id,
       r.id AS run_id, r.tldr, r.text, r.created_at
FROM agent_runs r
WHERE r.subject_kind = 'episode'
  AND r.status = 'completed'
  AND r.tldr IS NOT NULL
ORDER BY episode_id, r.created_at DESC;
        """
    )
    conn.execute(
        """
        CREATE TABLE agent_memory (
            use_case   TEXT        PRIMARY KEY,
            text       TEXT        NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )

    # Ledger seed — two explanations of the open episode (one per event
    # kind, which is what makes the subject key carry the kind), two
    # messenger turns without a person, one failed scheduled run outside the
    # default window, and Kim's and Lou's turns, Kim's older than both others.
    conn.execute(
        """
        INSERT INTO agent_runs (use_case, trigger, subject_kind, subject_key, session_id,
                                status, tldr, text, language, output_ref, output_state,
                                model_source, model, tokens_in, tokens_out, cost, duration,
                                created_at, finished_at)
        SELECT 'explain-episode', 'event', 'episode', e.id || ':appeared', NULL,
               'completed', 'Deckenlicht meldet seit vier Stunden nichts.',
               'Das Deckenlicht im Schlafzimmer hat zuletzt um 09:12 gesendet.',
               'de', ARRAY['discord:1'], ARRAY[NULL]::text[],
               'codex', 'gpt-5.5', 4200, 310, 0.021000, INTERVAL '42 seconds',
               NOW() - INTERVAL '4 hours', NOW() - INTERVAL '4 hours'
        FROM episodes e WHERE e.fault = 'silence' AND e.ended_at IS NULL
        UNION ALL
        SELECT 'explain-episode', 'event', 'episode', e.id || ':escalated', NULL,
               'completed', 'Auch die Nachbarkanäle des Geräts schweigen.',
               'Seit der Eskalation sendet kein Kanal des Geräts mehr.',
               'de', ARRAY['discord:2'], ARRAY[NULL]::text[],
               'codex', 'gpt-5.5', 5100, 280, 0.024000, INTERVAL '51 seconds',
               NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours'
        FROM episodes e WHERE e.fault = 'silence' AND e.ended_at IS NULL
        """
    )
    conn.execute(
        """
        INSERT INTO agent_runs (use_case, trigger, subject_kind, subject_key, session_id,
                                status, error, tldr, text, language, model_source, model,
                                tokens_in, tokens_out, cost, duration, created_at, finished_at)
        VALUES
            ('messenger', 'message', 'chat', 'session-a:1', 'session-a',
             'completed', NULL, 'Heute Nacht war nichts los.',
             'Keine Episode zwischen 22 und 07 Uhr.',
             'de', 'codex', 'gpt-5.5', 1900, 120, 0.009000, INTERVAL '8 seconds',
             NOW() - INTERVAL '3 hours', NOW() - INTERVAL '3 hours'),
            ('messenger', 'message', 'chat', 'session-a:2', 'session-a',
             'completed', NULL, 'Die Wallbox lädt mit 6,1 kW.',
             'Die Wallbox lädt seit 14:02 mit 6,1 kW.',
             'de', 'codex', 'gpt-5.5', 2100, 140, 0.010000, INTERVAL '9 seconds',
             NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes'),
            ('propose-faults', 'schedule', 'none', NULL, NULL,
             'failed', 'model source unreachable', NULL, NULL, 'en', 'codex', 'gpt-5.5',
             NULL, NULL, NULL, NULL,
             NOW() - INTERVAL '10 days', NOW() - INTERVAL '10 days')
        """
    )
    conn.execute(
        """
        INSERT INTO agent_runs (use_case, trigger, subject_kind, subject_key, session_id,
                                enduser_id, surface, status, tldr, text, language,
                                model_source, model, tokens_in, tokens_out, cost,
                                duration, created_at, finished_at)
        VALUES
            ('messenger', 'message', 'chat', 'session-k:1', 'session-k', 'kim', 'discord',
             'completed', 'Morgen wird es sonnig.', 'Morgen sonnig, 18 Grad.',
             'de', 'codex', 'gpt-6-luna', 1500, 90, 0,
             INTERVAL '7 seconds', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours'),
            ('messenger', 'message', 'chat', 'session-k:2', 'session-k', 'kim', 'discord',
             'completed', 'Der Trockner läuft noch 20 Minuten.',
             'Der Trockner ist um 15:40 fertig.',
             'de', 'codex', 'gpt-6-luna', 1700, 110, 0,
             INTERVAL '6 seconds', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes'),
            ('messenger', 'message', 'chat', 'session-l:1', 'session-l', 'lou', 'web',
             'completed', 'Die Garage ist zu.', 'Das Garagentor ist seit 12:10 zu.',
             'de', 'codex', 'gpt-6-luna', 1200, 60, 0,
             INTERVAL '5 seconds', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes')
        """
    )
    conn.execute(
        """
        INSERT INTO agent_memory (use_case, text)
        VALUES ('messenger', 'Der Besitzer fragt meist nach der Wallbox.')
        """
    )


# LiteralString so the tuple elements stay assignable to psycopg's Query type.
# Verbatim from bootstrap.sql: the tools read these columns by name, so a
# hand-written stand-in hides every drift between the bridge and the cluster.
_CAGGS: list[tuple[LiteralString, LiteralString]] = [
    (
        """
CREATE MATERIALIZED VIEW knx_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 hour', time) AS bucket, ga, knx_name,
       first(value, time) AS first_value, last(value, time) AS last_value,
       avg(value) AS avg_value, min(value) AS min_value, max(value) AS max_value,
       count(*) AS sample_count
FROM knx GROUP BY bucket, ga, knx_name WITH NO DATA
        """,
        "CALL refresh_continuous_aggregate('knx_1h', NULL, NULL)",
    ),
    (
        """
CREATE MATERIALIZED VIEW solaredge_powerflow_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 hour', time) AS bucket, inverter_id,
       avg(pv_production) AS pv_production_avg,
       max(pv_production) AS pv_production_max,
       avg(grid_power) AS grid_power_avg,
       avg(grid_consumption) AS grid_consumption_avg,
       avg(grid_delivery) AS grid_delivery_avg,
       avg(consumer_used_pv_production) AS consumer_used_pv_production_avg,
       avg(battery_charge) AS battery_charge_avg,
       avg(consumer_used_battery_production) AS consumer_used_battery_production_avg,
       avg(consumer_total) AS consumer_total_avg,
       max(consumer_total) AS consumer_total_max,
       sum(consumer_total) AS consumer_total_sum,
       count(*) AS sample_count
FROM solaredge_powerflow GROUP BY bucket, inverter_id WITH NO DATA
        """,
        "CALL refresh_continuous_aggregate('solaredge_powerflow_1h', NULL, NULL)",
    ),
    (
        """
CREATE MATERIALIZED VIEW warp_meter_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 hour', time) AS bucket, meter_id,
       avg(power_l1 + power_l2 + power_l3) AS power_total_avg,
       max(power_l1 + power_l2 + power_l3) AS power_total_max,
       avg(voltage_l1) AS voltage_l1_avg,
       avg(voltage_l2) AS voltage_l2_avg,
       avg(voltage_l3) AS voltage_l3_avg,
       avg(current_l1) AS current_l1_avg,
       avg(current_l2) AS current_l2_avg,
       avg(current_l3) AS current_l3_avg,
       count(*) AS sample_count
FROM warp_meter WHERE meter_id IS NOT NULL
GROUP BY bucket, meter_id WITH NO DATA
        """,
        "CALL refresh_continuous_aggregate('warp_meter_1h', NULL, NULL)",
    ),
]


@pytest.fixture
def settings(timescaledb_container: PostgresContainer) -> Settings:
    os.environ.update(
        MCP_DB_HOST=timescaledb_container.get_container_host_ip(),
        MCP_DB_PORT=str(timescaledb_container.get_exposed_port(5432)),
        MCP_DB_NAME="homelab",
        MCP_DB_USERNAME="test",
        MCP_DB_PASSWORD="test",
        # The container has a single superuser role, so the write path shares
        # it — in the cluster these are the separate rw credentials.
        MCP_DB_WRITE_USERNAME="test",
        MCP_DB_WRITE_PASSWORD="test",
        MCP_GATEWAY_KEY=GATEWAY_KEY,
        # The developer's own zone must not move the buckets the tests expect.
        TZ="UTC",
    )
    return Settings()  # type: ignore[call-arg]


@pytest_asyncio.fixture
async def db_pool(settings: Settings) -> AsyncIterator[None]:
    await db.init_pool(settings)
    await db.init_write_pool(settings)
    try:
        yield
    finally:
        await db.close_pool()


@pytest_asyncio.fixture
async def clean_verdicts(settings: Settings, db_pool: None) -> AsyncIterator[None]:
    """Every test starts with no verdict on any episode and none on any run."""

    def reset() -> None:
        conn = psycopg.connect(settings.db_dsn, autocommit=True)
        try:
            conn.execute("TRUNCATE TABLE episode_verdicts")
            conn.execute("UPDATE agent_runs SET verdict = NULL, verdict_at = NULL")
        finally:
            conn.close()

    reset()
    yield
    reset()


@pytest.fixture
def trigger_key_file(tmp_path: Path) -> Path:
    path = tmp_path / "trigger-api-key"
    path.write_text(f"{TRIGGER_KEY}\n", encoding="utf-8")  # trailing newline, like a mounted Secret
    return path


@pytest_asyncio.fixture
async def trigger_client(trigger_key_file: Path) -> AsyncIterator[None]:
    settings = Settings(
        db_host="localhost",
        db_name="x",
        db_username="x",
        db_password="x",
        gateway_key=GATEWAY_KEY,
        nats_enabled=False,
        trigger_url=TRIGGER_URL,
        trigger_key_file=str(trigger_key_file),
    )
    await trigger.init(settings)
    try:
        yield
    finally:
        await trigger.close()


@pytest.fixture
def trigger_router() -> Iterator[respx.MockRouter]:
    with respx.mock(base_url=TRIGGER_URL, assert_all_called=False) as mock:
        yield mock
