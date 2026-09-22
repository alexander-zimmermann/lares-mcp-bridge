"""Shared fixtures: a seeded TimescaleDB testcontainer, settings, and the DB pool."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from typing import LiteralString

import psycopg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from lares_mcp_bridge import db
from lares_mcp_bridge.config import Settings
from lares_mcp_bridge.tools import sources


@pytest.fixture(autouse=True)
def _fresh_sources_cache() -> None:
    """Drop the data-source catalog TTL cache so tests stay order-independent."""
    sources.invalidate_cache()


# TimescaleDB image with the extension preinstalled.
TIMESCALEDB_IMAGE = "timescale/timescaledb:latest-pg17"


def _connect(container: PostgresContainer) -> psycopg.Connection:
    """Open an autocommit connection to the running testcontainer."""
    return psycopg.connect(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(5432)),
        user="test",
        password="test",
        dbname="homelab",
        autocommit=True,
    )


@pytest.fixture(scope="session")
def timescaledb_container() -> Iterator[PostgresContainer]:
    container = PostgresContainer(
        TIMESCALEDB_IMAGE, username="test", password="test", dbname="homelab"
    )
    container.start()
    try:
        # 1) Extension + tables + seed (regular DDL/DML, autocommit-safe).
        conn = _connect(container)
        try:
            conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            _seed(conn)
        finally:
            conn.close()

        # 2) Continuous aggregates. `CALL refresh_continuous_aggregate()` must
        # run outside any transaction block — fresh connection per CAGG keeps
        # psycopg from wrapping them in an implicit BEGIN.
        for cagg_sql, refresh_sql in _CAGGS:
            conn = _connect(container)
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
            raw   JSONB
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
            current_l1 DOUBLE PRECISION
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

    # ems_esp seed — every 3rd row burns at 50 kW, otherwise 0. That gives
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
            started_at   TIMESTAMPTZ      NOT NULL,
            last_seen_at TIMESTAMPTZ      NOT NULL,
            ended_at     TIMESTAMPTZ,
            severity     SMALLINT         NOT NULL CHECK (severity BETWEEN 1 AND 3),
            peak_score   DOUBLE PRECISION NOT NULL,
            folded       BOOLEAN          NOT NULL DEFAULT false,
            externally_delivered BOOLEAN  NOT NULL DEFAULT false,
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
             NOW() - INTERVAL '39 days', NOW() - INTERVAL '39 days', 2, 3.5,  true)
        """
    )


# LiteralString so the tuple elements stay assignable to psycopg's Query type.
_CAGGS: list[tuple[LiteralString, LiteralString]] = [
    (
        """
        CREATE MATERIALIZED VIEW knx_1h
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 hour', time) AS bucket,
            ga,
            avg(value)  AS value,
            count(*)    AS samples
        FROM knx
        GROUP BY bucket, ga
        WITH NO DATA
        """,
        "CALL refresh_continuous_aggregate('knx_1h', NULL, NULL)",
    ),
    (
        """
        CREATE MATERIALIZED VIEW solaredge_powerflow_1h
        WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
        SELECT
            time_bucket('1 hour', time) AS bucket,
            inverter_id,
            avg(pv_production)              AS pv_production_avg,
            max(pv_production)              AS pv_production_max,
            avg(grid_power)                 AS grid_power_avg,
            avg(grid_consumption)           AS grid_consumption_avg,
            avg(grid_delivery)              AS grid_delivery_avg,
            avg(consumer_total)             AS consumer_total_avg,
            avg(battery_charge - battery_discharge) AS battery_net_avg,
            count(*)                         AS sample_count
        FROM solaredge_powerflow
        GROUP BY bucket, inverter_id
        WITH NO DATA
        """,
        "CALL refresh_continuous_aggregate('solaredge_powerflow_1h', NULL, NULL)",
    ),
    (
        """
        CREATE MATERIALIZED VIEW warp_meter_1h
        WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
        SELECT
            time_bucket('1 hour', time) AS bucket,
            meter_id,
            avg(power_l1 + power_l2 + power_l3) AS power_total_avg,
            max(power_l1 + power_l2 + power_l3) AS power_total_max,
            avg(voltage_l1)                     AS voltage_l1_avg,
            avg(current_l1)                     AS current_l1_avg,
            count(*)                            AS sample_count
        FROM warp_meter
        WHERE meter_id IS NOT NULL
        GROUP BY bucket, meter_id
        WITH NO DATA
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
        MCP_AUTH_ENABLED="false",
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
