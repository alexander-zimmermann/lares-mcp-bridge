# lares-mcp-bridge

_Formerly `iot-mcp-bridge`._

A near read-only [Model Context Protocol](https://modelcontextprotocol.io) server that lets a large language model — Claude.ai, a local Ollama instance, or any other MCP-aware client — answer questions about an **IoT-enabled home**: heating, photovoltaics, EV charging, KNX bus events, room climate.

Instead of giving the LLM raw SQL access (brittle, hard to bound, no audit), `lares-mcp-bridge` exposes a small set of well-shaped tools backed by **TimescaleDB hypertables** plus their **continuous aggregates**. The LLM picks a tool, the server runs a parametrised query against a long-term timeseries store, and the result comes back already aggregated and capped to a token-friendly size.

## Why

Modern homes generate a lot of telemetry — KNX writes, smart-meter readings, heat-pump flow temperatures, wallbox sessions, inverter data — and most of it lands in some database that nobody ever queries. Pointing an LLM at the database directly works for prototypes but breaks down quickly: the model invents column names, returns 50,000 rows, can't tell hypertables apart from rollups, and there is no policy on what it's allowed to read.

`lares-mcp-bridge` is the small, opinionated middle layer:

- **Discoverable** — the LLM can list data sources and inspect schemas, including a sample of JSONB keys for raw payloads.
- **Aggregation-aware** — when a query asks for hourly buckets or coarser, the server transparently routes to a TimescaleDB continuous aggregate, returning fewer rows and faster responses.
- **Read-only by construction, with one exception** — the query tools connect with a Postgres role that has `SELECT` privileges only, and there is no `execute_sql` tool. The single write is `set_episode_verdict`, which goes through a separate pool and a role that may write exactly one table (`episode_verdicts`) and reads only the episode it is judging. Leave the write credentials unset and the server is read-only outright.
- **Result-bounded** — every tool caps its output. The LLM cannot accidentally pull a year of 5-second sensor data into its context window.
- **Pluggable** — the data source is just Postgres + TimescaleDB. There is nothing homelab-specific in the server itself; the schema discovery works on any TimescaleDB instance.

## What it gives you

The server exposes two layers of tools — generic ones that work against any
TimescaleDB schema, and domain-specific ones that hide the table layout
behind high-level questions:

**Generic**

| Tool                                                                             | What it does                                                                                                                                                                            |
| -------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `list_data_sources()`                                                            | Enumerates all hypertables and continuous aggregates in the connected database, with their time range.                                                                                  |
| `get_schema(table)`                                                              | Returns columns, types, and — for tables with a `JSONB` payload column — a sample of the most common JSON keys, so the LLM can construct sensible expressions like `raw->>'flow_temp'`. |
| `query_timeseries(table, columns, from_ts, to_ts, aggregation, bucket, filters)` | Bucketed aggregate query (`avg`, `sum`, `min`, `max`, `count`). Automatically uses a `*_1h` continuous aggregate when the bucket is one hour or coarser and an aggregate exists.        |

**Domain**

| Tool                                                  | What it does                                                                                                                                                                          |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `query_energy_flow(from_ts, to_ts, bucket)`           | One row per bucket joining PV production, grid, household consumer, battery net, and wallbox power. Reads the hourly continuous aggregates; bucket must be `1 hour` or coarser.       |
| `query_heating_cycles(from_ts, to_ts, …)`             | Detects ON/OFF burner cycles from `ems_esp` (`topic = 'boiler_data'`) via `LAG` window functions. Returns one row per cycle with start, end, duration, peak and average burner power. |
| `query_room_climate(room, from_ts, to_ts, bucket)`    | Bucketed temp/humidity/etc. per GA name within a room. `room` is whitelisted against the imported KNX catalog so unknown rooms produce a precise error the LLM can self-correct.      |
| `query_knx_events(from_ts, to_ts, room, ga, name, …)` | Raw event log against the `ga_catalog_view` view; supports room/ga/name/functions predicates with a default limit of 200.                                                            |
| `query_unifi_events(from_ts, to_ts, camera, …)`       | Recent UniFi Protect Alarm Manager events (person/vehicle/motion detections per camera) for security review.                                                                          |
| `correlate_events(source_a, source_b, …)`             | Lagged Pearson correlation between two time-series streams. Each `source` is `{table, column}`; returns the best lag plus the top 10 by `\|corr\|`.                                   |

**Forecasts** (read-only views into the forecast table populated by [lares-diagnostics-engine](https://github.com/alexander-zimmermann/lares-diagnostics-engine) batch jobs)

| Tool                                            | What it does                                                                                       |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `get_forecast(metric, horizon_hours, model)`    | Stored model forecasts (PV via Forecast.Solar, seasonal via statsforecast).                        |

**Verdicts** (the review loop over the situations [lares-diagnostics-engine](https://github.com/alexander-zimmermann/lares-diagnostics-engine) recorded)

| Tool                                              | What it does                                                                                                                                 |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `list_episodes(state, fault, days, …)`            | The situations the detection chain folded from repeated observations, newest first, each with the verdict it carries. `only_unjudged=True` is what still needs judging. |
| `set_episode_verdict(episode_id, verdict)`        | Records `"real"` or `"nonsense"` on one episode; setting it again overwrites. Requires the write credentials below. Nothing acts on a verdict automatically — they are counted per fault on the dashboard, and thresholds stay a human decision. |

**Live** (current state straight from NATS JetStream; requires `MCP_NATS_ENABLED=true`)

| Tool                                          | What it does                                                                                                  |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `get_current_state(domain, identifier)`       | Last retained message for a domain (`knx`/`heating`/`dhw`/`solar`/`wallbox`) with freshness metadata.         |
| `get_current_knx(room, function, name, …)`    | Current value per matching KNX group address with its role (status / command / reading) — answers "which lights are on right now?". |
| `subscribe_nats(subject, duration_seconds)`   | Tails an allowlisted NATS subject for a short bounded window.                                                  |

**Wiki** (the house's human-written references in [Wiki.js](https://js.wiki/); requires `MCP_WIKIJS_URL` + `MCP_WIKIJS_TOKEN_FILE`)

| Tool                                | What it does                                                                                                        |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `search_wiki(query)`                | Title/description/path search (`pages.search`); returns ids and paths to read.                                      |
| `get_wiki_page(path \| page_id)`    | One page in full — raw Markdown `content` plus title, tags and dates. Locale comes from the page, never hardcoded.  |
| `list_wiki_pages()`                 | The table of contents (`pages.list`): id, path, locale, title, description, `updated_at` of every readable page.     |

The wiki key is read-only by construction: its Wiki.js group carries exactly
`read:pages` and `read:source`. Wiki.js 2.x guards the GraphQL `pages.single` /
`pages.singleByPath` queries with the `manage:pages` *edit* permission, so page
content is read from the source view (`GET /s/<locale>/<path>`, `read:source`)
instead — the one read-only route to raw content. There is no write tool.

Later phases (tracked separately) add optimization advisors and
approval-gated control.

## Standalone usage

`lares-mcp-bridge` does not depend on any specific cluster, ingress, or auth setup. You can run it against any TimescaleDB instance you already have.

### Prerequisites

- Python 3.14
- [uv](https://docs.astral.sh/uv/)
- A Postgres database with the [TimescaleDB](https://www.timescale.com/) extension and at least one hypertable
- A Postgres role with `SELECT` on the schemas/tables you want to expose

### Run it

```bash
git clone https://github.com/alexander-zimmermann/lares-mcp-bridge.git
cd lares-mcp-bridge
uv sync

export MCP_DB_HOST=localhost
export MCP_DB_PORT=5432
export MCP_DB_NAME=mydb
export MCP_DB_USERNAME=mcp_readonly
export MCP_DB_PASSWORD=secret
# The live tools default to an in-cluster NATS URL — disable them standalone:
export MCP_NATS_ENABLED=false

uv run lares-mcp-bridge
# server listening on http://0.0.0.0:8080/mcp
```

### Connect Claude.ai

Open Claude.ai → Settings → Connectors → "Add custom connector" → URL `http://localhost:8080/mcp` (or wherever you exposed the server). Start a chat and ask "what data sources do you have?" — the connector will surface the tools listed above.

For a production deployment, put the server behind TLS and an authenticating proxy (or enable the built-in Authentik OAuth + JWKS validation, see configuration below).

### Configuration

All settings are environment variables, prefixed with `MCP_`:

| Variable                             | Default                     | Purpose                                                        |
| ------------------------------------ | --------------------------- | --------------------------------------------------------------- |
| `MCP_HOST`                           | `0.0.0.0`                   | Bind address                                                   |
| `MCP_PORT`                           | `8080`                      | Bind port                                                      |
| `MCP_LOG_LEVEL`                      | `INFO`                      | structlog level                                                |
| `MCP_LOG_FORMAT`                     | `json`                      | `json` or `console`                                            |
| `MCP_DB_HOST`                        | _required_                  | Postgres host                                                  |
| `MCP_DB_PORT`                        | `5432`                      | Postgres port                                                  |
| `MCP_DB_NAME`                        | _required_                  | Database name                                                  |
| `MCP_DB_USERNAME`                    | _required_                  | Read-only role                                                 |
| `MCP_DB_PASSWORD`                    | _required_                  | Password                                                       |
| `MCP_DB_USERNAME_FILE`               | —                           | Read role from a mounted file (overrides `MCP_DB_USERNAME`)    |
| `MCP_DB_PASSWORD_FILE`               | —                           | Read password from a mounted file (overrides `MCP_DB_PASSWORD`) |
| `MCP_DB_WRITE_USERNAME`              | —                           | Verdict-write role; unset leaves the server read-only          |
| `MCP_DB_WRITE_PASSWORD`              | —                           | Password for the verdict-write role                            |
| `MCP_DB_WRITE_USERNAME_FILE`         | —                           | Read write role from a mounted file                            |
| `MCP_DB_WRITE_PASSWORD_FILE`         | —                           | Read write password from a mounted file                        |
| `MCP_DB_POOL_MIN`                    | `2`                         | psycopg pool min size                                          |
| `MCP_DB_POOL_MAX`                    | `10`                        | psycopg pool max size                                          |
| `MCP_QUERY_ROW_LIMIT`                | `5000`                      | Hard cap on rows returned by the query tools                   |
| `MCP_METRICS_PORT`                   | `9090`                      | Prometheus `/metrics` port                                     |
| `MCP_AUTH_ENABLED`                   | `false`                     | Enable Bearer-token validation                                 |
| `MCP_AUTH_JWKS_URL`                  | —                           | JWKS endpoint when auth is enabled                             |
| `MCP_AUTH_ISSUER`                    | —                           | Expected `iss` claim                                           |
| `MCP_AUTH_AUDIENCE`                  | —                           | Expected `aud` claim                                           |
| `MCP_AUTH_JWKS_TTL_SECONDS`          | `3600`                      | JWKS cache lifetime                                            |
| `MCP_AUTH_JWKS_MIN_REFRESH_SECONDS`  | `30`                        | Cooldown between JWKS fetch attempts                           |
| `MCP_AUTH_RESOURCE_URL`              | —                           | Public URL for RFC 9728 protected-resource metadata            |
| `MCP_AUTH_CLIENTS_FILE`              | —                           | Machine clients: JSON object of client name to API key         |
| `MCP_AUTH_CLIENT_TOOLS`              | `{}`                        | Tool allowlist per client name (JSON; names or fnmatch globs)  |
| `MCP_NATS_ENABLED`                   | `true`                      | Enable the live tools (NATS JetStream)                         |
| `MCP_NATS_SERVERS`                   | `nats://nats.nats.svc:4222` | Comma-separated NATS server URLs                               |
| `MCP_NATS_NKEY_SEED_FILE`            | —                           | nkey seed file for NATS auth (anonymous when unset)            |
| `MCP_LIVE_STALE_SECONDS`             | `600`                       | Age after which cyclic live state is flagged `stale`           |
| `MCP_SUBSCRIBE_MAX_SECONDS`          | `30`                        | Hard cap for `subscribe_nats` windows                          |
| `MCP_WIKIJS_URL`                     | —                           | Wiki.js base URL; with the token file enables the wiki tools   |
| `MCP_WIKIJS_TOKEN_FILE`              | —                           | Read-only Wiki.js API key from a mounted file                  |

When `MCP_AUTH_ENABLED=true`, every request must carry a valid OIDC Bearer token signed by the configured JWKS. Tested against [Authentik](https://goauthentik.io/) but works with any OIDC-compliant authorization server.

An agent that cannot run an OAuth flow authenticates with a static API key instead: `MCP_AUTH_CLIENTS_FILE` points at a mounted Secret holding `{"<client name>": "<key>"}` (keys of at least 32 characters), sent as `Authorization: Bearer <key>`. Such a machine client sees only the tools its entry in `MCP_AUTH_CLIENT_TOOLS` names, for example `{"lares-agent": ["list_episodes", "query_*", "get_*", "resolve"]}`; a machine client without an entry sees no tools, a user without one keeps every tool. Denied calls are counted in the tool-call metric with outcome `denied`.

### Operational endpoints

- `GET /livez` — liveness: process is up; never depends on DB/NATS health.
- `GET /healthz` — readiness/deep health: 503 when the database is unreachable; reports NATS connectivity when the live tools are enabled. The wiki is never part of it — a wiki outage fails the wiki tools, not the pod.
- `GET :9090/metrics` — Prometheus metrics (tool calls, DB query durations, JWKS refreshes, NATS fetches).

### Container image

Multi-arch images are published on every release tag:

```bash
docker run --rm -p 8080:8080 \
  -e MCP_DB_HOST=host.docker.internal \
  -e MCP_DB_NAME=mydb \
  -e MCP_DB_USERNAME=mcp_readonly \
  -e MCP_DB_PASSWORD=secret \
  -e MCP_NATS_ENABLED=false \
  ghcr.io/alexander-zimmermann/lares-mcp-bridge:latest
```

## GA catalog

The domain tools (`query_room_climate`, `query_knx_events`, `get_current_knx`)
need a Postgres `ga_catalog` table that maps each KNX group address to a name,
room, function, and description. The catalog is generated from the ETS
`.knxproj` file by
[knx-nats-bridge](https://github.com/alexander-zimmermann/knx-nats-bridge),
which also ships the import job that writes it into Postgres (the importer
was moved there when this repo was slimmed down to the MCP server itself).

## Development

```bash
uv sync --extra dev
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest -q
```

Tests use [`testcontainers`](https://testcontainers-python.readthedocs.io/) to spin up a real TimescaleDB instance — no DB mocks.

## License

GPL-2.0-or-later. See [LICENSE](LICENSE).
