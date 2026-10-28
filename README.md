# lares-mcp-bridge

A near read-only [Model Context Protocol](https://modelcontextprotocol.io) server that lets a large language model — Claude.ai, a local Ollama instance, or any other MCP-aware client — answer questions about an **IoT-enabled home**: heating, photovoltaics, EV charging, KNX bus events, room climate.

Instead of giving the LLM raw SQL access (brittle, hard to bound, no audit), `lares-mcp-bridge` exposes a small set of well-shaped tools backed by **TimescaleDB hypertables** plus their **continuous aggregates**. The LLM picks a tool, the server runs a parametrised query against a long-term timeseries store, and the result comes back already aggregated and capped to a token-friendly size.

## Why

Modern homes generate a lot of telemetry — KNX writes, smart-meter readings, heat-pump flow temperatures, wallbox sessions, inverter data — and most of it lands in some database that nobody ever queries. Pointing an LLM at the database directly works for prototypes but breaks down quickly: the model invents column names, returns 50,000 rows, can't tell hypertables apart from rollups, and there is no policy on what it's allowed to read.

`lares-mcp-bridge` is the small, opinionated middle layer:

- **Discoverable** — the LLM can list data sources and inspect schemas, including a sample of JSONB keys for raw payloads.
- **Aggregation-aware** — `list_data_sources` lists the TimescaleDB continuous aggregates next to their hypertables, so a query over a long range can name the `*_1h` aggregate and read fewer rows, faster.
- **Read-only by construction, with one exception** — the query tools connect with a Postgres role that has `SELECT` privileges only, and there is no `execute_sql` tool. The single write is `set_verdict`, which goes through a separate pool and a role whose whole privilege is one table (`episode_verdicts`) plus two columns of the ledger (`agent_runs.verdict`, `agent_runs.verdict_at`). Leave the write credentials unset and the server is read-only outright.
- **Result-bounded** — every tool caps its output. The LLM cannot accidentally pull a year of 5-second sensor data into its context window.
- **Pluggable** — the data source is just Postgres + TimescaleDB. There is nothing homelab-specific in the server itself; the schema discovery works on any TimescaleDB instance.

## What it gives you

The server exposes two layers of tools — generic ones that work against any
TimescaleDB schema, and domain-specific ones that hide the table layout
behind high-level questions:

**Generic**

| Tool                                                                             | What it does                                                                                                                                                                            |
| -------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `list_data_sources()`                                                            | Enumerates all hypertables and continuous aggregates in the connected database, with their time range, under `sources`.                                                                                  |
| `get_schema(table)`                                                              | Returns columns, types, and — for tables with a `JSONB` payload column — a sample of the most common JSON keys, so the LLM can construct sensible expressions like `raw->>'flow_temp'`. |
| `query_timeseries(table, columns, from_ts, to_ts, aggregation, bucket, filters)` | Bucketed aggregate query (`avg`, `sum`, `min`, `max`, `count`) over the table as named: a hypertable, or a `*_1h` continuous aggregate with that aggregate's columns.                 |

**Domain**

| Tool                                                  | What it does                                                                                                                                                                          |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `query_energy_flow(from_ts, to_ts, bucket)`           | One row per bucket with the energy in kWh of PV production, grid, household consumer, battery charge and discharge, and the wallbox. Sums both inverters and all wallbox meters from the hourly continuous aggregates; bucket must be whole hours (`1 hour`, `1 day`, …). |
| `query_heating_cycles(from_ts, to_ts, …)`             | Detects ON/OFF burner cycles from `ems_esp` (`topic = 'boiler_data'`) via `LAG` window functions. Returns one row per cycle with start, end, duration, and the peak and average burner modulation in percent (`peak_modulation_pct`, `avg_modulation_pct`). |
| `query_room_climate(room, from_ts, to_ts, bucket)`    | Bucketed temp/humidity/etc. per GA name within a room. `room` is whitelisted against the imported KNX catalog so unknown rooms produce a precise error the LLM can self-correct.      |
| `query_knx_events(from_ts, to_ts, room, ga, name, …)` | Raw event log against the `ga_catalog_view` view; supports room/ga/name/functions predicates with a default limit of 200.                                                            |
| `query_unifi_events(from_ts, to_ts, camera, …)`       | Recent UniFi Protect Alarm Manager events (person/vehicle/motion detections per camera) for security review.                                                                          |
| `query_presence(from_ts, to_ts)`                      | Who was at home over a window, from each person's `Person.<name>.Präsenz`: spans of `home` / `away` / `unknown` per person, the spans in which the house was empty, and the people whose presence reports nothing. A few lines where the raw events would be many. |
| `correlate_events(source_a, source_b, …)`             | Lagged Pearson correlation between two time-series streams. Each `source` is `{table, column}`; returns the best lag plus the top 10 by `\|corr\|`.                                   |

**Forecasts** (read-only views into the forecast table populated by [lares-diagnostics-engine](https://github.com/alexander-zimmermann/lares-diagnostics-engine) batch jobs)

| Tool                                            | What it does                                                                                       |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `get_forecast(metric, horizon_hours, model)`    | Stored model forecasts (PV via Forecast.Solar, seasonal via statsforecast).                        |

**Verdicts** (the review loop over the situations [lares-diagnostics-engine](https://github.com/alexander-zimmermann/lares-diagnostics-engine) recorded and the runs the agent platform made)

| Tool                                              | What it does                                                                                                                                 |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `list_episodes(state, fault, days, …)`            | The situations the detection chain folded from repeated observations, newest first, each with the verdict it carries and the newest explanation of it. `only_unjudged=True` is what still needs judging, `only_judged=True` what a verdict count reads. |
| `get_episode(episode_id)`                         | One episode whole: the row, its events and its last 48 observations, the catalog entry of its channel, the sibling channels of the same room that report a state or a reading (the channel's own device first, at most 50), and the newest explanation. What an explanation opens with: one call, so the bundle is consistent with itself and costs fewer tokens than the reads it replaces. |
| `list_runs(use_case, subject_kind, status, days, …)` | The ledger: one row per run of every use case, with what it produced, what it cost, the person and surface of a chat run and the verdict it carries. A caller reads their own runs and the machines', never another person's (see persons below). A run named by `run_id` comes back with its full output text. |
| `set_verdict(target, verdict, …)`                 | Records `"real"`/`"nonsense"` on an episode or `"helpful"`/`"useless"` on a run; setting it again overwrites. A run is named by `run_id`, by `subject_kind` + `subject_key` (the newest run on that subject), or by nothing (the caller's newest chat run), and only among the runs `list_runs` shows the caller. Requires the write credentials below. Nothing acts on a verdict automatically — they are counted per fault and per use case on the dashboard, and thresholds stay a human decision. |

**Back-test** (a candidate fault measured by [lares-diagnostics-engine](https://github.com/alexander-zimmermann/lares-diagnostics-engine) itself, a dependency at its deployed tag)

| Tool                                        | What it does                                                                                                   |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `backtest_fault(candidate, weeks, limit)`   | Runs one fault-list entry through the engine's own schema, measurement and episode fold over the last `weeks` and returns the episodes it would have produced (subject, dates, severity, peak score), oldest first, with the measurement's record. On a connection of its own on the read role, READ ONLY; whatever the engine cannot measure comes back as its reason. |

**Runs and memory** (the agent platform's own state, written through [lares-agent-trigger](https://github.com/alexander-zimmermann/lares-agent-trigger), its only writer; the trigger tools require `MCP_TRIGGER_URL` + `MCP_TRIGGER_KEY_FILE`)

| Tool                          | What it does                                                                                                   |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `start_run(use_case, subject, focus)` | Asks the trigger to start a declared use case now: on an episode (`subject` = the episode id) or, for a schedule use case, its cron job, with what the owner asked for as `focus`. The trigger claims the ledger row, counts the day's budget and delivers the output where the use case says; the tool hands back its answer (`run_id` or `job_id`, the `output` targets) or its reason for refusing. Meant for the chat only: a gateway route of its own, so no run the platform starts holds it. |
| `get_memory(use_case)`        | The working notes a use case keeps between its runs (`agent_memory`), read with the read role; one that has written nothing reads as empty. |
| `append_memory(use_case, line)` | Asks the trigger to append one note to that memory. The trigger keeps it to about 8 KB by dropping the oldest lines. A gateway route of its own with `get_memory`, held only by the runs of use cases that keep a memory. |

**Live** (current state straight from NATS JetStream; requires `MCP_NATS_ENABLED=true`)

| Tool                                          | What it does                                                                                                  |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `get_current_state(domain, identifier)`       | Last retained message for a domain (`knx`/`heating`/`dhw`/`solar`/`wallbox`) with freshness metadata.         |
| `get_current_knx(room, function, name, …)`    | Current value per matching KNX group address with its role (status / command / reading) — answers "which lights are on right now?". |
| `subscribe_nats(subject, duration_seconds)`   | Tails an allowlisted NATS subject for a short bounded window.                                                  |

**Wiki** (the house's human-written references in [Wiki.js](https://js.wiki/); the reads require `MCP_WIKIJS_URL` + `MCP_WIKIJS_TOKEN_FILE`, the write additionally `MCP_WIKIJS_WRITE_TOKEN_FILE`)

| Tool                                   | What it does                                                                                                        |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `search_wiki(query)`                   | Title/description/path search (`pages.search`); returns ids and paths to read.                                      |
| `get_wiki_page(path \| page_id)`       | One page in full — raw Markdown `content` plus title, tags and dates. Locale comes from the page, never hardcoded.  |
| `list_wiki_pages()`                    | The table of contents (`pages.list`): id, path, locale, title, description, `updated_at` of every readable page.     |
| `update_wiki_page(path, content, title)` | Creates the page at `path` in `MCP_WIKIJS_LOCALE`, or replaces content and title of the page there. An update keeps the page's description, tags and publish flag (a publish window set in the editor is cleared); the previous revision stays in the page's history. |

The read key is read-only by construction: its Wiki.js group carries exactly
`read:pages` and `read:source`. Wiki.js 2.x guards the GraphQL `pages.single` /
`pages.singleByPath` queries with the `manage:pages` *edit* permission, so page
content is read from the source view (`GET /s/<locale>/<path>`, `read:source`)
instead — the one read-only route to raw content.

The write runs under a second key whose group carries `read:pages` and
`write:pages` and nothing else — no delete, no move, no styles or scripts. It
is meant for a gateway route of its own that serves
`update_wiki_page` only, so no run that only reads ever holds it. Wiki.js
resets whatever an update leaves out (an omitted publish flag unpublishes the
page, omitted tags fail the update), so the tool reads the page's row first and
carries those fields over. A write answers with the raw page row, which has
`localeCode` and no `locale`, so the tool never selects `locale` there and
reports the configured one.

**Backups** (the [Proxmox Backup Server](https://pbs.proxmox.com/); requires `MCP_PBS_URL` + `MCP_PBS_TOKEN_ID` + `MCP_PBS_TOKEN_FILE`)

| Tool                                                       | What it does                                                                                       |
| ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `list_pbs_datastores()`                                    | Every datastore with its usage and fill estimate, its garbage collection and its verify jobs, each job with its last run's state and end and its next run. A job that never ran reads as `null`, never as a success. |
| `list_pbs_snapshots(datastore, backup_type, backup_id, limit)` | One datastore's snapshots, newest first, each with the state of its last verification (`ok` / `failed` / `null`) and its verify task; `counts` over every matching snapshot. |
| `list_pbs_tasks(task_type, datastore, errors_only, days, limit)` | The node's task log: backups, verifications, garbage collection, prune and sync, with start, end and end state. |

The token has the `Audit` role on `/`, like the Prometheus exporter's: it sees
every datastore, job and task and changes none of them. TLS is verified
against the server's ACME certificate.

**Storage** (the house's S3 object store: database and volume backups and the cold archive; requires `MCP_S3_ENDPOINT_URL` + `MCP_S3_ACCESS_KEY_FILE` + `MCP_S3_SECRET_KEY_FILE`)

| Tool                                   | What it does                                                                                       |
| -------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `list_s3_buckets()`                    | Every bucket with its object count, size, newest object and oldest time — whether today's backup arrived. |
| `list_s3_objects(bucket, prefix, limit)` | The objects under a key prefix, newest first, with the next `/`-level summed up per child (a stream of the archive, a backup of a database), so a child that stopped writing shows as one whose newest object lags. |

The key's policy allows `s3:ListAllMyBuckets` and `s3:ListBucket` only: the
tools read listings, never an object. They speak plain S3 (`ListBuckets`,
`ListObjectsV2`), so a change of the service behind the endpoint leaves them
as they are. S3 lists by key, not by time, so finding the newest object means
listing all of it; both tools page through the whole listing.

A tool that rejects its arguments — an unknown table, column or room, an
invalid bucket, a result over the row cap — answers with an ordinary result,
`{"refused": "<reason>"}`, naming what would have been valid. It is never an
MCP error: a harness counts error results as strikes against the server and
pauses it after a few, so one batch of guessed column names would blind every
run for a minute. Only the bridge's own failures — the database, NATS, a
timeout — are errors. The outcome metric counts `ok`, `refused` and `error`
apart.

Later phases (tracked separately) add optimization advisors and
approval-gated control.

## Standalone usage

`lares-mcp-bridge` does not depend on any specific cluster or ingress. You can run it against any TimescaleDB instance you already have. It does expect an MCP gateway in front of it that signs the callers in (see [Sign-in](#sign-in)).

### Prerequisites

- Python 3.15
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
# The key the gateway calls with, at least 32 characters:
export MCP_GATEWAY_KEY=$(openssl rand -hex 32)

uv run lares-mcp-bridge
# server listening on http://0.0.0.0:8080/mcp
```

### Connect a client

Every request carries the gateway key and names its caller (see [Sign-in](#sign-in)), so a client connects through a gateway that does both, such as [agentgateway](https://agentgateway.dev/). For a quick local check, an MCP client that sets headers can stand in for it: `Authorization: Bearer $MCP_GATEWAY_KEY` and `X-Client-Id: local` against `http://localhost:8080/mcp`.

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
| `MCP_TIMEZONE`                       | `UTC`                       | IANA zone the bucketing tools cut days in and show buckets in  |
| `MCP_METRICS_PORT`                   | `9090`                      | Prometheus `/metrics` port                                     |
| `MCP_GATEWAY_KEY`                    | _required_                  | The key the gateway calls with, at least 32 characters         |
| `MCP_GATEWAY_KEY_FILE`               | —                           | Read the gateway key from a mounted file (overrides `MCP_GATEWAY_KEY`) |
| `MCP_NATS_ENABLED`                   | `true`                      | Enable the live tools (NATS JetStream)                         |
| `MCP_NATS_SERVERS`                   | `nats://nats.nats.svc:4222` | Comma-separated NATS server URLs                               |
| `MCP_NATS_NKEY_SEED_FILE`            | —                           | nkey seed file for NATS auth (anonymous when unset)            |
| `MCP_LIVE_STALE_SECONDS`             | `600`                       | Age after which cyclic live state is flagged `stale`           |
| `MCP_SUBSCRIBE_MAX_SECONDS`          | `30`                        | Hard cap for `subscribe_nats` windows                          |
| `MCP_WIKIJS_URL`                     | —                           | Wiki.js base URL; with the token file enables the wiki tools   |
| `MCP_WIKIJS_TOKEN_FILE`              | —                           | Read-only Wiki.js API key from a mounted file                  |
| `MCP_WIKIJS_WRITE_TOKEN_FILE`        | —                           | Wiki.js API key with `write:pages`; enables `update_wiki_page` |
| `MCP_WIKIJS_LOCALE`                  | `en`                        | Locale `update_wiki_page` looks pages up in and creates them in |
| `MCP_PBS_URL`                        | —                           | Proxmox Backup Server base URL (`https://<host>:8007`)         |
| `MCP_PBS_TOKEN_ID`                   | —                           | API token id, `<user>@<realm>!<token name>`                    |
| `MCP_PBS_TOKEN_FILE`                 | —                           | The token's secret from a mounted file                         |
| `MCP_S3_ENDPOINT_URL`                | —                           | S3 endpoint of the object store (path-style, SigV4)            |
| `MCP_S3_ACCESS_KEY_FILE`             | —                           | Access key id of a list-only key, from a mounted file          |
| `MCP_S3_SECRET_KEY_FILE`             | —                           | Its secret key, from a mounted file                            |
| `MCP_TRIGGER_URL`                    | —                           | lares-agent-trigger base URL; with the key file enables `start_run` and `append_memory` |
| `MCP_TRIGGER_KEY_FILE`               | —                           | The key the trigger's API takes, from a mounted file           |
| `MCP_TRACING_ENDPOINT`               | —                           | OTLP/HTTP collector base URL (`http://<host>:4318`); unset keeps tracing off |
| `MCP_TRACING_SAMPLING_RATIO`         | `0.1`                       | Share of new traces kept, decided by trace id                  |

### Sign-in

The bridge answers its gateway only. The gateway signs every caller in (a key per route, or an OAuth sign-in for a person's connector), serves each route its own list of tools, and calls the bridge with one static key: every request but the probes must carry `Authorization: Bearer <MCP_GATEWAY_KEY>`, or it gets 401.

The gateway names the caller in two headers it sets itself, overwriting whatever the caller sent: `X-Client-Id`, the route the call came in on, and `X-Enduser-Id`, the person's handle, absent for a machine. A request with the key but without `X-Client-Id` gets 401 too. The client is `lares.client_id` on the call's span and the `client_id` label of the tool-call metric; the person is `enduser.id` on the request's log lines and its span. A person reads their own rows of the ledger and the machines' rows, and `set_verdict` records them; a caller without a person reads the machines' rows only.

### Tracing

With `MCP_TRACING_ENDPOINT` set, every request is one trace exported over OTLP/HTTP: FastMCP's server span (`tools/call <tool>`, continuing a `traceparent` the caller sent in `_meta`) with `enduser.id` and `lares.client_id` on it, and under it the database queries and the outgoing HTTP calls. Calls to the trigger carry `traceparent`, so a run `start_run` asks for continues the call's trace. Spans carry names and numbers, never a query's parameters or a tool's arguments; an outgoing call names its host, not its path. The readiness probe leaves no trace. New traces are kept at `MCP_TRACING_SAMPLING_RATIO`, decided by trace id; a trace that arrives sampled stays sampled. A log line inside a sampled span carries its `trace_id` and `span_id`.

### Operational endpoints

- `GET /livez` — liveness: process is up; never depends on DB/NATS health.
- `GET /healthz` — readiness/deep health: 503 when the database is unreachable; reports NATS connectivity when the live tools are enabled. Neither the wiki, the backup server nor the object store is part of it — their outage fails their tools, not the pod.
- `GET :9090/metrics` — Prometheus metrics (tool calls per client, DB query durations, NATS fetches).

### Container image

Multi-arch images are published on every release tag:

```bash
docker run --rm -p 8080:8080 \
  -e MCP_DB_HOST=host.docker.internal \
  -e MCP_DB_NAME=mydb \
  -e MCP_DB_USERNAME=mcp_readonly \
  -e MCP_DB_PASSWORD=secret \
  -e MCP_NATS_ENABLED=false \
  -e MCP_GATEWAY_KEY=<at least 32 characters> \
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

Tests use [`testcontainers`](https://testcontainers-python.readthedocs.io/) to spin up a real TimescaleDB instance — no DB mocks — and a real rustfs for the storage tools; external HTTP (Wiki.js, the backup server, the trigger) is faked with `respx`.

## License

GPL-2.0-or-later. See [LICENSE](LICENSE).
