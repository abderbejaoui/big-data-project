# Spark Structured Streaming + Batch Demo

A production-style, end-to-end big-data demo you can **watch happen live**.
Mock events flow from a Python producer → Kafka → a Spark Structured
Streaming job that writes Bronze Delta tables and pushes every micro-batch
to a React dashboard via FastAPI WebSockets. Scheduled batch jobs promote
data from Bronze → Silver → Gold and expose business aggregates on the
same dashboard.

![architecture overview](docs/architecture.txt)

## Architecture

```
 ┌─────────────┐   JSON   ┌─────────┐   Structured Streaming   ┌──────────┐
 │ mock_produce│─────────▶│  Kafka  │─────────────────────────▶│  Spark   │
 │  (Faker)    │  5 ev/s  │ (KRaft) │   raw.orders / raw.clicks│ Streaming│
 └─────────────┘          └─────────┘                          └────┬─────┘
                                                                    │
                                            foreachBatch:           │  Delta
                                                                    ▼
                   ┌──────────────────────────────────┐       ┌──────────────┐
                   │        FastAPI backend           │       │ /data/bronze │
                   │  POST /internal/batch-event      │◀──────│ (append-only)│
                   │  WS   /ws/stream  (fan-out)      │       └──────┬───────┘
                   │  GET  /stats/orders  /stats/…    │              │
                   └────────────┬─────────────────────┘              │
                                │                                    │
                                ▼                        spark-submit (on-demand)
                      ┌───────────────────┐                          ▼
                      │ React Dashboard   │              ┌─────────────────────┐
                      │ (Recharts, dark)  │              │ Silver  (MERGE)     │
                      │ LiveFeed / Metric │              │ Gold   (aggregates) │
                      │ / BatchStats      │              └─────────────────────┘
                      └───────────────────┘
```

## Tech stack

| Layer             | Tool                                                  |
|-------------------|-------------------------------------------------------|
| Event generation  | Python 3.11, Faker, confluent-kafka, Loguru, Pydantic |
| Message bus       | Apache Kafka 3.7 (KRaft, no Zookeeper)                |
| Stream processing | Apache Spark 3.5 Structured Streaming + Delta 3.2     |
| Batch processing  | Apache Spark 3.5 + Delta MERGE                        |
| Storage           | Delta Lake (Bronze / Silver / Gold on local volume)   |
| API               | FastAPI + DuckDB (`delta_scan`) + WebSockets          |
| Dashboard         | React 18 + Vite + Recharts                            |
| Orchestration     | Docker Compose                                        |

## Project layout

```
.
├── producer/          # Faker → Kafka producer
├── spark/
│   ├── streaming_job.py   # Kafka → Bronze + WS push
│   ├── batch_silver.py    # Bronze → Silver (MERGE)
│   ├── batch_gold.py      # Silver → Gold (aggregates)
│   └── shared/            # schemas + transforms
├── api/               # FastAPI (WebSocket + REST)
├── dashboard/         # React + Recharts UI
├── infra/
│   └── docker-compose.yml
├── data/              # persisted Delta tables (bind-mounted into containers)
├── run_demo.sh        # one-shot end-to-end launcher
└── .env.example
```

## Quick start

```bash
cp .env.example .env
./run_demo.sh
```

Open:

- Dashboard  : <http://localhost:3000>
- Kafka UI   : <http://localhost:9021>
- FastAPI    : <http://localhost:8000/docs>
- Spark UI   : <http://localhost:8080>

`run_demo.sh`:

1. Builds and starts every service with one `docker compose up`.
2. Waits for Kafka to be healthy.
3. Waits for the FastAPI `/health` endpoint.
4. Sleeps ~30 s so there is enough Bronze data to work with.
5. Runs `batch_silver.py` then `batch_gold.py`.
6. Refresh the dashboard — the BatchStats panel now has real aggregates.

## What you see on the dashboard

- **Header** — total events this session, current rec/s, last micro-batch
  time, split of orders vs clicks.
- **Left panel (Streaming Layer · Live Feed)** — every micro-batch lands
  as a colored row: blue for `raw.orders`, green for `raw.clicks`. Rows
  animate in, newest on top, up to the last 50.
- **Top right (Real-time throughput)** — live `LineChart` over the last
  60 seconds, one line per topic, updated on every WebSocket message.
- **Bottom right (Batch Layer · Gold Aggregates)** — polls `/stats/orders`
  and `/stats/clicks` every 10 s. Bar chart of revenue per product,
  pie chart of order-status distribution, table of top pages.

## REST & WebSocket surface

| Method | Path                    | Notes                                          |
|--------|-------------------------|------------------------------------------------|
| GET    | `/health`               | liveness                                       |
| GET    | `/bronze/count`         | rows in Bronze orders/clicks                   |
| GET    | `/silver/count`         | rows in Silver orders/clicks                   |
| GET    | `/layers/summary`       | combined layer row counts                      |
| GET    | `/stats/orders`         | Gold: revenue / status / per-minute            |
| GET    | `/stats/clicks`         | Gold: top pages / actions / devices            |
| GET    | `/stream/stats`         | # ws clients + last batch timestamp            |
| POST   | `/internal/batch-event` | called by Spark after each micro-batch         |
| WS     | `/ws/stream`            | live broadcast of every micro-batch summary    |

Batch summary payload:

```json
{
  "type": "batch",
  "batch_id": 12,
  "topic": "raw.orders",
  "records": 47,
  "timestamp": "2026-04-23T13:37:00.123+00:00",
  "sample": [ { "order_id": "…", "product": "…", "amount": 199.90, "…": "…" } ]
}
```

## Running individual pieces

```bash
# just the infra
docker compose -f infra/docker-compose.yml up -d kafka redpanda-console fastapi

# stop everything (keep data)
docker compose -f infra/docker-compose.yml down

# nuke data volume too
docker compose -f infra/docker-compose.yml down
rm -rf data/

# trigger just the batch jobs
docker compose -f infra/docker-compose.yml --profile batch \
  run --rm spark-batch /app/batch_silver.py

docker compose -f infra/docker-compose.yml --profile batch \
  run --rm spark-batch /app/batch_gold.py
```

## Configuration

All tunables live in `.env.example` — copy to `.env`. Key variables:

| Variable                   | Default        | Effect                                    |
|----------------------------|----------------|-------------------------------------------|
| `EVENTS_PER_SECOND`        | `5`            | producer rate                             |
| `STREAM_TRIGGER_SECONDS`   | `5`            | Structured Streaming micro-batch cadence  |
| `KAFKA_BOOTSTRAP_SERVERS`  | `kafka:9092`   | in-compose broker address                 |
| `KAFKA_BOOTSTRAP_SERVERS_HOST` | `localhost:29092` | host-side broker address          |
| `BRONZE_PATH` / `SILVER_PATH` / `GOLD_PATH` | `/data/...` | Delta roots (bind-mounted) |

## Code quality

- Full type hints (PEP 604 / modern `|` syntax).
- Loguru for structured logging across producer, streaming job, batch
  jobs, and API — no bare `print`.
- Pydantic models for every API payload and for the event generators.
- All configuration via env vars — no hardcoded hosts / paths.
- Single Spark image shared by streaming + batch services, so job code
  only needs to change in one place.

## Screenshots guide

When capturing screenshots:

1. Let the stack run ~60 s so MetricsChart has a full window.
2. Trigger `batch_silver` + `batch_gold` for BatchStats to populate.
3. Zoom to 90 % for the header + left panel + both right panels to fit.
4. Great captures: LiveFeed mid-animation, and the moment Gold tables
   appear in BatchStats.
