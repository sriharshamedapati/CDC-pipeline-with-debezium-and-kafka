# CDC Pipeline with Debezium and Kafka

A real-time Change Data Capture (CDC) pipeline that streams row-level changes from a source PostgreSQL database to a target data warehouse using Debezium and Apache Kafka.

## Architecture

```
source-db (PostgreSQL)
    │  logical replication (pgoutput)
    ▼
connect (Debezium / Kafka Connect)
    │  publishes change events
    ▼
kafka (Apache Kafka)
    │  topic: dbserver1.public.products
    ▼
consumer (Python service)
    │  transforms & upserts
    ▼
target-db (PostgreSQL — data warehouse)
```

**Services:**

| Service | Image | Role |
|---|---|---|
| zookeeper | confluentinc/cp-zookeeper:7.3.0 | Coordination for Kafka |
| kafka | confluentinc/cp-kafka:7.3.0 | Event streaming broker |
| source-db | postgres:13 | Source database (WAL logical replication) |
| target-db | postgres:13 | Target data warehouse |
| connect | debezium/connect:2.5 | Kafka Connect + Debezium PostgreSQL connector |
| connector-init | curlimages/curl | One-shot: registers Debezium connector on startup |
| consumer | custom Python | Reads Kafka topic, transforms, writes to target-db |

## Data Flow

1. A row is inserted/updated/deleted in `source_db.public.products`.
2. PostgreSQL writes the change to the Write-Ahead Log (WAL) at `wal_level=logical`.
3. Debezium reads the WAL via the `pgoutput` plugin and publishes a JSON event to the Kafka topic `dbserver1.public.products`.
4. The consumer service reads the event, identifies the operation (`c`, `u`, `d`), applies transformations, and performs an idempotent UPSERT or DELETE on `target_db.public.products_replicated`.

## Data Transformation

| Source column | Target column | Transform |
|---|---|---|
| `price_cents` (INTEGER) | `price_dollars` (DECIMAL 10,2) | divide by 100 |
| `created_at` (TIMESTAMPTZ) | `source_created_at` (TIMESTAMPTZ) | rename only |
| all other columns | same name | pass-through (supports schema evolution) |

## Prerequisites

- Docker ≥ 20.10
- Docker Compose ≥ 2.0

## Quick Start

```bash
# 1. Clone the repository
git clone <repo-url>
cd CDC-pipeline-with-debezium-and-kafka

# 2. Create your environment file
cp .env.example .env
# Edit .env if you need custom passwords (defaults work out of the box)

# 3. Start all services
docker compose up --build -d

# 4. Check status (allow ~3 minutes for full startup)
docker compose ps
```

All services should show as `healthy` within 3–5 minutes. The `connector-init` service will exit with code 0 after registering the Debezium connector — this is expected.

## Verify the Pipeline

```bash
# Confirm connector is RUNNING
curl -s http://localhost:8083/connectors/inventory-connector/status | python3 -m json.tool

# Insert a record into source
docker exec -it source-db psql -U postgres -d source_db \
  -c "INSERT INTO products (id, name, price_cents) VALUES (999, 'Test Product', 12345);"

# Wait ~5 seconds, then query target
docker exec -it target-db psql -U postgres -d target_db \
  -c "SELECT * FROM products_replicated WHERE id = 999;"
# Expected: price_dollars = 123.45

# Test UPDATE
docker exec -it source-db psql -U postgres -d source_db \
  -c "UPDATE products SET name='Updated', price_cents=54321 WHERE id=999;"

# Test DELETE
docker exec -it source-db psql -U postgres -d source_db \
  -c "DELETE FROM products WHERE id=999;"

# Schema evolution: add a new column
docker exec -it target-db psql -U postgres -d target_db \
  -c "ALTER TABLE products_replicated ADD COLUMN description VARCHAR(255);"
docker exec -it source-db psql -U postgres -d source_db \
  -c "ALTER TABLE products ADD COLUMN description VARCHAR(255);"
docker exec -it source-db psql -U postgres -d source_db \
  -c "UPDATE products SET description='New Description' WHERE id=1;"
```

## Stopping the Pipeline

```bash
docker compose down           # stop and remove containers
docker compose down -v        # also remove volumes (full reset)
```

## Project Structure

```
.
├── docker-compose.yml            # All services
├── .env.example                  # Environment variable template
├── README.md
├── connectors/
│   └── debezium-pg-connector.json  # Debezium connector config
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       └── consumer.py           # CDC consumer logic
├── db-init/
│   ├── source/
│   │   └── init.sql              # source_db schema + seed data
│   └── target/
│       └── init.sql              # target_db schema
└── scripts/
    └── register-connector.sh     # Connector registration (one-shot)
```

## Design Decisions

- **Idempotency**: all writes use `INSERT ... ON CONFLICT (id) DO UPDATE`, so replaying messages is safe.
- **Schema evolution**: the consumer dynamically maps all fields from the Debezium `after` payload; new source columns are forwarded automatically and added to the target table via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
- **Tombstone handling**: Kafka messages with a null value (tombstones emitted after deletes for log compaction) are detected and skipped cleanly.
- **Config via environment variables**: all connection details are read from environment variables with sensible defaults, documented in `.env.example`.
- **Retry logic**: both the Kafka consumer and the DB connection loop on failure with a 5-second backoff.
