#!/usr/bin/env bash
# One-shot end-to-end launcher for the streaming demo.
# Boots infra, waits for Kafka, prints the dashboard URL, then runs the
# Silver + Gold batch jobs so the BatchStats panel has real data.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="docker compose -f $ROOT_DIR/infra/docker-compose.yml"

step() { printf "\n\033[1;36m▸ %s\033[0m\n" "$*"; }
ok()   { printf "\033[1;32m✓ %s\033[0m\n" "$*"; }

step "Building and starting infrastructure"
$COMPOSE up -d --build kafka redpanda-console fastapi producer spark-streaming dashboard

step "Waiting for Kafka to be healthy"
for i in $(seq 1 60); do
  status="$($COMPOSE ps --format json kafka | sed -n 's/.*"Health":"\([^"]*\)".*/\1/p' | head -n1 || true)"
  if [[ "$status" == "healthy" ]]; then ok "Kafka healthy"; break; fi
  sleep 2
  [[ $i -eq 60 ]] && { echo "Kafka never became healthy"; exit 1; }
done

step "Waiting for FastAPI /health"
for i in $(seq 1 30); do
  if curl -fsS http://localhost:8000/health >/dev/null 2>&1; then ok "FastAPI ready"; break; fi
  sleep 2
done

cat <<EOF

==================================================================
  Open the live dashboard:  http://localhost:3001
  Kafka UI (Redpanda):      http://localhost:9021
  FastAPI docs:             http://localhost:8000/docs
  Spark UI:                 http://localhost:8080
==================================================================

Streaming events are flowing now. Micro-batches land on the
dashboard every ~5 seconds.

EOF

step "Letting events pile up for ~30 s before batch jobs"
sleep 30

step "Running Silver batch job (Bronze → Silver, MERGE upsert)"
$COMPOSE --profile batch run --rm spark-batch /app/batch_silver.py

step "Running Gold batch job (Silver → Gold, business aggregates)"
$COMPOSE --profile batch run --rm spark-batch /app/batch_gold.py

ok "Batch jobs complete — refresh http://localhost:3001 to see BatchStats"
