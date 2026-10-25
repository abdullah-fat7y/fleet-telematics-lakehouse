#!/usr/bin/env bash
# Registers (or updates) the Debezium PostgreSQL CDC connector: every new row in
# public.telemetry_live becomes a JSON message on Kafka topic fleet.public.telemetry_live.
set -euo pipefail
cd "$(dirname "$0")"
until curl -sf http://localhost:8083/connectors >/dev/null; do echo "waiting for Kafka Connect..."; sleep 3; done
name=$(python3 -c 'import json;print(json.load(open("postgres-cdc.json"))["name"])')
python3 -c 'import json;print(json.dumps(json.load(open("postgres-cdc.json"))["config"]))' |
  curl -sf -X PUT -H "Content-Type: application/json" --data @- "http://localhost:8083/connectors/$name/config" >/dev/null
sleep 3
curl -s "http://localhost:8083/connectors/$name/status"; echo
