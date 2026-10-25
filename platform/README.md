# Source system (hybrid: local "fleet company" -> Fabric cloud)

```
generator ──HTTP──▶ ingestion API ──▶ PostgreSQL ──CDC (Debezium)──▶ Kafka ──bridge (Kafka protocol)──▶ Fabric Eventstream v2 ──▶ bronze_telemetry_v2
                     (validates,        (source DB:                       (topic fleet.public.         (at-least-once:
                      keeps every        telemetry_live,                   telemetry_live)              commit only after
                      line + reason)     telemetry_history)                                             the cloud acks)
```

The laptop plays the fleet company's own systems (API, operational database, Kafka); only the bridge
talks to the cloud. The pilot path (`bronze_telemetry`) is untouched.

## Run

```bash
cd platform
docker compose up -d --build                          # postgres, kafka, debezium connect, api, kafka-ui
./connect/register.sh                                 # CDC connector: telemetry_live -> Kafka
docker compose --profile cloud up -d --build bridge   # Kafka -> Fabric (needs EVENTSTREAM_V2_CONNECTION_STRING in ../generator/.env)

cd ../generator
.venv/bin/python run_fleet.py --mode api --channel live --from-hours 72 --hours 96 --rate 700 --minutes 75
```

| What | Where |
|---|---|
| Ingestion API (+ docs) | http://localhost:8000/docs |
| Kafka UI (topics, messages, consumer lag) | http://localhost:8085 |
| PostgreSQL | `localhost:55432`, db/user `fleet` (local only) |
| Debezium Connect REST | http://localhost:8083/connectors |
| Bridge log | `platform/bridge/logs/bridge_intervals.csv` (delivered, events/s, Kafka lag per 10 s) |
| Cloud check | `ingestion/notebooks/07_check_bronze_v2.py` |

## Smoke test (2026-10-25)

50 vehicles, simulated 2026-11-04 from 06:00, paced at 200 events/s for 1 minute:

| Hop | Count |
|---|---|
| Generator sent | 12,325 |
| PostgreSQL `telemetry_live` (via API) | 12,325, of which 164 rejected with a reason |
| Kafka topic (via Debezium) | 12,325 |
| Bridge delivered to Fabric | 12,325 |
| `bronze_telemetry_v2` | 12,325 distinct `db_id` |

Rejected by type at every hop: 39 malformed, 50 missing field, 34 null, 41 wrong type = the generator's manifest.

**Incident during setup:** while recovering from a mistyped connection string, the bridge offsets were
rewound and the bridge rebuilt; an old container briefly ran alongside, so the cloud received every row
twice (24,650 rows, 12,325 distinct `db_id`). Bronze keeps what was received; `db_id` removes the
duplicates in silver. A plain restart and a rebuild afterwards re-sent nothing. The same incident exposed a
bridge bug (unacknowledged sends were counted as delivered), fixed before the re-send.
