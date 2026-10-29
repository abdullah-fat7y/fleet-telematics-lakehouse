#!/usr/bin/env bash
# Push a new day of fleet data through the whole source system, for a live pipeline test.
#
#   ./push_new_data.sh --dry-run                 checks + plan only, nothing is sent (rehearse with this)
#   ./push_new_data.sh                           day 5: batch 06:00-09:00 + stream from 09:00 for 10 min at 700/s
#   ./push_new_data.sh --batch-hours 2 --stream-minutes 5 --rate 500
#
# Batch : generator -> API (history) -> PostgreSQL -> extractor -> Files/landing_v2   (DAG task 11 loads it)
# Stream: generator -> API (live) -> PostgreSQL -> Debezium -> Kafka -> bridge -> Eventstream v2 -> bronze
# Both start together (each first fast-forwards the earlier days, ~5 min), then the stream runs paced.
# Finally the 5-day trips and the truth tables are uploaded for notebook 13.
# Refuses to send a time window that was already sent (no duplicates), unless --force.
set -euo pipefail

DAY=5 BATCH_HOURS=3 STREAM_MINUTES=10 RATE=700 DRY_RUN=0 FORCE=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --day) DAY=$2; shift 2 ;;
    --batch-hours) BATCH_HOURS=$2; shift 2 ;;
    --stream-minutes) STREAM_MINUTES=$2; shift 2 ;;
    --rate) RATE=$2; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --force) FORCE=1; shift ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) echo "unknown option $1"; exit 2 ;;
  esac
done

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
GEN="$REPO/generator"
PY="${PYTHON:-}"
for c in "$GEN/.venv/bin/python" "$REPO/../generator/.venv/bin/python"; do [[ -z "$PY" && -x "$c" ]] && PY="$c"; done
[[ -n "$PY" ]] || { echo "No Python venv found (set PYTHON=...)"; exit 1; }
LOGS="$HERE/logs/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$LOGS"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
ok()   { printf '  \033[32m✔\033[0m %s\n' "$*"; }
bad()  { printf '  \033[31m✘\033[0m %s\n' "$*"; FAILED=1; }
psql_q() { docker exec fleet-platform-postgres-1 psql -U fleet -d fleet -tAc "$1"; }
kafka_lag() { docker exec fleet-platform-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
                --describe --group fleet-cloud-bridge 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {l+=$6} END {print l+0}'; }

# Simulated hours are counted from the simulation start (2026-11-01 06:00 UTC); day N starts at (N-1)*24.
read -r B_FROM B_TO S_FROM S_TO B_START B_END S_START CFG_DAYS < <("$PY" - "$GEN/config.yaml" "$DAY" "$BATCH_HOURS" <<'EOF'
import sys, yaml
from datetime import datetime, timedelta, timezone
cfg = yaml.safe_load(open(sys.argv[1])); day, bh = int(sys.argv[2]), float(sys.argv[3])
start = datetime.strptime(cfg["simulation"]["start_time_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
b_from = (day - 1) * 24; b_to = b_from + bh; s_from = b_to; s_to = b_from + 24
t = lambda h: (start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ")
print(b_from, b_to, s_from, s_to, t(b_from), t(b_to), t(s_from), cfg["simulation"].get("days", 1))
EOF
)

bold "Plan"
echo "  Batch : $B_START -> $B_END (simulated), channel=history, then hourly extract to Files/landing_v2"
echo "  Stream: from $S_START (simulated), channel=live, ${STREAM_MINUTES} min at ${RATE} events/s ≈ $((STREAM_MINUTES * 60 * RATE)) events"
echo "  Logs  : $LOGS"

bold "Checks"
FAILED=0
docker info >/dev/null 2>&1 && ok "Docker is running" || bad "Docker is not running"
for s in postgres kafka connect api bridge; do
  docker ps --format '{{.Names}}' | grep -q "fleet-platform-$s-1" && ok "$s is up" || bad "$s is not running"
done
curl -sf -m 5 localhost:8000/health >/dev/null && ok "API answers" || bad "API does not answer on :8000"
curl -sf -m 5 localhost:8083/connectors/fleet-postgres-cdc/status | grep -q '"state":"RUNNING"' \
  && ok "CDC connector running" || bad "CDC connector not running (./connect/register.sh)"
LAG=$(kafka_lag); [[ "$LAG" == "0" ]] && ok "bridge has nothing waiting (Kafka lag 0)" || echo "  ! bridge Kafka lag is $LAG (still delivering)"
grep -q '^EVENTSTREAM_V2_CONNECTION_STRING=Endpoint=' "$GEN/.env" && ok "Eventstream v2 connection string set" || bad "EVENTSTREAM_V2_CONNECTION_STRING missing in generator/.env"
grep -q '^FABRIC_TENANT_ID=' "$GEN/.env" && ok "Fabric tenant set (for uploads)" || bad "FABRIC_TENANT_ID missing in generator/.env"
[[ "$DAY" -le "$CFG_DAYS" ]] && ok "reference data has trips for day $DAY" || bad "config has only $CFG_DAYS days of trips (set simulation.days and run generate.py reference)"
FREE_GB=$(df -g "$REPO" | awk 'NR==2 {print $4}')
[[ "$FREE_GB" -ge 5 ]] && ok "disk: ${FREE_GB} GB free" || bad "disk: only ${FREE_GB} GB free"
ALREADY_B=$(psql_q "select count(*) from telemetry_history where event_time >= '$B_START' and event_time < '$B_END'")
ALREADY_S=$(psql_q "select count(*) from telemetry_live where event_time >= '$S_START' and event_time < timestamptz '$S_START' + interval '1 day'")
if [[ "$ALREADY_B" == "0" && "$ALREADY_S" == "0" ]]; then ok "this time window was never sent (no duplicates)"
elif [[ "$FORCE" == "1" ]]; then echo "  ! window already has data ($ALREADY_B batch, $ALREADY_S stream rows) - sending anyway (--force)"
else bad "window already sent ($ALREADY_B batch, $ALREADY_S stream rows) - pick another --day or use --force"; fi
[[ "$FAILED" == "0" ]] || { echo; echo "Fix the ✘ items first."; exit 1; }

if [[ "$DRY_RUN" == "1" ]]; then
  bold "Dry run - nothing was sent. Without --dry-run this would run:"
  echo "  $PY run_fleet.py --mode api --channel history --from-hours $B_FROM --hours $B_TO"
  echo "  $PY run_fleet.py --mode api --channel live --from-hours $S_FROM --hours $S_TO --rate $RATE --minutes $STREAM_MINUTES"
  echo "  $PY extract.py --incremental"
  echo "  $PY upload_to_onelake.py reference && $PY upload_to_onelake.py truth"
  exit 0
fi

BEFORE_H=$(psql_q "select count(*) from telemetry_history"); BEFORE_L=$(psql_q "select count(*) from telemetry_live")
STARTED=$(date +%s)

bold "1. Generators start (batch + stream); each first fast-forwards to day $DAY (~5 min)"
(cd "$GEN" && "$PY" -u run_fleet.py --mode api --channel history --from-hours "$B_FROM" --hours "$B_TO" > "$LOGS/batch_generator.log" 2>&1; echo "EXIT $?" >> "$LOGS/batch_generator.log") &
(cd "$GEN" && "$PY" -u run_fleet.py --mode api --channel live --from-hours "$S_FROM" --hours "$S_TO" --rate "$RATE" --minutes "$STREAM_MINUTES" > "$LOGS/stream_generator.log" 2>&1; echo "EXIT $?" >> "$LOGS/stream_generator.log") &

last_sent() { grep -E ' sent ' "$1" 2>/dev/null | tail -1 | awk '{print $3}'; }
EXTRACT_STARTED=0
while true; do
  sleep 15
  B=$(last_sent "$LOGS/batch_generator.log"); S=$(last_sent "$LOGS/stream_generator.log")
  B_DONE=$(grep -c '^EXIT' "$LOGS/batch_generator.log" 2>/dev/null || true); S_DONE=$(grep -c '^EXIT' "$LOGS/stream_generator.log" 2>/dev/null || true)
  if [[ "$B_DONE" == "1" && "$EXTRACT_STARTED" == "0" ]]; then
    grep -q '^EXIT 0' "$LOGS/batch_generator.log" || { echo "batch generator failed - see $LOGS/batch_generator.log"; exit 1; }
    (cd "$REPO/platform/extractor" && "$PY" -u extract.py --incremental > "$LOGS/extract.log" 2>&1; echo "EXIT $?" >> "$LOGS/extract.log") &
    EXTRACT_STARTED=1
  fi
  X=$(grep -cE '^  20[0-9-]+ [0-9]+:00 ' "$LOGS/extract.log" 2>/dev/null || true)
  printf '  %4ss | batch: %-9s %s | extract: %s hour(s) | stream: %-9s %s | kafka lag %s\n' "$(( $(date +%s) - STARTED ))" \
    "${B:-warming}" "$([[ $B_DONE == 1 ]] && echo done || echo ...)" "${X:-0}" "${S:-warming}" "$([[ $S_DONE == 1 ]] && echo done || echo ...)" "$(kafka_lag)"
  [[ "$S_DONE" == "1" && "$EXTRACT_STARTED" == "1" ]] && grep -q '^EXIT' "$LOGS/extract.log" 2>/dev/null && break
done
grep -q '^EXIT 0' "$LOGS/stream_generator.log" || { echo "stream generator failed - see $LOGS/stream_generator.log"; exit 1; }
grep -q '^EXIT 0' "$LOGS/extract.log" || { echo "extractor failed - see $LOGS/extract.log (re-run: extract.py --incremental)"; exit 1; }

bold "2. Waiting for the bridge to deliver everything to the cloud"
for i in $(seq 1 60); do L=$(kafka_lag); echo "  kafka lag $L"; [[ "$L" == "0" ]] && break; sleep 5; done

bold "3. Uploading 5-day trips and truth tables (for notebook 13)"
(cd "$GEN" && "$PY" upload_to_onelake.py reference && "$PY" upload_to_onelake.py truth) 2>&1 | tee "$LOGS/uploads.log" | grep -E "trips.csv|truth:" || true

AFTER_H=$(psql_q "select count(*) from telemetry_history"); AFTER_L=$(psql_q "select count(*) from telemetry_live")
B_RUN=$(grep -oE 'api-history-[0-9TZ]+' "$LOGS/batch_generator.log" | head -1); S_RUN=$(grep -oE 'api-live-[0-9TZ]+' "$LOGS/stream_generator.log" | head -1)
bold "Done in $(( ($(date +%s) - STARTED) / 60 )) min"
echo "  Batch  run $B_RUN : $((AFTER_H - BEFORE_H)) rows into the source DB, extracted to Files/landing_v2 ($X hour files)"
echo "  Stream run $S_RUN : $((AFTER_L - BEFORE_L)) rows into the source DB -> Kafka -> cloud (Kafka lag $(kafka_lag))"
echo
echo "  Next, in Fabric:"
echo "   1. notebook 13_load_reference_and_truth_v2  (5-day ref_trips + telemetry_truth_v2)"
echo "   2. the DAG: 11_load_landing_v2 -> silver -> anomalies -> gold NB00-NB04 -> refresh dashboard"
echo "   3. check: bronze_telemetry_v2 has run_ids $B_RUN (db_extract) and $S_RUN (kafka_cdc)"
