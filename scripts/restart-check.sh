#!/usr/bin/env bash
set -euo pipefail

target=${SERVICE_URL:-http://localhost:8080}
compose=${COMPOSE:-docker compose}
python3 event_generator/generate.py --mode baseline --target "$target" --devices "${DEVICES:-100}" --duration 20 &
generator=$!
sleep 5
$compose kill app
$compose up -d app
until curl -fsS "$target/metrics" >/dev/null; do sleep 1; done
wait "$generator"
until [ "$(curl -fsS "$target/metrics" | awk '/events_pending/{print $2}')" = 0 ]; do sleep 1; done
curl -fsS "$target/metrics"
echo "Restart recovery passed: the repository generator continued after the hard kill and the durable backlog drained"
