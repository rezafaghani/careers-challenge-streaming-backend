#!/usr/bin/env bash
set -euo pipefail

target=${SERVICE_URL:-http://localhost:8080}
devices=${DEVICES:-500}
duration=${DURATION:-60}
python3 event_generator/generate.py --mode burst --target "$target" --devices "$devices" --duration "$duration"
curl -fsS "$target/metrics"
curl -fsS "$target/alarms?since=0" | python3 -c 'import json,sys; print("alarms", len(json.load(sys.stdin)["alarms"]))'
