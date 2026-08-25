#!/usr/bin/env bash
# Brings up a cluster, deploys the sample workload, and applies ramped load.
set -euo pipefail

cd "$(dirname "$0")/.."
RUN_DIR="${RUN_DIR:-/tmp/mini-k8s-run}"
API="localhost:${API_PORT:-8080}"

./scripts/cluster.sh up

curl -sS -X POST "$API/deployments" -H 'content-type: application/json' -d "{
  \"name\": \"webapp\",
  \"exec_path\": \"$RUN_DIR/bin/webapp\",
  \"port\": 8000,
  \"cpu_request\": 150,
  \"mem_request\": 128,
  \"min_replicas\": 2,
  \"max_replicas\": 8,
  \"target_cpu_percentage\": 70
}"
echo

curl -sS -X POST "$API/deployments/webapp/scale" \
  -H 'content-type: application/json' -d '{"desired_replicas": 2}'
echo

echo "waiting for pods to come up..."
sleep 6
curl -sS "$API/state" | head -c 600
echo

PORT=$(for k in $(redis-cli -p "${REDIS_PORT:-6379}" --scan --pattern 'pod:webapp:*'); do
  redis-cli -p "${REDIS_PORT:-6379}" hget "$k" host_port
done | head -1)
[ -n "$PORT" ] || { echo "no pod came up; see $RUN_DIR/log/" >&2; exit 1; }

echo "applying ramped load to port $PORT (watch the dashboard at http://$API/)"
"$RUN_DIR/bin/loadgen" -target "http://localhost:$PORT/" \
  -start-rps 20 -peak-rps 200 -step-rps 20 -step-every 5s -hold 45s

echo "final state:"
curl -sS "$API/state" | head -c 600
echo
echo "run ./scripts/cluster.sh down when finished"
