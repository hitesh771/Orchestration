#!/usr/bin/env bash
# Brings a mini-k8s cluster up or down on a single host.
#
# The README's topology puts each node agent in its own Incus container. This
# script runs them as processes on one host instead, because the orchestration
# logic under test does not depend on that isolation: a node is whatever holds a
# NODE_ID and a lease, and the control plane cannot tell the difference. What
# the container topology adds is network isolation, which matters for the
# throughput claim in NFR-1 and not for the correctness claims the chaos
# scenarios check.
set -euo pipefail

RUN_DIR="${RUN_DIR:-/tmp/mini-k8s-run}"
REDIS_PORT="${REDIS_PORT:-6379}"
API_PORT="${API_PORT:-8080}"
NODE_COUNT="${NODE_COUNT:-3}"
NODE_CPU="${NODE_CPU:-1000}"
NODE_MEM="${NODE_MEM:-2048}"

BIN_DIR="$RUN_DIR/bin"
LOG_DIR="$RUN_DIR/log"
PID_DIR="$RUN_DIR/pid"

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*"; }

build() {
  mkdir -p "$BIN_DIR" "$LOG_DIR" "$PID_DIR"
  log "building binaries"
  go build -o "$BIN_DIR/api-server" ./cmd/api-server
  go build -o "$BIN_DIR/node-agent" ./cmd/node-agent
  go build -o "$BIN_DIR/loadgen" ./cmd/loadgen
  go build -o "$BIN_DIR/webapp" ./examples/webapp
}

start_redis() {
  if redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1; then
    log "redis already running on $REDIS_PORT"
    return
  fi
  log "starting redis on $REDIS_PORT"
  # Keyspace expiry notifications are what make node-death detection immediate.
  # Without them the health controller still works, but only via its sweep.
  redis-server --port "$REDIS_PORT" --daemonize yes \
    --save '' --appendonly no \
    --notify-keyspace-events Ex \
    --dir "$RUN_DIR" --logfile "$LOG_DIR/redis.log"
  sleep 0.5
  redis-cli -p "$REDIS_PORT" ping >/dev/null
}

start_control_plane() {
  log "starting api-server on $API_PORT"
  REDIS_ADDR="localhost:$REDIS_PORT" PORT="$API_PORT" \
    "$BIN_DIR/api-server" > "$LOG_DIR/api-server.log" 2>&1 &
  echo $! > "$PID_DIR/api-server.pid"

  for _ in $(seq 30); do
    if curl -sf "localhost:$API_PORT/healthz" >/dev/null 2>&1; then return; fi
    sleep 0.2
  done
  echo "api-server did not become healthy; see $LOG_DIR/api-server.log" >&2
  return 1
}

start_nodes() {
  for i in $(seq 1 "$NODE_COUNT"); do
    start_node "node-$i"
  done
}

start_node() {
  local id="$1"
  log "starting agent $id"
  REDIS_ADDR="localhost:$REDIS_PORT" NODE_ID="$id" \
    TOTAL_CPU="$NODE_CPU" TOTAL_MEM="$NODE_MEM" \
    "$BIN_DIR/node-agent" > "$LOG_DIR/$id.log" 2>&1 &
  echo $! > "$PID_DIR/$id.pid"
}

stop_all() {
  log "stopping cluster"
  # Workloads first: an agent that is killed leaves its processes running for
  # re-adoption, which is correct in production and just litter here.
  pkill -f "$BIN_DIR/webapp" 2>/dev/null || true
  for pidfile in "$PID_DIR"/*.pid; do
    [ -e "$pidfile" ] || continue
    kill "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
  done
  sleep 0.5
  redis-cli -p "$REDIS_PORT" shutdown nosave 2>/dev/null || true
}

case "${1:-up}" in
  up)
    mkdir -p "$RUN_DIR"
    build
    start_redis
    start_control_plane
    start_nodes
    log "cluster up: dashboard at http://localhost:$API_PORT/"
    ;;
  down) stop_all ;;
  build) build ;;
  start-node) start_node "${2:?node id required}" ;;
  *) echo "usage: $0 {up|down|build|start-node <id>}" >&2; exit 2 ;;
esac
