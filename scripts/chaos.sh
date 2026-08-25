#!/usr/bin/env bash
# Runs the README's three chaos scenarios against a running cluster.
#
# Each scenario asserts on state in Redis rather than on log output: a log line
# says a controller believed it acted, whereas the recorded state says the
# cluster actually converged.
set -euo pipefail

cd "$(dirname "$0")/.."
RUN_DIR="${RUN_DIR:-/tmp/mini-k8s-run}"
REDIS_PORT="${REDIS_PORT:-6379}"
API="localhost:${API_PORT:-8080}"
DEP="${DEP:-webapp}"

r() { redis-cli -p "$REDIS_PORT" "$@"; }
log() { printf '\n=== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

pod_keys() { r --scan --pattern "pod:$DEP:*"; }

# Set by the scenarios and read by their wait helpers, which take no arguments
# so they can be passed to wait_for.
VICTIM_KEY=""
DEAD_PID=""
DEAD_NODE=""

running_count() {
  local n=0
  for k in $(pod_keys); do
    [ "$(r hget "$k" status)" = "Running" ] && n=$((n + 1))
  done
  echo "$n"
}

# wait_for polls a command until it prints the expected value, so assertions
# describe the state that must be reached rather than a duration to sleep for.
wait_for() {
  local want="$1" timeout="$2"; shift 2
  local deadline=$((SECONDS + timeout))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ "$("$@")" = "$want" ] && return 0
    sleep 1
  done
  return 1
}

# restarted_in_place prints "yes" once the pod is backed by a different, live
# process than the one recorded in DEAD_PID.
restarted_in_place() {
  local status pid
  status=$(r hget "$VICTIM_KEY" status)
  pid=$(r hget "$VICTIM_KEY" pid)
  if [ "$status" = "Running" ] && [ -n "$pid" ] && [ "$pid" != "$DEAD_PID" ] \
     && kill -0 "$pid" 2>/dev/null; then
    echo yes
  else
    echo no
  fi
}

# running_on_dead_node counts pods still recorded as Running on DEAD_NODE.
running_on_dead_node() {
  local n=0
  for k in $(pod_keys); do
    if [ "$(r hget "$k" node_id)" = "$DEAD_NODE" ] && [ "$(r hget "$k" status)" = "Running" ]; then
      n=$((n + 1))
    fi
  done
  echo "$n"
}

# rescheduled_off_dead_node prints "yes" once the deployment is back to the
# expected replica count with nothing left running on the dead node.
rescheduled_off_dead_node() {
  local want="$1"
  if [ "$(running_count)" -ge "$want" ] && [ "$(running_on_dead_node)" -eq 0 ]; then
    echo yes
  else
    echo no
  fi
}

scenario_pod_failure() {
  log "Scenario 1: kill a workload process, expect a restart in place"
  local before
  before=$(running_count)
  [ "$before" -gt 0 ] || fail "no running pods to kill"

  VICTIM_KEY=$(pod_keys | head -1)
  DEAD_PID=$(r hget "$VICTIM_KEY" pid)
  echo "killing pid $DEAD_PID of ${VICTIM_KEY#pod:}"
  kill -9 "$DEAD_PID" 2>/dev/null || fail "could not kill $DEAD_PID"

  # Waiting on the replica count would prove nothing here: the record still says
  # Running, so the count is already satisfied before any healing happens. The
  # condition that matters is a different live process backing the same pod.
  #
  # The budget covers the whole detection path: FAILURE_THRESHOLD probes at
  # HEALTH_CHECK_INTERVAL apart, then the restart backoff.
  if ! wait_for yes 90 restarted_in_place; then
    fail "pod was not restarted (status=$(r hget "$VICTIM_KEY" status), pid=$(r hget "$VICTIM_KEY" pid))"
  fi
  local newpid
  newpid=$(r hget "$VICTIM_KEY" pid)
  [ "$(running_count)" -ge "$before" ] \
    || fail "replica count dropped to $(running_count) from $before"
  echo "OK: pod restarted in place, pid $DEAD_PID -> $newpid, $(running_count) replicas running"
}

scenario_node_failure() {
  log "Scenario 2: kill a node agent, expect its pods rescheduled elsewhere"
  local before victim node
  before=$(running_count)

  # Pick a node that actually holds pods; killing an idle agent proves nothing.
  node=$(for k in $(pod_keys); do r hget "$k" node_id; done | sort | uniq -c | sort -rn | head -1 | awk '{print $2}')
  [ -n "$node" ] || fail "no node holds pods"
  echo "killing agent $node and its workloads (simulating host loss)"

  # SIGKILL the agent, then its workloads: a real node loss takes both, and
  # leaving the processes alive would let the pods be re-adopted rather than
  # rescheduled.
  kill -9 "$(cat "$RUN_DIR/pid/$node.pid")" 2>/dev/null || true
  for k in $(pod_keys); do
    if [ "$(r hget "$k" node_id)" = "$node" ]; then
      kill -9 "$(r hget "$k" pid)" 2>/dev/null || true
    fi
  done

  DEAD_NODE="$node"
  # The lease must lapse before eviction is even correct, and the stale records
  # still read as Running until it does, so the count alone would be satisfied
  # before any healing happened. This waits on the real end state: the desired
  # number of pods Running, none of them on the node that died.
  #
  # The budget covers the lease TTL, the eviction, and rescheduling.
  if ! wait_for yes 120 rescheduled_off_dead_node "$before"; then
    fail "pods were not rescheduled off $node: $(running_count) of $before running, $(running_on_dead_node) still on it"
  fi
  echo "OK: $(running_count) replicas running, none on $node"
}

scenario_autoscale() {
  log "Scenario 3: ramp load, expect scale up then a cooldown-guarded scale down"
  local start_replicas port peak
  start_replicas=$(r hget "deployment:$DEP" desired_replicas)
  port=$(for k in $(pod_keys); do r hget "$k" host_port; done | head -1)
  [ -n "$port" ] || fail "no pod port to target"

  echo "starting at $start_replicas replicas, ramping load against port $port"
  "$RUN_DIR/bin/loadgen" -target "http://localhost:$port/" \
    -start-rps 40 -peak-rps 300 -step-rps 40 -step-every 4s -hold 30s \
    > "$RUN_DIR/log/loadgen.log" 2>&1 &
  local loadpid=$!

  # Scaling up has to happen while the load is still being applied.
  local deadline=$((SECONDS + 90)) scaled=0
  while [ "$SECONDS" -lt "$deadline" ]; do
    peak=$(r hget "deployment:$DEP" desired_replicas)
    if [ "$peak" -gt "$start_replicas" ]; then scaled=1; break; fi
    sleep 2
  done
  wait "$loadpid" 2>/dev/null || true
  [ "$scaled" = 1 ] || fail "no scale-up under load (still $start_replicas replicas)"
  echo "scaled up: $start_replicas -> $peak replicas"

  # The anti-flapping guarantee: the count must hold through the cooldown.
  local cooldown="${SCALE_DOWN_COOLDOWN_SECONDS:-180}"
  echo "load stopped; verifying the count holds for the ${cooldown}s scale-down cooldown"
  local check=$((SECONDS + 30)) now
  while [ "$SECONDS" -lt "$check" ]; do
    now=$(r hget "deployment:$DEP" desired_replicas)
    [ "$now" -lt "$peak" ] && fail "scaled down to $now during the cooldown window"
    sleep 3
  done
  echo "OK: held at $peak replicas through the sampled cooldown window"
  echo "note: the full scale-down takes up to ${cooldown}s after the last scale action"
}

case "${1:-all}" in
  pod)  scenario_pod_failure ;;
  node) scenario_node_failure ;;
  hpa)  scenario_autoscale ;;
  all)
    scenario_pod_failure
    scenario_node_failure
    scenario_autoscale
    printf '\nall scenarios passed\n'
    ;;
  *) echo "usage: $0 {all|pod|node|hpa}" >&2; exit 2 ;;
esac
