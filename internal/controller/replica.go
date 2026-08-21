// Package controller holds the reconciliation loops that drive observed cluster
// state toward the declared spec.
//
// Reconciliation is dual-mode. A Pub/Sub subscriber reacts to deployment events
// within milliseconds, and a periodic sweep re-examines every deployment
// regardless of whether any event arrived. Both call the same delta engine, so
// the two modes cannot diverge in behavior.
//
// The sweep is what makes the system correct; the subscriber only makes it
// fast. Redis Pub/Sub is fire-and-forget: a message published while this
// process is restarting, or dropped by a slow consumer, is gone forever. A
// design that acted only on events would silently stop converging the moment
// one was missed, and nothing would ever notice. Treating events as a latency
// optimization means the worst case of a lost event is one sweep interval of
// delay rather than a permanently wrong cluster.
package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"mini-k8s/internal/capacity"
	"mini-k8s/internal/events"
	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/scheduler"
	"mini-k8s/internal/schema"
	"mini-k8s/internal/supervisor"
)

// DefaultSweepInterval is how often the level-triggered sweep runs when no
// interval is configured.
const DefaultSweepInterval = 10 * time.Second

// ReplicaController keeps the number of pods per deployment equal to its
// desired replica count.
type ReplicaController struct {
	client        *redisclient.Client
	logger        *logging.Logger
	sched         *scheduler.Scheduler
	sweepInterval time.Duration
}

// NewReplicaController builds a controller. A non-positive interval falls back
// to DefaultSweepInterval, since a zero-interval ticker would panic and a
// negative one would mean the safety net never runs.
func NewReplicaController(client *redisclient.Client, logger *logging.Logger, sweepInterval time.Duration) *ReplicaController {
	if sweepInterval <= 0 {
		sweepInterval = DefaultSweepInterval
	}
	return &ReplicaController{
		client:        client,
		logger:        logger,
		sched:         scheduler.New(client, logger),
		sweepInterval: sweepInterval,
	}
}

// Delta is the outcome of reconciling one deployment.
type Delta struct {
	Deployment string
	Desired    int
	Observed   int
	Created    int
	Removed    int
}

// Run starts both reconciliation modes and blocks until ctx is cancelled.
//
// The sweep runs once immediately. On startup the observed state is whatever
// the last process left behind, and waiting a full interval before looking at
// it would extend any outage across a control-plane restart.
func (c *ReplicaController) Run(ctx context.Context) {
	go c.RunSubscriber(ctx)
	c.RunSweeper(ctx)
}

// RunSweeper periodically reconciles every deployment.
func (c *ReplicaController) RunSweeper(ctx context.Context) {
	if _, err := c.ReconcileAll(ctx); err != nil && ctx.Err() == nil {
		c.logger.Error(ctx, "sweep_failed", err.Error())
	}

	ticker := time.NewTicker(c.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := c.ReconcileAll(ctx); err != nil && ctx.Err() == nil {
				c.logger.Error(ctx, "sweep_failed", err.Error())
			}
		}
	}
}

// RunSubscriber reconciles a single deployment as soon as an event names it.
//
// A failure here is logged and not retried: the sweep is already guaranteed to
// revisit the deployment, so retrying would add a second, redundant path to the
// same outcome.
func (c *ReplicaController) RunSubscriber(ctx context.Context) {
	for ctx.Err() == nil {
		sub, err := c.client.Subscribe(ctx, schema.DeploymentEventsChannel())
		if err != nil {
			c.logger.Error(ctx, "event_subscribe_failed", err.Error())
			// Reconnect rather than give up. Losing the subscriber permanently
			// would silently downgrade the system to sweep-only latency.
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}

		c.consume(ctx, sub)
		_ = sub.Close()
	}
}

// consume drains one subscription until it closes or ctx is cancelled.
func (c *ReplicaController) consume(ctx context.Context, sub *redisclient.Subscription) {
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			ev, err := events.Decode(msg.Payload)
			if err != nil {
				// Malformed payloads are dropped, not retried: replaying an
				// undecodable message cannot succeed on a second attempt.
				c.logger.Warn(ctx, "event_decode_failed", err.Error())
				continue
			}
			delta, err := c.ReconcileDeployment(ctx, ev.Deployment)
			if err != nil {
				if ctx.Err() == nil {
					c.logger.Error(ctx, "event_reconcile_failed", err.Error(),
						logging.DeploymentID(ev.Deployment))
				}
				continue
			}
			c.logDelta(ctx, "event_reconciled", delta)
		}
	}
}

// ReconcileAll reconciles every deployment, then cleans up pods whose
// deployment no longer exists.
func (c *ReplicaController) ReconcileAll(ctx context.Context) ([]Delta, error) {
	keys, err := c.client.ScanKeys(ctx, schema.DeploymentKeyPattern())
	if err != nil {
		return nil, fmt.Errorf("scan deployments: %w", err)
	}

	known := make(map[string]bool, len(keys))
	var deltas []Delta
	for _, key := range keys {
		name := strings.TrimPrefix(key, "deployment:")
		if name == "" || name == key {
			continue
		}
		known[name] = true

		delta, err := c.ReconcileDeployment(ctx, name)
		if err != nil {
			// One bad deployment must not stop the sweep: the remaining
			// deployments are unrelated and still need converging.
			if ctx.Err() != nil {
				return deltas, ctx.Err()
			}
			c.logger.Error(ctx, "reconcile_failed", err.Error(), logging.DeploymentID(name))
			continue
		}
		if delta.Created > 0 || delta.Removed > 0 {
			c.logDelta(ctx, "sweep_reconciled", delta)
		}
		deltas = append(deltas, delta)
	}

	if err := c.reapOrphanPods(ctx, known); err != nil && ctx.Err() == nil {
		c.logger.Error(ctx, "orphan_pod_reap_failed", err.Error())
	}
	return deltas, nil
}

// ReconcileDeployment is the delta engine: the single place a replica count is
// compared against reality and acted on. Both reconciliation modes call it.
func (c *ReplicaController) ReconcileDeployment(ctx context.Context, name string) (Delta, error) {
	if name == "" {
		return Delta{}, errors.New("reconcile: empty deployment name")
	}

	fields, err := c.client.HashGet(ctx, schema.DeploymentKey(name))
	if err != nil {
		return Delta{}, fmt.Errorf("read deployment %s: %w", name, err)
	}
	if len(fields) == 0 {
		// Deleted between the event and this read. Its pods are garbage.
		removed, err := c.removePods(ctx, name, nil)
		return Delta{Deployment: name, Removed: removed}, err
	}

	dep, err := schema.MapToDeployment(fields)
	if err != nil {
		return Delta{}, fmt.Errorf("parse deployment %s: %w", name, err)
	}

	pods, err := c.podsForDeployment(ctx, name)
	if err != nil {
		return Delta{}, err
	}

	// Evicted pods are records of work that no longer exists anywhere: their
	// capacity is already released and their node is gone. They are removed
	// rather than counted, otherwise a dead node's pods would suppress the
	// replacements the deployment needs.
	var live []*schema.PodSpec
	var evicted []string
	for _, pod := range pods {
		if pod.Status == schema.StatusEvicted {
			evicted = append(evicted, pod.PodID)
			continue
		}
		live = append(live, pod)
	}

	delta := Delta{Deployment: name, Desired: dep.DesiredReplicas, Observed: len(live)}

	if len(evicted) > 0 {
		if err := c.deletePodRecords(ctx, name, evicted); err != nil {
			return delta, err
		}
	}

	switch {
	case len(live) < dep.DesiredReplicas:
		created, err := c.sched.ScheduleReplicas(ctx, dep, dep.DesiredReplicas-len(live))
		delta.Created = len(created)
		if err != nil {
			return delta, fmt.Errorf("scale up %s: %w", name, err)
		}
	case len(live) > dep.DesiredReplicas:
		victims := selectVictims(live, len(live)-dep.DesiredReplicas)
		removed, err := c.removePods(ctx, name, victims)
		delta.Removed = removed
		if err != nil {
			return delta, fmt.Errorf("scale down %s: %w", name, err)
		}
	}
	return delta, nil
}

// selectVictims picks which pods to remove when there are too many.
//
// Pods that are not doing useful work go first: an unscheduled or crashing pod
// costs a reservation without serving traffic, so removing it is strictly
// better than removing a healthy one. Among equals the newest goes first,
// because an older pod has demonstrated it can stay up.
func selectVictims(pods []*schema.PodSpec, count int) []*schema.PodSpec {
	ranked := make([]*schema.PodSpec, len(pods))
	copy(ranked, pods)

	sort.SliceStable(ranked, func(i, j int) bool {
		pi, pj := statusPriority(ranked[i].Status), statusPriority(ranked[j].Status)
		if pi != pj {
			return pi < pj
		}
		// Later CreatedAt sorts first. Timestamps are second-granularity
		// strings, so ties are broken by pod ID to keep the order stable across
		// runs rather than dependent on scan order.
		if ranked[i].CreatedAt != ranked[j].CreatedAt {
			return ranked[i].CreatedAt > ranked[j].CreatedAt
		}
		return ranked[i].PodID > ranked[j].PodID
	})

	if count > len(ranked) {
		count = len(ranked)
	}
	return ranked[:count]
}

// statusPriority orders statuses by how little a pod in that status is worth
// keeping. Lower is removed sooner.
func statusPriority(status string) int {
	switch status {
	case schema.StatusUnschedulable:
		return 0
	case schema.StatusFailed:
		return 1
	case schema.StatusCrashLoopBackOff:
		return 2
	case schema.StatusPending:
		return 3
	default: // Running
		return 4
	}
}

// removePods removes the given pods, or every pod of the deployment when
// victims is nil.
//
// Removal goes through the owning node's command queue so the node that holds
// the process is the one that kills it. A pod whose node is not live cannot be
// reached that way, so its record is deleted here and its reservation returned
// directly: waiting for a dead node to acknowledge would strand the capacity
// forever.
func (c *ReplicaController) removePods(ctx context.Context, deployment string, victims []*schema.PodSpec) (int, error) {
	if victims == nil {
		all, err := c.podsForDeployment(ctx, deployment)
		if err != nil {
			return 0, err
		}
		victims = all
	}
	if len(victims) == 0 {
		return 0, nil
	}

	liveNodes, err := c.sched.LiveNodes(ctx)
	if err != nil {
		return 0, fmt.Errorf("list live nodes: %w", err)
	}
	alive := make(map[string]bool, len(liveNodes))
	for _, n := range liveNodes {
		alive[n] = true
	}

	removed := 0
	for _, pod := range victims {
		if alive[pod.NodeID] {
			err := supervisor.SendCommand(ctx, c.client, pod.NodeID, supervisor.Command{
				Type:       supervisor.CommandStopPod,
				Deployment: deployment,
				PodID:      pod.PodID,
			})
			if err != nil {
				if ctx.Err() != nil {
					return removed, ctx.Err()
				}
				c.logger.Warn(ctx, "stop_command_dispatch_failed", err.Error(),
					logging.DeploymentID(deployment), logging.PodID(pod.PodID))
				continue
			}
			removed++
			continue
		}

		if err := c.forceRemove(ctx, pod); err != nil {
			if ctx.Err() != nil {
				return removed, ctx.Err()
			}
			c.logger.Warn(ctx, "force_remove_failed", err.Error(),
				logging.DeploymentID(deployment), logging.PodID(pod.PodID))
			continue
		}
		removed++
	}
	return removed, nil
}

// forceRemove deletes a pod whose node cannot act on a command.
//
// Capacity is released before the record is deleted. A crash in between leaves
// the pod present but over-counted, which the owning agent's startup rebuild
// corrects; the reverse order would lose the record while its reservation stood.
func (c *ReplicaController) forceRemove(ctx context.Context, pod *schema.PodSpec) error {
	if pod.NodeID != "" && schema.ConsumesCapacity(pod.Status) {
		if err := capacity.Release(ctx, c.client, pod.NodeID, pod.CPURequest, pod.MemRequest); err != nil {
			return fmt.Errorf("release capacity for %s: %w", pod.PodID, err)
		}
	}
	if err := c.client.DeleteKey(ctx,
		schema.PodKey(pod.Deployment, pod.PodID),
		schema.TelemetryCPUKey(pod.Deployment, pod.PodID),
	); err != nil {
		return fmt.Errorf("delete pod %s: %w", pod.PodID, err)
	}
	return nil
}

// deletePodRecords drops pod records whose capacity is already released.
func (c *ReplicaController) deletePodRecords(ctx context.Context, deployment string, podIDs []string) error {
	keys := make([]string, 0, len(podIDs)*2)
	for _, id := range podIDs {
		keys = append(keys, schema.PodKey(deployment, id), schema.TelemetryCPUKey(deployment, id))
	}
	if err := c.client.DeleteKey(ctx, keys...); err != nil {
		return fmt.Errorf("delete evicted pod records for %s: %w", deployment, err)
	}
	return nil
}

// reapOrphanPods removes pods whose deployment no longer exists.
//
// Deleting a deployment does not delete its pods atomically, and a delete event
// can be missed entirely, so the sweep re-derives the orphan set from scratch
// instead of trusting that any single deletion path ran to completion.
func (c *ReplicaController) reapOrphanPods(ctx context.Context, known map[string]bool) error {
	keys, err := c.client.ScanKeys(ctx, schema.PodKeyPattern())
	if err != nil {
		return fmt.Errorf("scan pods: %w", err)
	}

	orphansByDeployment := make(map[string][]*schema.PodSpec)
	for _, key := range keys {
		fields, err := c.client.HashGet(ctx, key)
		if err != nil || len(fields) == 0 {
			continue
		}
		pod, err := schema.MapToPod(fields)
		if err != nil {
			continue
		}
		if pod.Deployment == "" || known[pod.Deployment] {
			continue
		}
		orphansByDeployment[pod.Deployment] = append(orphansByDeployment[pod.Deployment], pod)
	}

	for deployment, pods := range orphansByDeployment {
		removed, err := c.removePods(ctx, deployment, pods)
		if err != nil {
			return err
		}
		if removed > 0 {
			c.logger.Info(ctx, "orphan_pods_reaped",
				fmt.Sprintf("removed %d pod(s) of deleted deployment", removed),
				logging.DeploymentID(deployment))
		}
	}
	return nil
}

// podsForDeployment reads every pod record belonging to a deployment.
func (c *ReplicaController) podsForDeployment(ctx context.Context, deployment string) ([]*schema.PodSpec, error) {
	keys, err := c.client.ScanKeys(ctx, schema.DeploymentPodsPattern(deployment))
	if err != nil {
		return nil, fmt.Errorf("scan pods of %s: %w", deployment, err)
	}

	pods := make([]*schema.PodSpec, 0, len(keys))
	for _, key := range keys {
		fields, err := c.client.HashGet(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("read pod %s: %w", key, err)
		}
		if len(fields) == 0 {
			continue // deleted mid-scan
		}
		pod, err := schema.MapToPod(fields)
		if err != nil {
			// A record that cannot be parsed cannot be reasoned about, but it
			// must not abort reconciliation of the pods that are readable.
			c.logger.Warn(ctx, "pod_parse_failed", err.Error(), logging.DeploymentID(deployment))
			continue
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

// logDelta records a reconciliation outcome.
func (c *ReplicaController) logDelta(ctx context.Context, event string, delta Delta) {
	c.logger.Info(ctx, event,
		fmt.Sprintf("desired %d, observed %d, created %d, removed %d",
			delta.Desired, delta.Observed, delta.Created, delta.Removed),
		logging.DeploymentID(delta.Deployment))
}

// sleepCtx waits for d, reporting false if ctx was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
