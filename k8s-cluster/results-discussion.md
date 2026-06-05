# Results & Discussion

## Thesis, in one sentence

For autoscaling a **sharded, stateful** workload (a Redis Cluster), a dedicated Operator is
**necessary but not sufficient**: the native Horizontal Pod Autoscaler (HPA) *cannot do the job
at all*, while the Operator + KEDA approach *can* — but only at the cost of non-trivial
integration and with a real robustness gap under autoscaling churn. The Operator moves stateful
scaling from **impossible** to **feasible-but-operationally-demanding**; it shifts operational
toil rather than eliminating it.

---

## 1. Experimental setup (summary)

- **Cluster:** 6-node k3s (6 mini-PCs, 4 vCPU / 8 GB each), one control-plane + worker.
- **Workload:** a FastAPI "leaderboard" entrypoint (6 replicas, NodePort) backed by Redis Cluster;
  keys spread across many sorted sets (`lb:<region>:<mode>:<date>`) so adding/removing masters
  actually changes load distribution.
- **Load generator:** Grafana k6 on the intermediary VM, fanned out across all 6 NodePorts.
- **Observability:** kube-prometheus-stack (Prometheus + Grafana); a Redis exporter sidecar per
  node feeding `redis_keyspace_*`, `redis_cluster_*`, `redis_db_keys`, etc.
- **Scenario A — Vanilla HPA (Phase 1):** raw `StatefulSet` Redis Cluster, cluster formed by a
  manual `redis-cli --cluster create`, scaled by an HPA on CPU.
- **Scenario B — Operator + KEDA (Phase 3):** Opstree Redis Operator–managed `RedisCluster`,
  scaled by a KEDA `ScaledObject` on the cache-miss ratio.

> Both scenarios run Redis in cluster mode, ephemeral (no PVC), so storage is held constant and
> the independent variable is purely the **scaling mechanism**.

---

## 2. Findings

### 2.1 Scale-OUT

| | Vanilla HPA | Operator + KEDA |
|---|---|---|
| Mechanism | HPA increases `StatefulSet` replicas | KEDA increases `clusterSize`; operator reconciles |
| New pod's role | **Ghost Pod** — joins the Deployment but **not the cluster**: 0 hash slots, ~0 CPU, serves no traffic | **Real master** — operator `CLUSTER MEET`s it and **reshards** slots onto it |
| Effect on load | Original masters stay saturated; HPA keeps scaling toward max while throughput/latency **do not improve** | New master takes slots + traffic; load is actually relieved |

**Claim:** the HPA's actuation variable (pod *count*) is **orthogonal** to a Redis Cluster's real
capacity (*slot ownership*). The HPA can provision a pod but has no concept of hash slots, so the
capacity it adds is inert. Only the Operator closes that gap by resharding.
→ **This is why an Operator is *necessary*.**

**Evidence to cite:**
- Kubernetes pod count vs. `redis-cli cluster nodes` membership (HPA: count > members = the ghost).
- `redis_cluster_slots_assigned` per node; ghost = 0 slots vs. operator's new master with a share.
- Grafana overlay: replica count climbing 3→6 against flat client throughput/latency (HPA).
- `kubectl describe hpa` showing repeated `SuccessfulRescale` chasing a CPU target that never drops.

### 2.2 Scale-IN

| | Vanilla HPA | Operator + KEDA (happy path) |
|---|---|---|
| Mechanism | `StatefulSet` deletes the highest-ordinal pod (reverse-ordinal), **no drain** | Operator **drains** the departing master's slots to survivors, *then* deletes it |
| Result | **Data cliff** — orphaned slots, `CLUSTERDOWN`, lost keys | Slots stay fully covered (16384), no data loss |

**Claim:** the HPA/StatefulSet scale-in mechanism cannot contract a sharded store safely — it
removes a slot-owning master without migrating its data. The Operator performs the
reshard-before-remove that the native primitive structurally cannot.
→ **Reinforces *necessary*.**

**Evidence to cite:**
- `redis_cluster_slots_assigned` dropping below 16384 (HPA) vs. holding at 16384 (operator).
- `redis_cluster_state` → fail (HPA) during the cliff.
- `sum(redis_db_keys{namespace="redis"})` step-down (HPA) vs. flat (operator).

### 2.3 The cost of the Operator — *why it is not sufficient*

The Operator works, but getting it to work was not turnkey. Two categories of cost:

**(a) Integration plumbing (one-time, but non-trivial).** Contrary to the "industry-standard =
clean" assumption, the out-of-the-box pieces did **not** simply compose:
- The `RedisCluster` CRD shipped **without a Kubernetes `/scale` subresource**, so KEDA's managed
  HPA had nothing to actuate. We had to patch the CRD to map `/scale` → `spec.clusterSize`.
- The HPA additionally requires the scale subresource to expose a **label selector**; the
  operator's status had none, so we patched a defaulted `labelSelectorPath`.
- The chart's `serviceMonitor.enabled` flag **did not create a ServiceMonitor** (confirmed by the
  vendor docs: the user must supply one); without it Prometheus scraped nothing and KEDA read 0.
- Result: the "operator = one flag" expectation for monitoring **did not hold** — a standalone
  ServiceMonitor was hand-written, just as in the HPA scenario.

**(b) Runtime fragility under churn (the headline limitation).** The Operator has **no
self-healing** for an interrupted reshard:
- When a slot migration is interrupted (e.g., a rapid scale-out→scale-in, or KEDA *flapping*
  under fluctuating load), the cluster is left with an **open slot** (stuck migrating/importing).
- `redis-cli` then refuses *every* further reshard/rebalance/`del-node` with
  `*** Please fix your cluster problems`, and the operator **loops indefinitely** — it cannot
  repair the open slot itself.
- Recovery requires a **human** to run `redis-cli --cluster fix`. (This is exactly the kind of
  manual operational toil the thesis set out to measure.)

**Claim:** the Operator transforms stateful scaling from *impossible* (HPA) to *feasible*, but it
remains *operationally demanding* — it needs careful integration and is not robust to the very
autoscaling churn it is meant to absorb.
→ **This is why an Operator is *not sufficient* / not turnkey.**

**Evidence to cite:**
- Operator logs: the repeating `Redis cluster is downscaling…` → reshard fails
  (`Please fix your cluster problems`) → `del-node` fails (`Node is not empty`) loop.
- `RedisCluster` status stuck at `state=Bootstrap`, `readyLeaderReplicas=4` while
  `clusterSize=3` — desired ≠ actual, never converging.
- The CRD patches and standalone ServiceMonitor that had to be authored to make scaling work.

---

## 3. Synthesis: *necessary but not sufficient*

- **Necessary.** The HPA literally cannot scale a Redis Cluster's capacity — it has no notion of
  hash slots, so scale-out yields ghosts and scale-in yields a data cliff. No amount of
  configuration fixes this; it is the wrong mechanism. The Operator is the *only* path to
  topology-aware scaling.
- **Not sufficient.** The Operator is not a drop-in "clean button." It demanded substantial
  integration glue (scale subresource, selector, ServiceMonitor, KEDA wiring) and exhibited a
  robustness gap: under churn it can wedge on an un-healable open slot, requiring manual repair.

The contribution is therefore sharper than "use an operator": it **quantifies the residual
operational cost** of the correct tool, and locates the remaining failure mode (interrupted
reshards / no self-healing) that practitioners must design around.

---

## 4. Threats to validity & caveats

- **Some operator complexity was self-inflicted.** KEDA was bolted onto a `RedisCluster` CR that
  this operator *version* did not natively expose for `/scale`; a newer operator (native HPA
  support) would shed some glue. The integration burden is therefore partly version-specific —
  but the *need* for it out-of-the-box is real and was observed.
- **Small node resources** (100m–250m CPU per Redis container) make resharding slow, widening the
  window in which an interruption can occur.
- **Aggressive autoscaling config.** The KEDA trigger uses `metricType: Value` with a low
  threshold (0.05, below the realized ~0.10 miss ratio), which scales aggressively and **flaps** —
  this churn is what interrupts reshards. A scale-down `cooldownPeriod` / stabilization window
  would reduce, though not eliminate, the fragility.
- **Ephemeral storage** in both scenarios; persistence (and its own scale-in semantics) is out of
  scope.
- The scale-in experiments were driven deterministically (KEDA `paused-replicas`) for
  reproducibility, rather than waiting on the live metric.

---

## 5. Practical recommendations

1. **Do not use HPA to scale a sharded stateful service.** It is structurally incapable; the
   ghost pod and data cliff are unavoidable.
2. **Use a topology-aware Operator** — but budget for integration work and verify the CRD exposes
   `/scale` (with a selector) for KEDA/HPA before relying on autoscaling.
3. **Tune the autoscaler conservatively** (cooldown / stabilization) to avoid interrupting
   reshards; treat scale-**in** as a higher-risk operation than scale-**out**.
4. **Have a repair runbook.** Even with the operator, an interrupted reshard needs manual
   `redis-cli --cluster fix`; this should be an expected, documented operation, not a surprise.

---

## 6. Evidence index (artifact → claim)

| Artifact | Supports |
|---|---|
| pod count vs. `cluster nodes`; per-node `slots_assigned` | Ghost pod (HPA scale-out) |
| Grafana: replicas vs. throughput/latency (flat) | HPA capacity is inert |
| `slots_assigned` / `cluster_state` / total keys panels | Data cliff (HPA scale-in) |
| `redis-cli --cluster check` after operator scale-out | Operator reshards onto the new master |
| `slots_assigned` flat at 16384 across operator scale-in | Operator drains gracefully (no cliff) |
| Operator logs: `Please fix your cluster problems` loop; `state=Bootstrap` stuck | No self-healing / robustness gap |
| CRD `/scale` + selector patches; standalone ServiceMonitor | Integration is not turnkey |
