# Operator process tree

PremiScale supports three deployment compositions, selected with
`--deployment-mode`, `PREMISCALE_DEPLOYMENT_MODE`, or the chart's `mode` value.
This is separate from `controller.mode`, which selects Kubernetes or standalone
infrastructure behavior. HA compositions require `controller.mode: kubernetes`.

| Composition | PremiScale containers | Metrics transport | Controller ownership |
| --- | --- | --- | --- |
| `singular` | One container containing collection, publication, and controller services | Dragonfly streams by default; Kafka is optional | One instance, with a local SQLite journal by default |
| `ha` | Replicated identical containers; every replica collects and publishes | Kafka topic, one consumer group per database | One elected controller; PostgreSQL journals |
| `hha` | Elected controller replicas, a collector Deployment, and a Deployment per selected database publisher | Kafka topic, one consumer group per database | One elected controller; PostgreSQL journals |

The same Docker image runs every role. Dragonfly, Kafka, PostgreSQL, and the
upstream Kubernetes Cluster Autoscaler run outside the PremiScale container.
The optional Strimzi dependency manages persistent Kafka pods through
Kafka/KafkaNodePool resources; these are not stateless worker Deployments.

## Singular

```text
tini (PID 1 in the container)
└── premiscale: parent supervisor
    ├── api: Flask/Werkzeug (:9090 in Kubernetes mode)
    ├── platform: optional registration and websocket client
    ├── autoscaling: action-stream consumer
    ├── reconciliation
    │   ├── collectors: process-pool supervisor
    │   │   └── collector-0 … collector-(C-1)
    │   │       └── host-connection threads: bounded PID-controlled pool
    │   └── publishers: process-pool supervisor
    │       ├── publisher-state: one observed-state writer
    │       ├── publisher-primary-0: one TinyFlux writer
    │       └── publisher-<database>-0 … publisher-<database>-(P-1)
    ├── kubernetes: Python external-cloud-provider backend
    │   ├── gRPC request threads on a private Unix socket
    │   ├── autoscaler-vms thread: durable VM lifecycle operations
    │   └── premiscale-autoscaler: Go gRPC frontend (:50051)
    └── operator: Kopf event loop, config projection/ASG status timers, /healthz (:8085)
```

`C` respects available CPUs, affinity, and container CPU quotas. Singular
collectors receive disjoint host partitions. Their PID controllers adjust
thread counts between completed passes. Empty partitions idle without opening
host or database connections. `P` defaults to two PostgreSQL connections per
subscriber, configurable with `--publisher-connections`; local-file destinations
always get one writer. Collection and publication have separate subprocesses,
clients, queues, and failure handling.

In standalone mode, reconciliation also runs infrastructure decision passes;
the Kubernetes provider and Kopf children are absent, and Flask serves health
on the configured health port. External-metrics modes omit collectors and
publishers. `standalone-external-metrics` still runs decision reconciliation;
`kubernetes-external-metrics` omits reconciliation entirely.

## Consolidated HA

```text
each replicated PremiScale container
└── premiscale
    ├── api: /healthz, /ready, /metrics, /scaling/* (:9090)
    ├── reconciliation: work active on every replica
    │   ├── collectors
    │   │   └── C collector processes → PID-controlled host-connection threads
    │   └── publishers
    │       └── P processes per selected remote database subscriber
    └── leadership: Kubernetes Lease candidate
        └── only while elected and holding the PostgreSQL ownership lock
            ├── platform (optional)
            ├── autoscaling
            ├── reconciliation
            │   ├── collection-scheduler: enqueue due hosts once
            │   └── publishers → state / primary / remaining subscribers
            ├── kubernetes → VM thread, Python gRPC threads, Go frontend
            └── operator: Kopf config projection/status handlers and health endpoint
```

Every replica does useful collection and publication work, including standbys.
The elected scheduler uses Dragonfly to keep at most one unfinished request per
host and maintain the configured collection interval. Requests contain host
names; consumers resolve credentials locally. Successful collection publishes
one UUID-bearing metrics batch to Kafka before acknowledging the host request.

A single KEDA ScaledObject targets the consolidated Deployment. It contains
both collection signals and each selected database's publication signals:
active outbound operations and outstanding work. The HPA takes the largest
replica recommendation across these metrics, rather than adding them together.
See the [Kubernetes HPA algorithm](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale-walkthrough/).
The minimum is two replicas by default and cannot be zero: an elected scheduler
and the metrics API must remain available.

## Horizontally split HA

```text
controller Deployment (two candidates by default)
└── each container: premiscale
    ├── api: aggregate metrics and process health
    └── leadership → elected controller subtree shown above

collector Deployment (KEDA scales this independently)
└── each container: premiscale --role collector
    ├── api
    └── reconciliation → collectors → C processes → bounded thread pools

publisher Deployment (one per selected database; KEDA scales independently)
└── each container: premiscale --role publisher --publisher <name>
    ├── api
    └── reconciliation → publishers → P database-connection processes

Kafka cluster (external or managed by the optional Strimzi operator)
└── persistent broker/controller pods, metrics topic, dead-letter topic
```

Both HA compositions use the same collector, publisher, and Kafka code.
KEDA reads `/scaling/collectors` and `/scaling/publishers/<name>` through an
internal Service selecting all controller candidates. Host backlog includes
leased Dragonfly requests. Publication backlog is Kafka's retained log end
minus the subscriber's committed offsets, so database writes in progress still
count. Active-operation observations expire after a crashed process stops
refreshing them. Idle PostgreSQL connections count toward the connection cap,
but not active demand. Broker errors return HTTP 503, preventing a false zero.

A subscriber's Kafka group is `<groupPrefix>.<subscriber>`. Every destination
reads the complete metrics topic, while its replicas divide the partitions.
The publisher commits an offset only after the database adapter returns
successfully. Failed writes leave the offset unchanged. Message UUIDs support
PostgreSQL deduplication on retry. Malformed records go to `<topic>.dead` before
that subscriber commits past them. This is at-least-once delivery; the Kafka
retention period bounds recovery after a prolonged outage. Partition count
bounds useful publishing concurrency per subscriber.

## Election, journals, and provider routing

The `leadership` process elects through a namespaced Kubernetes Lease, renews
about every two seconds, and stops its subtree after a ten-second local renewal
deadline. Contenders observe an unchanged foreign Lease for its 45-second
lifetime before taking over; clock comparisons use local monotonic observation
time. Updates use Kubernetes resource versions. Each candidate process has a
fresh UUID even when its pod restarts.

Before starting services, the winner acquires a PostgreSQL session advisory
lock. The provider has a separate ownership lock. Both locks use the stable
cluster identity; connections are never silently re-established after loss.
VM operations and exact libvirt volume ownership reside in the same
cluster-specific PostgreSQL schema. A successor reopens these journals and
resumes pending operations. `stateDsn` overrides `stateFile`, so HA does not open
a local provider SQLite database. Existing SQLite journals are not automatically
copied into PostgreSQL: migrate existing operation and volume records while the
old controller is stopped before enabling HA for an already managed cluster.

The leader advertises its pod label only after its required children and
provider observations are ready. The provider Service selects that label;
the scaling Service selects all candidates. During shutdown, routing is removed
first, the child tree is stopped, and only then are PostgreSQL ownership and the
Kubernetes Lease released. API or database outages fail closed; an unreachable
API can leave a stale routing label until Kubernetes removes or restarts the
old pod, causing transient connection failures during failover.

Kopf remains in standalone peering mode *inside the elected subtree*.
[Kopf peering](https://docs.kopf.dev/en/latest/peering/) only pauses Kopf handlers;
it cannot fence sibling VM/provider processes. Supervising the complete subtree
under one election also protects those writers. As with
[Kubernetes client-go leader election](https://pkg.go.dev/k8s.io/client-go/tools/leaderelection),
a Lease is not a hypervisor fencing mechanism. A remote libvirt operation already
accepted before a process or network failure can finish afterwards; provider
ownership marks, durable intent, and idempotent recovery remain necessary.

## Health and shutdown

The parent writes atomic PID and heartbeat snapshots into a private directory
under `controller.healthcheck.stateDirectory`. Pool supervisors own nested
status directories; each child records initialization there. Flask and Kopf
read cached snapshots. Standby controller pods remain ready for metrics;
leadership heartbeat expiry fails their health checks. The elected operator
publishes AutoscalingGroup status from fresh, committed provider observations.

Top-level services have separate POSIX process groups. Descendants stay in
their top-level group's session, so abrupt supervisor failure does not orphan
collectors, publishers, or the Go frontend. SIGINT/SIGTERM asks for cleanup;
the parent allows ten seconds before forcing remaining groups to stop and
five more seconds to reap them. Nested pools use shorter deadlines. Kubernetes
allows thirty seconds by default. Unexpected required-child exits fail the
container; declined platform registration is optional. There is no
multiprocessing manager or shared-process queue. Python `spawn` may additionally
launch its own resource-tracker helper.

## Source map

- [Process selection](processes/__init__.py) and [deployment settings](settings.py).
- [Parent runtime](runtime.py) and [process-group supervisor](supervisor.py).
- [Reconciliation](../reconciliation/runtime.py), [collectors](../reconciliation/collectors/), and [publishers](../reconciliation/publishers/).
- [Leadership lifecycle](../operator/leadership.py) and [Lease client](../operator/election.py).
- [Kafka transport](../messaging/kafka.py) and [shared journals](../connections/journal.py).

## Configuration and rollout ownership

The chart projects either structured `config` values or a selected ControllerConfig and its referenced Host/AutoscalingGroup CRs into one ConfigMap. All roles read their mounted file at startup. In CRD mode, only the elected Kopf subtree reconciles that file; it validates changes and retains the previous configuration on invalid input. An installed Stakater Reloader restarts every affected Deployment after a valid ConfigMap change. Helm upgrades also trigger restarts through a checksum.

HA controller candidates require distinct nodes; HHA collector and publisher pools spread independently across nodes and zones. Each Deployment has a PDB for evictions and separate rolling-update limits. Those controls preserve available replicas, while the existing Lease and PostgreSQL ownership locks guard the single controller during replacement. They do not eliminate the brief election handoff or singular mode's planned restart downtime. See [chart configuration, placement, and rollout settings](../../../charts/premiscale/README.md#configuration-reloads-placement-and-disruption).

Collection now verifies managed VM ownership, requests raw counters, closes the host connection, and compiles relevant observations/metrics. The publisher supervisor also owns one internal `_state` subscriber in singular mode or the elected controller subtree. Collector processes no longer open state databases. InfluxDB subscribers can use the same remote publisher pools as PostgreSQL. See the [collection pipeline and state revision semantics](../metrics/README.md).
