## Install PremiScale

Paraphrased, this should look like...

On your hosts, create a new user with XXX permissions and generate a new set of RSA keys for use with SSH (if that's your method of authentication with libvirt or the hosts, qemu+ssh:// e.g.) or, alternatively, we set up connectivity over TLS.

## Runtime configuration and CRDs

`config` contains the complete runtime YAML mapping: `version` and `controller`. All configuration fields, including backend settings, host inventories, ASGs, connection limits, reconciliation PID tuning, Kafka, and health listeners, are validated against the schema generated from the configuration CRDs. Deployment settings remain in chart values. Choose one composition per namespace/cluster identity: `singular`, consolidated `ha`, or split `hha`.

By default, Helm projects `config` into the enabled ConfigMap's `data.config.yaml`. Every controller, collector, and publisher mounts that file at `controller.config.mountPath`; its rendered checksum triggers rollouts on Helm upgrades. `configMap.name` overrides the shared name. For example:

```yaml
config:
  controller:
    databases:
      maxHostConnectionThreads: 16
      collectionInterval: 30
    reconciliation:
      collection:
        minThreads: 2
        initialThreads: 4
```

The complete mapping is described by [`values.schema.json`](values.schema.json) and the [runtime configuration documentation](../../pkg/premiscale/config/docs/README.md). Helm merges mappings with defaults; set an unwanted default to `null` when changing to a backend with different fields. Existing full-file configurations remain supported through `--set-file configMap.config=path/to/controller.yaml`, which takes precedence over `config`.

For Kubernetes-managed configuration, set `configMap.source: crds` and `configMap.controllerConfig` to a key in `premiscale-crds.controllerConfigs`. Helm resolves that ControllerConfig and its referenced Host/AutoscalingGroup resources into the initial ConfigMap. All referenced resources must share the operator namespace, and every ASG host must belong to the ControllerConfig inventory. Creating unrelated resources does not change a running controller.

```shell
helm upgrade --install premiscale ./charts/premiscale \
  --namespace premiscale --create-namespace -f .config/crds/values.yaml
```

Use [the CRD example](../../.config/crds/values.yaml) as a starting point; [the CRD chart](../premiscale-crds/README.md) describes creating multiple hosts and groups. In CRD mode, the elected Kopf process rechecks the selected ControllerConfig and its references every five seconds, validates the resulting runtime configuration, and patches the ConfigMap only when its content changes. Invalid or missing references retain the last valid file. ControllerConfig status reports its ConfigMap, configuration digest, observed generation, and Ready condition. Ready confirms projection; Deployment readiness confirms the rollout.

Live projection permits runtime tuning and host/ASG updates. Listener ports, runtime-state mount paths, cluster identity, journal location, and broker/Kafka destinations require a coordinated Helm upgrade because Services, mounts, routing, and workload ownership also depend on them. Selecting different distributed publishers likewise requires chart changes. Only the elected operator writes the projected ConfigMap, with a resource-version precondition; its Role limits writes to that named ConfigMap. The ConfigMap must remain mutable. Direct edits to a CRD-backed ConfigMap are reconciled back to its selected CRs.

Keep passwords, private keys, and tokens in Secrets. Use environment references in `config` or CR specifications and inject them through `controller.extraEnv` (`autoscaling.workers.extraEnv` can add worker-only credentials). Projection retains the references and never writes their expanded values. ConfigMaps and CRs are not secret storage.

## Configuration reloads, placement, and disruption

Install [Stakater Reloader](https://docs.stakater.com/reloader/1.4/reference/annotations.html) in the cluster to trigger rollouts after live ConfigMap or Secret changes. This chart supplies its annotations on every PremiScale Deployment; it does not install Reloader. `reload.enabled` controls ConfigMap reload annotations and `reload.secrets` controls automatic reloads for referenced Secrets. Helm checksum rollouts work independently. The file is mounted with `subPath` and each process reads it at startup, so a new pod is required to apply changes.

HA controllers require distinct nodes through pod anti-affinity and spread across hostnames with `maxSkew: 1`; zone spreading is preferred where zone labels exist. There must be at least as many eligible nodes as desired controller replicas. `scheduling.controller` exposes node selectors, tolerations, affinity, and topology rules. Singular mode retains one pod.

In HHA, collectors and each database publisher spread independently across nodes and zones, with additional preferred anti-affinity across all worker pools to reduce shared network contention. Their default minimum is two replicas. Use `scheduling.collectors.nodeSelector` for nodes with suitable hypervisor network access and `scheduling.publishers.nodeSelector` for database-facing placement; common settings live under `scheduling.workers`. A collector or named publisher may also override `scheduling` and `resources` under `autoscaling.collectors` or `autoscaling.publishers.<name>`. Spreading is best effort for workers, so limited capacity does not stop throughput entirely.

Each controller, collector, and publisher Deployment has its own PodDisruptionBudget. Defaults retain one controller and permit one unavailable pod in each worker pool. Unhealthy pods may be evicted. Singular's `minAvailable: 1` intentionally prevents voluntary drains; disable its budget for planned single-pod maintenance. Budget settings are under `disruptionBudgets`.

[PodDisruptionBudgets](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/) constrain voluntary evictions; they do not constrain Deployment rollouts or HPA replica changes. `rollouts.controller` allows one unavailable controller and no surge, allowing updates with exactly two eligible nodes while election and PostgreSQL locks guard ownership. `rollouts.workers` allows one surge pod and no unavailable replicas, with ten seconds of stable readiness. Reserve extra database connection capacity for surge and terminating publisher pods. Singular uses `Recreate` to protect its SQLite journal, so reconfiguration has downtime. Existing graceful shutdown and durable queue acknowledgments apply during all rollouts; active controller replacement can briefly pause VM operations while leadership transfers.

## Operator health and shared state

In singular Kubernetes mode, Kopf serves `/healthz` on port 8085 (`controller.healthcheck.port`). Flask serves `/ready` and `/metrics` on port 9090 (`controller.healthcheck.apiPort`). Readiness requires initialized required processes and a fresh provider snapshot; it returns 503 when unavailable. Kopf liveness fails when process supervision is unavailable. Standalone modes retain all three paths on the original healthcheck port without requiring Kubernetes credentials.

The parent creates a private directory for each run beneath `/run/premiscale`, supplied by an `emptyDir` volume and `PREMISCALE_STATE_DIRECTORY`. Children publish atomic mode-0600 JSON files with one writer per source. Reads cache data for at most one second, and parent/provider observations expire after 15 seconds. Kopf additionally caches successful probe responses for up to ten seconds. No management process, shared Python queue, credentials, or raw hypervisor errors are used for health state. The provider journal uses a persistent SQLite volume in singular mode and shared PostgreSQL in HA modes. The chart mounts the private status `emptyDir` at `config.controller.healthcheck.stateDirectory`.

`serviceAccount.create` and `rbac.create` default to true. For an externally managed service account, set its name and provide equivalent permissions before disabling chart-managed RBAC. Kopf uses fixed namespace discovery and annotation-based bookkeeping. In values mode, ASG status is published for resources labelled `premiscale.com/cluster` with the configured cluster name. In CRD mode, it covers the selected configuration’s groups. Differences from the running configuration are reported until rollout completes.

## Dragonfly work queues

The chart installs the official [DragonflyDB Helm chart](https://www.dragonflydb.io/docs/getting-started/kubernetes), pinned to v1.40.0, with `dragonfly.enabled=true`. An empty `controller.broker.url` discovers the dependency's actual Service name, namespace, and port, including name overrides. For example, release `premiscale` in namespace `premiscale` uses `redis://premiscale-dragonfly.premiscale.svc:6379/0`. Enabling the dependency's TLS changes the discovered URL to `rediss://`; configure the controller's CA file for private certificates.

For an external broker, set `dragonfly.enabled=false` and supply `controller.broker.url`. An empty URL with the dependency disabled retains the external default `redis://dragonfly:6379/0`. To keep credentials out of values, set `controller.broker.existingSecret` to a Secret containing the complete connection URL under `controller.broker.secretKey` (default `url`). The Secret takes precedence over both explicit and discovered URLs. Bundled broker authentication can use the dependency's `dragonfly.passwordFromSecret` settings alongside that controller URL Secret.

The chart injects the URL as `PREMISCALE_REDIS_URL` and scopes queues to the release namespace and name, unless `controller.broker.namespace` is set explicitly. These environment settings supply defaults when the corresponding controller YAML fields are omitted; explicit YAML broker settings take precedence.

The bundled broker uses one StatefulSet replica, a 1 GiB PVC, a 256 MB data limit, eviction disabled, and snapshots every minute under `/data`. The default storage class is used unless `dragonfly.storage.storageClassName` is set. Service and PVC names include the dependency name to avoid colliding with the controller. Snapshots can lose writes since the last completed snapshot after a broker crash; tune persistence and resources for the required durability. The chart restricts `dragonfly.replicaCount` to one because multiple independent writable replicas do not form a replicated queue. Queue delivery is at least once, and pending work survives controller restarts while retained by the broker. Singular mode requires persistent SQLite journal storage. HA modes use shared PostgreSQL journals and an elected controller. All compositions retain a 30-second container termination grace period.

The default enabled ConfigMap mounts the projected controller YAML at `controller.config.mountPath`. Set its healthcheck host to `0.0.0.0` for Kubernetes probes. The default command runs the production image; for a development image, use `deployment.command: [/usr/bin/tini, --, poetry, run, premiscale]`.

Mount provider storage using `deployment.extraVolumes` and `deployment.extraVolumeMounts`, and point `controller.kubernetes.stateFile` in the controller YAML at that mount. The default `deployment.strategy.type` is `Recreate`, preventing old and new pods from opening the same journal during upgrades. The [Minikube addon](../../integrations/minikube/README.md) supplies a local PVC and a complete development configuration. When creating a registration Secret with this chart, `controller.registration.value` is the plain token; Kubernetes handles its encoding.

## Scale collection and database publishing with KEDA

Set `mode` to select the composition:

| Mode | Placement | Scaling |
| --- | --- | --- |
| `singular` (default) | One container; reconciliation owns collector and publisher process pools | One replica; KEDA disabled |
| `ha` | Each replica contains collection, publication, and a leader candidate | One ScaledObject uses collection **and** per-database demand |
| `hha` | Elected controllers plus separate collector and per-database publisher Deployments | Collectors and each publisher scale independently |

Both HA modes require Kubernetes-mode runtime configuration with `config.controller.kafka.enabled`, broker addresses, and `config.controller.kubernetes.stateDsn`. A namespaced Lease elects one controller; PostgreSQL ownership locks guard the VM and volume journals. The elected subtree contains Kopf, the VM provider, scheduling, and local publishers. The provider Service selects the ready leader; the internal scaling Service reaches all candidates. HA probes use Flask's metrics port on every pod, including standbys.

Kafka decouples collection from publication. Each database has its own consumer group and acknowledges records after committed writes. Active operations still come from expiring Dragonfly observations; publication backlog comes from uncommitted Kafka offsets. KEDA gets both signals through `/scaling/collectors` and `/scaling/publishers/<name>`. Multiple triggers choose the largest replica recommendation. A broker failure returns 503 instead of zero demand.

`autoscaling.publishers` selects matching PostgreSQL or InfluxDB subscribers. Each `connectionsPerReplica` caps that subscriber's database processes per pod. For InfluxDB, these are HTTP publisher sessions. In `ha`, the database connection budget is `autoscaling.controller.maxReplicaCount × connectionsPerReplica`; in `hha`, use that publisher's maximum replicas instead. Terminating pods can briefly overlap replacements, so reserve database headroom or enforce a server-side limit. Kafka partitions bound active consumers per subscriber. Primary/local metrics are ephemeral in HA; publish durable metrics to PostgreSQL. Existing SQLite journals need an offline migration before enabling HA on a managed cluster.

The optional Strimzi dependency and `kafka.enabled` create a three-node persistent Kafka cluster, a 16-partition metrics topic, and a dead-letter topic, with seven-day retention. Disable both for an external cluster and configure bootstrap addresses plus TLS/SASL properties under `controller.kafka.options`; environment references can supply Secret values. Internal managed Kafka uses a plaintext cluster-local listener. Its resources require the Strimzi CRDs; install the dependency or an existing compatible operator first.

Create the `premiscale-postgresql` Secret with `dsn` (metrics) and `journal-dsn` (controller journals), then configure your host inventory and credentials. The PostgreSQL database must already exist, for example as a CloudNativePG Cluster. The journal role needs schema/table creation and read/write privileges.

```shell
# Separate HHA Deployments and managed Kafka.
helm upgrade --install premiscale ./charts/premiscale \
  --namespace premiscale --create-namespace \
  -f .config/keda/values.yaml

# Apply this additional values file for consolidated HA containers.
# -f .config/keda/ha-values.yaml
```

The [process-tree documentation](../../pkg/premiscale/daemon/README.md) describes process/thread ownership, failover, storage, and shutdown in every mode. `.config/keda/minikube-values.yaml` applies the same HHA setup through the Minikube wrapper; a local test cluster needs enough capacity for Kafka, PostgreSQL, KEDA, and the controller candidates.

The optional [InfluxDB overlay](../../.config/influxdb/values.yaml) configures publication to an existing InfluxDB 2.x bucket for Grafana. Merge its Secret environment entries when combining values files. [Collection and publication](../../pkg/premiscale/metrics/README.md) describes managed-VM filtering, state revisions, metric units, and delivery guarantees.

## Optional dependencies

The lockfile pins cluster-autoscaler 9.59.0 (application 1.35.0) and CloudNativePG 0.29.0 (operator 1.30.0). Both are disabled by default. Chart values use the dependency names `cluster-autoscaler` and `cloudnative-pg`.

Before linting or rendering the chart, build the locked dependencies:

```shell
helm repo add autoscaler https://kubernetes.github.io/autoscaler
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo add kedacore https://kedacore.github.io/charts
helm dependency build charts/premiscale
```

Helm fetches Dragonfly directly from `oci://ghcr.io/dragonflydb/dragonfly/helm`; it requires no `helm repo add` entry.

Enable CloudNativePG with `cloudnative-pg.enabled=true` only when the cluster does not already have an operator. It watches the release namespace by default, requests 100m CPU and 128Mi memory, and has a 512Mi memory limit. Prometheus PodMonitor creation is disabled. This installs the operator; create PostgreSQL Cluster resources separately and configure a named PostgreSQL metrics publisher. The primary time-series store remains local.

Before setting `cluster-autoscaler.enabled=true`, configure its `cloudProvider` and node-group discovery for your infrastructure. Match its image's Kubernetes minor version to your cluster. Defaults use one replica, leader election, the least-waste expander, and protections for nodes with system pods or local storage. It requests 100m CPU and 256Mi memory, with a 512Mi memory limit.

The chart requires Kubernetes 1.29 or newer. An empty controller image tag falls back to the chart's application version.

## Parameters

### Deployment composition

| Name   | Description                                                                                                                                         | Value      |
| ------ | --------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `mode` | Composition: singular (one container), ha (replicated consolidated containers), or hha (separate controller, collector, and publisher Deployments). | `singular` |

### Runtime configuration

| Name     | Description                                                                     | Value |
| -------- | ------------------------------------------------------------------------------- | ----- |
| `config` | Complete typed runtime configuration; validated against the CRD-derived schema. | `{}`  |

### Configuration reloads

| Name             | Description                                                             | Value  |
| ---------------- | ----------------------------------------------------------------------- | ------ |
| `reload.enabled` | Annotate all PremiScale Deployments for an installed Stakater Reloader. | `true` |
| `reload.secrets` | Also reload when Secrets referenced by each pod change.                 | `true` |

### Rollout protection

| Name                           | Description                                                                                                   | Value |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------- | ----- |
| `rollouts.controller`          | HA controller rolling-update bounds and readiness stabilization; singular always uses Recreate.               | `{}`  |
| `rollouts.workers`             | Worker rolling-update bounds and readiness stabilization.                                                     | `{}`  |
| `disruptionBudgets.controller` | Keep an available controller during voluntary evictions; singular maintenance requires disabling this budget. | `{}`  |
| `disruptionBudgets.collectors` | Limit voluntary host-collector disruption independently of publishers.                                        | `{}`  |
| `disruptionBudgets.publishers` | Apply a separate disruption budget to each database publisher Deployment.                                     | `{}`  |

### Pod placement

| Name                    | Description                                                                                                                                  | Value |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| `scheduling.controller` | HA controllers require separate nodes; spread across nodes and prefer separate zones. Extra affinity is merged with generated anti-affinity. | `{}`  |
| `scheduling.workers`    | Common worker placement; spread each pool and prefer nodes with fewer PremiScale workers overall.                                            | `{}`  |
| `scheduling.collectors` | Host-collection placement overrides, such as NIC-oriented node labels.                                                                       | `{}`  |
| `scheduling.publishers` | Database-publication placement overrides.                                                                                                    | `{}`  |

### Kafka

| Name                             | Description                                                                                          | Value              |
| -------------------------------- | ---------------------------------------------------------------------------------------------------- | ------------------ |
| `strimzi-kafka-operator.enabled` | Install Strimzi 1.2.0 when the cluster does not already have the operator.                           | `false`            |
| `kafka.enabled`                  | Create a Strimzi Kafka cluster and metrics/dead-letter topics. HA modes also support external Kafka. | `false`            |
| `kafka.name`                     | Kafka cluster name, used by bootstrap Service discovery.                                             | `premiscale-kafka` |
| `kafka.version`                  | Kafka version supported by the pinned Strimzi operator.                                              | `4.3.1`            |
| `kafka.replicas`                 | Broker/controller nodes; three provide the normal quorum.                                            | `3`                |
| `kafka.storageSize`              | Persistent storage per Kafka node.                                                                   | `10Gi`             |
| `kafka.storageClass`             | Optional storage class for Kafka PVCs.                                                               | `""`               |
| `kafka.partitions`               | Metrics topic partitions; this bounds active publication consumers per destination.                  | `16`               |
| `kafka.retentionMilliseconds`    | Retain metrics for replay and independent database recovery.                                         | `604800000`        |
| `kafka.resources`                | Resources per Kafka broker/controller node.                                                          | `{}`               |

### Global Configuration

| Name                       | Description                                                       | Value       |
| -------------------------- | ----------------------------------------------------------------- | ----------- |
| `global.image.registry`    | The global docker registry for all of the image.                  | `docker.io` |
| `global.image.pullSecrets` | Container registry pull secrets applied to every container image. | `[]`        |

### Controller Deployment

| Name                                       | Description                                                                         | Value                   |
| ------------------------------------------ | ----------------------------------------------------------------------------------- | ----------------------- |
| `deployment.command`                       | Container command; development images can prepend poetry run before premiscale.     | `[]`                    |
| `deployment.terminationGracePeriodSeconds` | Allow the controller's 10-second cleanup and 5-second forced shutdown to finish.    | `30`                    |
| `deployment.strategy.type`                 | Deployment update strategy; Recreate prevents overlapping provider journal writers. | `Recreate`              |
| `deployment.extraVolumes`                  | Additional volumes, including persistent provider storage.                          | `[]`                    |
| `deployment.extraVolumeMounts`             | Additional mounts in the controller container.                                      | `[]`                    |
| `deployment.image.name`                    | The name of the controller image.                                                   | `premiscale/premiscale` |
| `deployment.image.tag`                     | The tag of the controller image.                                                    | `0.0.1`                 |
| `deployment.image.pullPolicy`              | The pull policy of the controller image.                                            | `Always`                |
| `deployment.image.pullSecrets`             | Container registry pull secrets that only pertain to this container image.          | `[]`                    |
| `deployment.resources.requests.cpu`        | The CPU request for the controller container.                                       | `0.5`                   |
| `deployment.resources.requests.memory`     | The memory request for the controller container.                                    | `1Gi`                   |
| `deployment.resources.limits.cpu`          | The CPU limit for the controller container.                                         | `4.0`                   |
| `deployment.resources.limits.memory`       | The memory limit for the controller container.                                      | `2Gi`                   |
| `deployment.podSecurityContext`            | Configure the controller pod's security context.                                    | `{}`                    |
| `deployment.containerSecurityContext`      | Configure the controller container's security context.                              | `{}`                    |
| `deployment.annotations`                   | Annotations to be added to the deployment.                                          | `{}`                    |
| `deployment.labels`                        | Labels to be added to the deployment.                                               | `{}`                    |
| `deployment.startupProbe`                  | Configure the deployment's startup probe.                                           | `{}`                    |
| `deployment.startupProbe.enabled`          | Enable or disable the startup probe.                                                | `true`                  |
| `deployment.startupProbe.path`             | The startup probe endpoint's path.                                                  | `/healthz`              |
| `deployment.startupProbe.port`             | The startup probe endpoint's port.                                                  | `healthcheck`           |
| `deployment.startupProbe.config`           | Additional configuration for the startup probe.                                     | `{}`                    |
| `deployment.livenessProbe`                 | Configure the deployment's liveness probe.                                          | `{}`                    |
| `deployment.livenessProbe.enabled`         | Enable or disable the liveness probe.                                               | `true`                  |
| `deployment.livenessProbe.path`            | The liveness probe endpoint's path.                                                 | `/healthz`              |
| `deployment.livenessProbe.port`            | The liveness probe endpoint's port.                                                 | `healthcheck`           |
| `deployment.livenessProbe.config`          | Additional configuration for the liveness probe.                                    | `{}`                    |
| `deployment.readinessProbe`                | Configure the deployment's readiness probe.                                         | `{}`                    |
| `deployment.readinessProbe.enabled`        | Enable or disable the readiness probe.                                              | `true`                  |
| `deployment.readinessProbe.path`           | The readiness probe endpoint's path.                                                | `/ready`                |
| `deployment.readinessProbe.port`           | The Flask readiness endpoint's port.                                                | `metrics`               |
| `deployment.readinessProbe.config`         | Additional configuration for the readiness probe.                                   | `{}`                    |
| `deployment.extraEnv`                      | Extra environment variables to be passed to the controller container.               | `[]`                    |
| `deployment.extraPorts`                    | Extra ports to be exposed on the controller container.                              | `[]`                    |

### PremiScale Controller

| Name                                    | Description                                                                                                                                                                                                          | Value                           |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| `controller.publisherConnections`       | Default PostgreSQL publication processes per destination in consolidated containers.                                                                                                                                 | `2`                             |
| `controller.broker.url`                 | Broker URL override; empty discovers the bundled Dragonfly Service, or dragonfly:6379 when the dependency is disabled. Put credentials in an existing Secret.                                                        | `""`                            |
| `controller.broker.existingSecret`      | Secret containing the complete redis:// or rediss:// URL. Overrides broker.url.                                                                                                                                      | `""`                            |
| `controller.broker.secretKey`           | Key containing the broker URL in the existing Secret.                                                                                                                                                                | `""`                            |
| `controller.broker.namespace`           | Shared queue namespace. Empty uses the release namespace and name.                                                                                                                                                   | `""`                            |
| `controller.registration.createSecret`  | If true, the controller will create a secret with the registration token. If false, the secret must already exist.                                                                                                   | `false`                         |
| `controller.registration.secretName`    | The name of the secret that contains the registration token. If createSecret is true, the controller will create this secret. If createSecret is false, the controller will use this secret and expects it to exist. | `premiscale-registration-token` |
| `controller.registration.key`           | The key in the secret that contains the registration token.                                                                                                                                                          | `token`                         |
| `controller.registration.value`         | User-provided platform registration key.                                                                                                                                                                             | `""`                            |
| `controller.registration.immutable`     | If true, the registration secret cannot be updated. If false, the registration secret can be updated.                                                                                                                | `true`                          |
| `controller.config.mountPath`           | The path where the controller config file is mounted.                                                                                                                                                                | `/opt/premiscale/config.yaml`   |
| `controller.logging.level`              | Can be one of info|debug|warn|error.                                                                                                                                                                                 | `info`                          |
| `controller.extraEnv`                   | Extra environment variables to be passed to the controller container. These are useful for injecting and referencing environment variables in the config that's read from the ConfigMap below.                       | `[]`                            |
| `controller.libvirt`                    | Configuration for the libvirt provider.                                                                                                                                                                              | `{}`                            |
| `controller.platform.domain`            | The domain of the platform.                                                                                                                                                                                          | `$PREMISCALE_PLATFORM`          |
| `controller.platform.certificates`      | For local-only testing, you can provide self-signed certificates to the controller for connection to the platform services.                                                                                          | `{}`                            |
| `controller.platform.certificates.path` | If using a self-signed certificate for development purposes, specify the path.                                                                                                                                       | `''`                            |

### RBAC configuration

| Name                    | Description                                                                            | Value  |
| ----------------------- | -------------------------------------------------------------------------------------- | ------ |
| `serviceAccount.create` | Create a dedicated account for namespaced Kopf watches and status updates.             | `true` |
| `serviceAccount.name`   | Existing or created service account name; empty uses the chart name when creating one. | `""`   |
| `rbac.create`           | Grant namespaced AutoscalingGroup watch and status permissions.                        | `true` |

### PremiScale Controller Config

| Name                         | Description                                                                                                     | Value    |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------- | -------- |
| `configMap.enabled`          | Render and mount the runtime ConfigMap.                                                                         | `true`   |
| `configMap.name`             | ConfigMap name; empty preserves the chart-name default.                                                         | `""`     |
| `configMap.source`           | Choose values for structured config, or crds to project a named ControllerConfig and its referenced Hosts/ASGs. | `values` |
| `configMap.controllerConfig` | Name in premiscale-crds.controllerConfigs to project when source is crds.                                       | `""`     |
| `configMap.config`           | Optional complete YAML override for existing --set-file users; leave empty to use structured config.            | `""`     |
| `configMap.immutable`        | If true, the ConfigMap cannot be updated. If false, the ConfigMap can be updated.                               | `false`  |
| `configMap.labels`           | Labels to be added to the ConfigMap.                                                                            | `{}`     |
| `configMap.annotations`      | Annotations to be added to the ConfigMap.                                                                       | `{}`     |

### Controller service

| Name                               | Description                                                                                                                            | Value       |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `service.enabled`                  | Enable or disable the service. If ingress is enabled, the service type is automatically enabled and the type switched to LoadBalancer. | `true`      |
| `service.type`                     | The service type.                                                                                                                      | `ClusterIP` |
| `service.ports.liveness`           | Configure the liveness probe port.                                                                                                     | `{}`        |
| `service.ports.metrics.protocol`   | Transport protocol for Flask readiness and metrics.                                                                                    | `TCP`       |
| `service.ports.metrics.port`       | Service port for Flask readiness and metrics.                                                                                          | `9090`      |
| `service.ports.metrics.targetPort` | Container port for Flask readiness and metrics.                                                                                        | `metrics`   |

### Dragonfly broker

| Name                                      | Description                                                                                           | Value       |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------- | ----------- |
| `dragonfly.enabled`                       | Install the DragonflyDB broker dependency. Disable when using an external broker.                     | `true`      |
| `dragonfly.nameOverride`                  | Override the broker chart name used in resource names.                                                | `""`        |
| `dragonfly.fullnameOverride`              | Override the broker resource name; automatic controller discovery follows this value.                 | `""`        |
| `dragonfly.replicaCount`                  | Keep one writable broker; additional chart replicas are independent servers, not a replicated queue.  | `1`         |
| `dragonfly.service.type`                  | Kubernetes Service type for the broker.                                                               | `ClusterIP` |
| `dragonfly.service.port`                  | Redis-compatible Service port; the controller discovers this port automatically.                      | `6379`      |
| `dragonfly.storage.enabled`               | Use a StatefulSet and persistent volume for broker snapshots.                                         | `true`      |
| `dragonfly.storage.requests`              | Persistent volume capacity for broker snapshots.                                                      | `1Gi`       |
| `dragonfly.storage.storageClassName`      | Storage class; empty uses the cluster's default class.                                                | `""`        |
| `dragonfly.storage.useFullnameForVolumes` | Use dependency-specific Service and PVC names to avoid collisions with the parent chart.              | `true`      |
| `dragonfly.podSecurityContext`            | Run the broker as UID/GID 1001 and make its persistent volume writable.                               | `{}`        |
| `dragonfly.securityContext`               | Drop container capabilities and prevent privilege escalation.                                         | `{}`        |
| `dragonfly.extraArgs`                     | Broker arguments: one worker, 256 MB data limit, no eviction, and snapshots every minute under /data. | `[]`        |
| `dragonfly.resources.requests.cpu`        | Broker CPU request.                                                                                   | `100m`      |
| `dragonfly.resources.requests.memory`     | Broker memory request.                                                                                | `128Mi`     |
| `dragonfly.resources.limits.cpu`          | Broker CPU limit.                                                                                     | `1`         |
| `dragonfly.resources.limits.memory`       | Broker memory limit, including headroom for snapshots.                                                | `512Mi`     |

### PremiScale worker autoscaling

| Name                                                          | Description                                                                                                                                                                                     | Value   |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `keda.enabled`                                                | Install KEDA 2.20.2; leave disabled when the cluster already has KEDA.                                                                                                                          | `false` |
| `autoscaling.enabled`                                         | Enable KEDA for ha consolidated replicas or hha collection/publication Deployments. HA modes require Kafka, PostgreSQL journals, and a Kubernetes-mode configMap.                               | `false` |
| `autoscaling.controller.minReplicaCount`                      | Minimum consolidated ha replicas or fixed elected controller replicas in hha.                                                                                                                   | `2`     |
| `autoscaling.controller.maxReplicaCount`                      | Maximum consolidated ha replicas; all collection and publication triggers share this target.                                                                                                    | `8`     |
| `autoscaling.pollingInterval`                                 | Seconds between KEDA activation checks.                                                                                                                                                         | `15`    |
| `autoscaling.cooldownPeriod`                                  | Idle seconds before scaling a worker deployment to zero.                                                                                                                                        | `300`   |
| `autoscaling.scaleDownStabilizationWindowSeconds`             | Stabilize scale-down decisions while existing work drains.                                                                                                                                      | `300`   |
| `autoscaling.collectors.minReplicaCount`                      | Minimum collection pods; zero allows idle scale-to-zero.                                                                                                                                        | `2`     |
| `autoscaling.collectors.maxReplicaCount`                      | Maximum collection pods.                                                                                                                                                                        | `8`     |
| `autoscaling.collectors.activeConnectionsPerReplica`          | Target active host operations per collection pod.                                                                                                                                               | `8`     |
| `autoscaling.collectors.outstandingRequestsPerReplica`        | Target queued or leased host requests per collection pod.                                                                                                                                       | `16`    |
| `autoscaling.publisherDefaults.minReplicaCount`               | Minimum pods for each remote database subscriber.                                                                                                                                               | `2`     |
| `autoscaling.publisherDefaults.maxReplicaCount`               | Maximum pods for each remote database subscriber.                                                                                                                                               | `4`     |
| `autoscaling.publisherDefaults.connectionsPerReplica`         | Maximum database publisher connections per pod, including idle connections.                                                                                                                     | `2`     |
| `autoscaling.publisherDefaults.activeConnectionsPerReplica`   | Target connections actively writing batches per publisher pod.                                                                                                                                  | `1.5`   |
| `autoscaling.publisherDefaults.outstandingRequestsPerReplica` | Target queued or leased batches per publisher pod.                                                                                                                                              | `20`    |
| `autoscaling.publishers`                                      | Map of controller.databases.publishers names to per-subscriber scaling overrides. PostgreSQL and InfluxDB subscribers can scale; primary and other local-file writers remain in the controller. | `{}`    |
| `autoscaling.workers.annotations`                             | Deployment annotations shared by separate worker pools.                                                                                                                                         | `{}`    |
| `autoscaling.workers.resources`                               | CPU and memory resources for each worker pod; collector subprocess count respects its CPU limit.                                                                                                | `{}`    |
| `autoscaling.workers.extraEnv`                                | Worker environment additions, including database DSNs from Secrets; controller.extraEnv is also inherited.                                                                                      | `[]`    |
| `autoscaling.workers.extraVolumes`                            | Worker credential and certificate volumes. Controller journal volumes are not inherited.                                                                                                        | `[]`    |
| `autoscaling.workers.extraVolumeMounts`                       | Mounts for worker credentials and certificates.                                                                                                                                                 | `[]`    |

### Kubernetes autoscaler

| Name                                                         | Description                                                                                                                                                       | Value         |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `cluster-autoscaler.enabled`                                 | Enable or disable the autoscaler dependency of the PremiScale controller. Enable this if you would like to autoscale the cluster on which the controller resides. | `false`       |
| `cluster-autoscaler.replicaCount`                            | Number of Cluster Autoscaler replicas.                                                                                                                            | `1`           |
| `cluster-autoscaler.resources.requests.cpu`                  | Cluster Autoscaler CPU request.                                                                                                                                   | `100m`        |
| `cluster-autoscaler.resources.requests.memory`               | Cluster Autoscaler memory request.                                                                                                                                | `256Mi`       |
| `cluster-autoscaler.resources.limits.memory`                 | Cluster Autoscaler memory limit.                                                                                                                                  | `512Mi`       |
| `cluster-autoscaler.extraArgs.v`                             | Cluster Autoscaler log verbosity.                                                                                                                                 | `2`           |
| `cluster-autoscaler.extraArgs.leader-elect`                  | Enable leader election for Cluster Autoscaler.                                                                                                                    | `true`        |
| `cluster-autoscaler.extraArgs.expander`                      | Node-group expansion policy.                                                                                                                                      | `least-waste` |
| `cluster-autoscaler.extraArgs.balance-similar-node-groups`   | Balance scaling across similar node groups.                                                                                                                       | `true`        |
| `cluster-autoscaler.extraArgs.skip-nodes-with-local-storage` | Protect nodes with local storage during scale-down.                                                                                                               | `true`        |
| `cluster-autoscaler.extraArgs.skip-nodes-with-system-pods`   | Protect nodes with system pods during scale-down.                                                                                                                 | `true`        |

### PostgreSQL operator

| Name                                          | Description                                                                                  | Value   |
| --------------------------------------------- | -------------------------------------------------------------------------------------------- | ------- |
| `cloudnative-pg.enabled`                      | Install the CloudNativePG operator. Leave disabled when the cluster already has an operator. | `false` |
| `cloudnative-pg.replicaCount`                 | Number of CloudNativePG operator replicas.                                                   | `1`     |
| `cloudnative-pg.config.clusterWide`           | Watch PostgreSQL resources across all namespaces.                                            | `false` |
| `cloudnative-pg.resources.requests.cpu`       | CloudNativePG operator CPU request.                                                          | `100m`  |
| `cloudnative-pg.resources.requests.memory`    | CloudNativePG operator memory request.                                                       | `128Mi` |
| `cloudnative-pg.resources.limits.memory`      | CloudNativePG operator memory limit.                                                         | `512Mi` |
| `cloudnative-pg.monitoring.podMonitorEnabled` | Create a PodMonitor for the CloudNativePG operator.                                          | `false` |
