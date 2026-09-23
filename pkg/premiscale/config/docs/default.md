# `default.yaml`

## Parameters

### Work queues

`controller.broker` configures the Dragonfly/Redis transport used by controller workers. Omitted settings use these defaults:

| Field | Default | Meaning |
| --- | --- | --- |
| `url` | `PREMISCALE_REDIS_URL`, otherwise `redis://dragonfly:6379/0` | Service URL; `rediss://` enables TLS. |
| `namespace` | `PREMISCALE_QUEUE_NAMESPACE`, otherwise `premiscale` | Shared identity for this controller's queues. |
| `leaseSeconds` | `60` | Idle time before unfinished deliveries can be recovered; minimum 3. |
| `blockMilliseconds` | `1000` | Maximum blocking read duration. |
| `connectTimeout` | `5` | Connection timeout in seconds. |
| `socketTimeout` | `5` | Socket timeout in seconds; must exceed the blocking read duration. |
| `caFile` | empty | Mounted CA certificate path for TLS connections. |

URLs, namespaces, and CA paths support environment-variable expansion. Supply credentials using a Kubernetes Secret rather than storing them in controller YAML. See [work queue delivery and recovery](../../../../CONTRIBUTING.md#work-queues).

### Controller Configuration

| Name                                   | Description                                                                                                                                                                                                                                                    | Value                            |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| `controller.pidFile`                   | Path to the file where the controller daemon process writes its PID.                                                                                                                                                                                           | `/opt/premiscale/premiscale.pid` |
| `controller.mode`                      | The mode of the controller. Can be 'kubernetes' or 'standalone'. If 'standalone', the controller will not attempt to connect to a Kubernetes cluster autoscaler. If 'kubernetes', the controller will attempt to connect to the Kubernetes cluster autoscaler. | `standalone`                     |
| `controller.kubernetes.autoscalerPort` | The port on which the Kubernetes autoscaler is listening.                                                                                                                                                                                                      | `8080`                           |
| `controller.kubernetes.autoscalerHost` | The host on which the Kubernetes autoscaler is running. See also the cluster autoscaler Helm chart: https://github.com/premiscale/kubernetes-autoscaler/tree/master/charts/cluster-autoscaler.                                                                 | `cluster-autoscaler`             |

### Cluster Autoscaler provider

These settings configure the external gRPC provider in Kubernetes controller modes.

| Name | Description | Default |
| --- | --- | --- |
| `controller.kubernetes.providerHost` | Provider listener interface; use a reachable interface when exposing a Service. | `127.0.0.1` |
| `controller.kubernetes.providerPort` | Provider gRPC listener port. | `50051` |
| `controller.kubernetes.clusterName` | Stable owner identity for managed VMs and the operation journal. | `premiscale` |
| `controller.kubernetes.stateFile` | Persistent SQLite operation journal. | `/opt/premiscale/autoscaler.db` |
| `controller.kubernetes.providerCert` | Optional server certificate PEM path. | `""` |
| `controller.kubernetes.providerKey` | Optional server private-key PEM path. | `""` |
| `controller.kubernetes.clientCA` | Optional CA for authenticating autoscaler clients with mutual TLS. | `""` |

See the [provider documentation](../../cluster-autoscaler/README.md) for node-group templates, cloud-init bootstrap, and deployment configuration.

### Healthcheck Configuration

| Name                          | Description                                            | Value       |
| ----------------------------- | ------------------------------------------------------ | ----------- |
| `controller.healthcheck`      | Configure the healthcheck endpoint for the controller. | `{}`        |
| `controller.healthcheck.host` | The host to bind the healthcheck endpoint to.          | `127.0.0.1` |
| `controller.healthcheck.port` | The port to bind the healthcheck endpoint to.          | `8085`      |

### Database Configuration

| Name                                            | Description                                                                                                                                                                             | Value                           |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| `controller.databases.maxHostConnectionThreads` | Maximum concurrent host connections per collection subprocess.                                                                                                                           | `10`                            |
| `controller.databases.hostConnectionQueueSize`  | Maximum outstanding host futures per collection subprocess. Defaults to the same value as 'controller.databases.maxHostConnectionThreads'. | `10`                            |
| `controller.databases.collectionInterval`       | How often the agent retrieves state from all of the connected hosts.                                                                                                                    | `60`                            |
| `controller.databases.hostConnectionTimeout`    | How long to wait for a connection to a host before timing out.                                                                                                                          | `60`                            |
| `controller.databases.state.type`               | The type of database to use for storing state. Can be 'mysql' or 'sqlite' or 'memory'.                                                                                                  | `memory`                        |
| `controller.databases.timeseries.type`          | The type of database to use for storing time series data. Currently supports 'memory'.                                                                                  | `memory`                        |
| `controller.databases.timeseries.dbfile`        | If using the 'memory' type, the path to the file where the time series data is stored as a CSV format.                                                                                  | `/opt/premiscale/timeseries.db` |
| `controller.databases.timeseries.retention`     | How long to keep time series data in the database.                                                                                                                                      | `300`                           |

Additional `controller.databases.publishers` entries fan out metrics alongside the primary time-series store. Each named entry accepts `type` (`memory`, `postgresql`, or `influxdb`), `retention` (default 300 seconds), `dbfile` for TinyFlux, or `dsn` for PostgreSQL. PostgreSQL supports `connectTimeout` (default 5 seconds) and `statementTimeout` (default 10000 milliseconds). The `primary` name is reserved; names may contain letters, digits, underscores, and hyphens, must start with a letter or digit, and have at most 63 characters. See [metrics publication](../../../../CONTRIBUTING.md#metrics-collection-and-publication) for an example.

### Platform Configuration

| Name                                    | Description                                                                                                                 | Value                   |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| `controller.platform`                   | Configure the platform                                                                                                      | `{}`                    |
| `controller.platform.actionsQueueMax`   | The maximum number of inbound actions from the platform to queue up before dropping them. 0 means no limit.                 | `0`                     |
| `controller.platform.domain`            | The domain of the platform.                                                                                                 | `$PREMISCALE_PLATFORM`  |
| `controller.platform.token`             | The token to use to authenticate with the platform.                                                                         | `$PREMISCALE_TOKEN`     |
| `controller.platform.certificates`      | For local-only testing, you can provide self-signed certificates to the controller for connection to the platform services. | `{}`                    |
| `controller.platform.certificates.path` | Path to a directory containing the controller's certificates.                                                               | `/opt/premiscale/certs` |

### Reconciliation Configuration

| Name                                 | Description                                                                                                                                                                  | Value |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| `controller.reconciliation.interval` | How often the controller reconciles the state of the ASGs. The controller collects time series and state in separate databases and queues up actions for autoscaling groups. | `60`  |
| `controller.reconciliation.collection.minThreads` | Minimum threads per nonempty host partition. | `1` |
| `controller.reconciliation.collection.initialThreads` | Initial threads per worker; must fit `maxHostConnectionThreads`. | `1` |
| `controller.reconciliation.collection.targetThroughput` | Successful hosts per second per worker; omitted derives the target from partition size and collection interval. | Derived |
| `controller.reconciliation.collection.proportionalGain` | Gain applied to relative throughput error. | `1.0` |
| `controller.reconciliation.collection.integralGain` | Gain applied to accumulated relative error in seconds. | `0.1` |
| `controller.reconciliation.collection.derivativeGain` | Gain applied to relative error change per second. | `0.05` |
| `controller.reconciliation.collection.smoothing` | Weight of the latest throughput measurement, greater than zero and at most one. | `0.3` |
| `controller.reconciliation.collection.deadband` | Relative error tolerance, from zero to less than one. | `0.1` |
| `controller.reconciliation.collection.maxStep` | Maximum change in thread count per pass. | `2` |

### Autoscaling Configuration

| Name                          | Description                                                  | Value |
| ----------------------------- | ------------------------------------------------------------ | ----- |
| `controller.autoscale.hosts`  | Groups of hosts to assign to ASGs.                           | `[]`  |
| `controller.autoscale.groups` | Specify and configure autoscaling groups on the hosts above. | `{}`  |

InfluxDB 2.x publishers use `url`, `organization`, `bucket`, and a Secret-backed `token`, with optional `timeoutSeconds` and `caFile`. Their retention is configured on the InfluxDB bucket. See the [pipeline and backend guide](../../metrics/README.md) and [Helm overlay](../../../../.config/influxdb/values.yaml).
