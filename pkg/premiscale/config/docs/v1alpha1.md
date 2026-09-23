# PremiScale config version `v1alpha1`

This documentation outlines the PremiScale controller `v1alpha1` configuration specification.

## Parameters

### Work queues

The optional `controller.broker` section configures Redis-compatible Dragonfly queues. Its URL and queue namespace default to `PREMISCALE_REDIS_URL` and `PREMISCALE_QUEUE_NAMESPACE`, with fallback values `redis://dragonfly:6379/0` and `premiscale`. Explicit YAML values override those defaults. See the [broker settings](default.md#work-queues) for delivery leases, timeouts, and TLS configuration.

### Controller Configuration

| Name                                                           | Description                                                                                                                                                             | Value                   |
| -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| `controller`                                                   | Configure the controller, such as database connection criteria and how often to take certain controller-specific actions.                                               | `{}`                    |
| `contoller.databases`                                          | Configure the databases the controller uses to store state and metrics.                                                                                                 | `{}`                    |
| `controller.databases.state`                                   | State is where the ASGs' current state as of the (latest reconciliation loop execution) is stored so it can be queried without making additional calls through libvirt. | `{}`                    |
| `controller.databases.state.type`                              | The type of database to use for storing state. Can be 'mysql' or 'sqlite' or 'memory'.                                                                                  | `mysql`                 |
| `controller.databases.state.collectionInterval`                | How often the agent collects metrics and stores them in the database.                                                                                                   | `60`                    |
| `controller.databases.state.connection`                        | Connection details for the state database.                                                                                                                              | `{}`                    |
| `controller.databases.state.connection.url`                    | The URL of the database.                                                                                                                                                | `$MYSQL_HOST`           |
| `controller.databases.state.connection.database`               | The name of the database to create or use, if it already exists.                                                                                                        | `$MYSQL_DATABASE`       |
| `controller.databases.state.connection.credentials`            | The credentials to use to connect to the database.                                                                                                                      | `{}`                    |
| `controller.databases.state.connection.credentials.username`   | The username to use to connect to the database.                                                                                                                         | `$MYSQL_USERNAME`       |
| `controller.databases.state.connection.credentials.password`   | The password to use to connect to the database.                                                                                                                         | `$MYSQL_PASSWORD`       |
| `controller.databases.metrics`                                 | How often metrics collection is performed from every host.                                                                                                              | `{}`                    |
| `controller.databases.metrics.type`                            | The type of database to use for storing metrics. Currently supports 'memory'.                                                                                         | `memory`              |
| `controller.databases.metrics.collectionInterval`              | How often the agent retrieves metrics from all of the connected hosts.                                                                                                  | `60`                    |
| `controller.databases.metrics.maxThreads`                      | Establish connections to hosts in maxThreads-batches.                                                                                                                   | `10`                    |
| `controller.databases.metrics.hostConnectionTimeout`           | How long to wait for a connection to a host before timing out.                                                                                                          | `60`                    |
| `controller.databases.metrics.trailing`                        | Seconds of collected metrics to keep and evaluate upon (must be >=interval). Default is 20 minutes                                                                      | `1200`                  |
| `controller.platform`                                          | Configure the platform                                                                                                                                                  | `{}`                    |
| `controller.platform.actionsQueueMax`                          | The maximum number of actions from the platform to queue up before dropping them. 0 means no limit.                                                                     | `0`                     |
| `controller.platform.domain`                                   | The domain of the platform.                                                                                                                                             | `$PREMISCALE_PLATFORM`  |
| `controller.platform.certificates`                             | For local-only testing, you can provide self-signed certificates to the controller for connection to the platform services.                                             | `{}`                    |
| `controller.platform.certificates.path`                        | Path to a directory containing the controller's certificates.                                                                                                           | `/opt/premiscale/certs` |

### Reconciliation Configuration

| Name                                 | Description                                                                                                                                                                                    | Value |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| `controller.reconciliation.interval` | How often the controller reconciles the state of the ASGs. The controller collects metrics and state in separate databases and queues up actions for autoscaling groups in separate processes. | `60`  |
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

| Name                                               | Description                                                                                                                                                                                                                           | Value     |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `autoscale`                                        | Configure hosts and autoscaling groups on those hosts.                                                                                                                                                                                | `{}`      |
| `autoscale.hosts`                                  | Groups of hosts to assign to ASGs.                                                                                                                                                                                                    | `{}`      |
| `autoscale.hosts.group-1`                          | An example group of hosts to assign to an ASG.                                                                                                                                                                                        | `[]`      |
| `controller.autoscale.groups`                      | Specify and configure autoscaling groups on the hosts above.                                                                                                                                                                          | `{}`      |
| `controller.autoscale.groups.asg-1`                | An example ASG configuration.                                                                                                                                                                                                         | `{}`      |
| `controller.autoscale.groups.asg-1.image`          | The image to use for VMs in this group.                                                                                                                                                                                               | `{}`      |
| `controller.autoscale.groups.asg-1.name`           | The name of the Libvirt domain to use as a template for VMs in this group.                                                                                                                                                            | `""`      |
| `controller.autoscale.groups.asg-1.imageMigration` | Options could be 'migrate' or 'centralized'. If set to migrate, the controller will copy the specified domain to every host, or, if centralized, the controller will not attempt to move the domain to every host a VM is created on. | `migrate` |


