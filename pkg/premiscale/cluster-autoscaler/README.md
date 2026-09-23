# Cluster Autoscaler integration

PremiScale implements Kubernetes Cluster Autoscaler's [external gRPC cloud provider](https://github.com/kubernetes/autoscaler/tree/cluster-autoscaler-1.35.0/cluster-autoscaler/cloudprovider/externalgrpc). The public protocol is pinned to **Cluster Autoscaler 1.35.0**, matching the bundled autoscaler chart.

The Go frontend exposes the upstream `CloudProvider` service and forwards requests to a Python backend over a private Unix socket. It marshals template nodes using the Kubernetes Go API so scale-from-zero receives real Kubernetes protobuf messages. Python owns discovery, scheduling, provisioning, deletion, and a persistent operation journal: SQLite in the default singular composition, or shared PostgreSQL for HA. The existing internal reconciliation queue is independent of this provider.

## Layout

- `proto/`: the upstream contract and the internal template service.
- `protocol/`: generated Go bindings.
- `bridge/`: the public provider and Kubernetes template conversion.
- `cmd/premiscale-autoscaler/`: the supervised native frontend.
- `integrations/python/premiscale_cluster_autoscaler/`: Python bindings, lifecycle worker, libvirt driver, and supervisor.

Python imports use `premiscale_cluster_autoscaler`. Both Python packages and the Go sources are included in Poetry distributions. The hyphenated directory is a Go module and package resource, rather than a Python import name.

## Controller configuration

Use `controller.mode: kubernetes` or `kubernetes-external-metrics`. Under `controller.kubernetes`, configure:

```yaml
providerHost: 0.0.0.0
providerPort: 50051
clusterName: production
stateFile: /var/lib/premiscale/autoscaler.db
# stateDsn: ${PREMISCALE_JOURNAL_DSN}  # Required for ha and hha; overrides stateFile.
# providerCert: /etc/premiscale/tls/tls.crt
# providerKey: /etc/premiscale/tls/tls.key
# clientCA: /etc/premiscale/tls/ca.crt
```

Keep `stateFile` on persistent storage and run one active provider per journal. An exclusive process lock prevents two local providers from claiming the same file. `clusterName` must remain stable: it identifies managed domains and their journaled disk ownership records. A separate journal and cluster name are required for each independent cluster. The legacy `autoscalerHost` and `autoscalerPort` configuration keys remain accepted; autoscaler initiates the provider connection, so they are not the provider listener settings.

For `ha` and `hha`, set `stateDsn` to a shared PostgreSQL database. VM operations and volume ownership use a schema derived from `clusterName`. Only the elected controller starts the provider, and a PostgreSQL session advisory lock protects provider ownership. Existing SQLite journals require an offline data migration before switching a managed cluster to PostgreSQL. See the [process tree and failover behavior](../daemon/README.md).

The public frontend supports server TLS and optional mutual TLS. The private backend uses a Unix socket in a supervisor-owned temporary directory. `providerHost` defaults to loopback. Bind a reachable interface when using a Kubernetes Service.

## Node groups and VM templates

Each named `controller.autoscale.groups` entry becomes a node group. `scaling.minNodes` and `maxNodes` enforce group limits. Referenced hosts must be configured QEMU/libvirt hosts using SSH or TLS. Inline host definitions also work.

On **each candidate host**, define a shut-off template domain whose name matches the group's `name`. Its disks must be file-backed volumes in filesystem-backed libvirt storage pools. The first disk is cloned from `image`; additional template disks are cloned separately. Templates containing host devices, shared filesystems, or TPM devices are rejected. Prepare guest images for cloning, including cloud-init, a clean machine identity, Kubernetes prerequisites, and the intended worker architecture.

`imageMigration: centralized` requires the source volumes to be visible on each candidate host. With `migrate`, a missing source image is streamed from another group host through libvirt. Migrated images must be flattened rather than dependent on backing files. A destination template and its storage pool must still exist on each host. Clones receive unique UUIDs, names, MAC addresses, disks, and cloud-init media; source templates are never deleted.

`cloud-init.inline` takes precedence over `cloud-init.file`. Supply the worker bootstrap/join procedure appropriate to your cluster. The provider replaces these literal placeholders in user-data:

- `${PREMISCALE_NODE_NAME}`
- `${PREMISCALE_PROVIDER_ID}`
- `${PREMISCALE_NODE_GROUP}`

Configure kubelet with `--provider-id=${PREMISCALE_PROVIDER_ID}` and the generated node name. If setting `nodeLabels`, `nodeTaints`, or `maxPods` on a group, apply the same settings during guest bootstrap: these fields describe the node to autoscaler's scheduling simulator. They do not themselves configure kubelet. Cluster credentials and join commands come from your cloud-init configuration; the provider does not invent or fetch them.

NoCloud metadata always sets a unique instance ID and hostname. Networking can use DHCP (`dynamic`), a reserved address from the group's pool (`static`), or the image/user-data's networking (`ignore`). Static subnets use CIDR notation. Reservations include pending and deleting VMs, preventing address reuse before deletion completes.

`random` selects a host randomly; `linear` balances managed node counts; `linear-random` randomizes ties; `vacancy` considers free memory and storage for the template. The libvirt driver checks memory again before defining a VM. All provisioning and deletion failures remain visible in `NodeGroupNodes` and are retried using the same persistent identity.

## Autoscaler configuration

Run Cluster Autoscaler with:

```text
--cloud-provider=externalgrpc
--cloud-config=/etc/autoscaler/cloud-config.yaml
```

An example cloud configuration is checked in at [.config/cluster-autoscaler/cloud-config.yaml](../../../.config/cluster-autoscaler/cloud-config.yaml). Set its address to the reachable provider endpoint and mount any TLS material referenced by the file.

For the bundled dependency, use `cloudProvider: externalgrpc`, set `extraArgs.cloud-config` to the mounted file, and configure `extraVolumes`/`extraVolumeMounts`. The upstream chart's `cloudConfigPath` option alone does not emit the flag for this provider. [.config/cluster-autoscaler/values.yaml](../../../.config/cluster-autoscaler/values.yaml) supplies an example; create its referenced ConfigMap from the cloud configuration and provide a controller configuration/PVC for your environment.

Scale-up commits new target reservations before replying and provisions asynchronously. Target reduction cancels only queued work; it never deletes an existing VM. Node deletion validates every requested identity before changing state, decrements the target once, and retries cleanup until the owned VM and disks are gone. On startup/refresh, marked domains are rediscovered; externally removed running VMs are reprovisioned using their recorded identity. Pricing is optional in the upstream protocol and returns `UNIMPLEMENTED`; this driver exposes no GPU instance types.

## Build and test

Use Go 1.25+ and the project's Python 3.14 Poetry environment:

```shell
poetry install
cd pkg/premiscale/cluster-autoscaler
go test -race ./...
go build -mod=readonly -o /tmp/premiscale-autoscaler ./cmd/premiscale-autoscaler
cd ../../..
PREMISCALE_AUTOSCALER_BINARY=/tmp/premiscale-autoscaler poetry run pytest -q
```

Docker images include the compiled frontend. For other installations, set `PREMISCALE_AUTOSCALER_BINARY` to a prebuilt executable, or make Go available for the first run. The supervisor compiles the packaged source with `-mod=readonly`, caches it under `$XDG_CACHE_HOME/premiscale/autoscaler` (default `~/.cache`), and reuses it until the sources/dependency lock/platform change. Compilation may download the pinned Go dependencies on its first run.

Regenerate bindings with:

```shell
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.8
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
PATH="$(go env GOPATH)/bin:$PATH" poetry run bash scripts/generate-autoscaler-protos.sh
```

The generator preserves multiline Python docstrings. Native Go tests validate upstream RPC forwarding, status codes, deadlines, and Kubernetes node serialization. Python tests exercise lifecycle invariants and the real Go↔Python boundary against fake infrastructure. Live hypervisor and Kubernetes acceptance testing requires prepared worker templates and cluster credentials.

The vendored `proto/externalgrpc.proto` retains the Kubernetes Authors' Apache-2.0 notice. Its upstream source is [here](https://github.com/kubernetes/autoscaler/blob/cluster-autoscaler-1.35.0/cluster-autoscaler/cloudprovider/externalgrpc/protos/externalgrpc.proto).
