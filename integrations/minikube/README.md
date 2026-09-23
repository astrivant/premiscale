# Local Minikube addon

This integration starts a local test cluster and installs PremiScale, Dragonfly, Cluster Autoscaler, and the CloudNativePG operator as one Helm release. It uses the repository's development image and the existing PremiScale chart. Default runtime configuration is embedded under `premiscale.config` in [the wrapper values](chart/values.yaml). [`.config/minikube/controller.yaml`](../../.config/minikube/controller.yaml) remains a standalone file example.

Install Minikube 1.39+, Docker with Buildx and a running engine, kubectl, and Helm 3.14+ or 4. Run these commands from the project root:

```sh
./integrations/minikube/minikube.sh start
./integrations/minikube/minikube.sh test
```

`start` creates or resumes the `premiscale` profile with Kubernetes 1.35.0, four CPUs, 4 GiB memory, and one node. It enables storage provisioning and metrics-server, builds the development image, loads an image tagged by its content into Minikube, installs the addon in namespace `premiscale`, waits for the workloads, then tests their HTTP, gRPC, and Dragonfly Service endpoints. The autoscaler uses the matching 1.35 image. Cluster creation keeps the current kubeconfig context; subsequent commands explicitly target the selected profile.

The defaults contain no hypervisor hosts or node groups, so the provider runs without provisioning VMs. CloudNativePG installs its operator and CRDs; it does not create a PostgreSQL Cluster or change the application's database backend. Dragonfly comes from the PremiScale chart's official DragonflyDB dependency, with overrides under `premiscale.dragonfly`. It is a single local instance with PVC storage and snapshots every minute. That snapshot interval can lose recent writes after a broker crash; it is a development setup, not a production durability configuration.

## Iteration and lifecycle

```sh
# Rebuild the image and upgrade the addon after changing code.
./integrations/minikube/minikube.sh enable
./integrations/minikube/minikube.sh test

# Inspect workloads and storage.
./integrations/minikube/minikube.sh status

# Render the complete addon, including CRDs, without starting a cluster.
./integrations/minikube/minikube.sh render > /tmp/premiscale-minikube.yaml

# Remove workloads, retaining the namespace, PVCs, and operator CRDs.
./integrations/minikube/minikube.sh disable

# Stop the profile while preserving its data.
./integrations/minikube/minikube.sh stop

# Delete this profile, including its local volumes.
./integrations/minikube/minikube.sh delete
```

In the default `singular` composition, the provider journal is mounted at `/var/lib/premiscale` on `premiscale-data`. Controller upgrades use `Recreate` so two processes cannot own the SQLite journal during rollout. HA compositions use shared PostgreSQL journals and elect one active controller. Dragonfly's `premiscale-dragonfly-data-premiscale-dragonfly-0` PVC also survives `disable`. Re-enabling the same release reuses these volumes. Failed installs are left in place for inspection with `status` and `kubectl --context premiscale -n premiscale logs deployment/premiscale`.

Profiles created with the former addon-local Dragonfly StatefulSet have a separate `data-dragonfly-0` PVC. Switching to the dependency creates a new broker volume and retains the old PVC; existing broker data is not migrated automatically. Export and restore data before upgrading if that local queue history is needed.

Images are loaded directly; this integration does not require a local registry, ingress controller, or registry redirect process. The image's content tag changes when the build changes, causing a rollout; enabling an unchanged build is idempotent. Helm repository metadata is kept under the ignored `.cache/minikube/helm/` directory. The script resolves all repository paths relative to its own location.

To access HTTP endpoints from the host:

```sh
kubectl --context premiscale -n premiscale port-forward service/premiscale 8085:8085 9090:9090
```

## Settings

To exercise KEDA, create a `premiscale-postgresql` Secret in the addon namespace with `dsn` and `journal-dsn` keys pointing to reachable PostgreSQL databases, then use the supplied configuration and wrapper values:

```sh
PREMISCALE_MINIKUBE_NODES=3 \
PREMISCALE_MINIKUBE_VALUES=.config/keda/minikube-values.yaml \
  ./integrations/minikube/minikube.sh start
kubectl --context premiscale -n premiscale get scaledobjects,hpa
```

This selects `hha`, installs KEDA and Strimzi-managed Kafka, and scales collectors and PostgreSQL publisher pods independently. Two controller candidates elect one active infrastructure controller and share PostgreSQL journals. The example has no hypervisor hosts; populate its host inventory to produce measurements and activate PostgreSQL publication. A cluster that already has KEDA should override `premiscale.keda.enabled=false`. Installing CloudNativePG does not create the PostgreSQL databases or this credential Secret. Controller candidates require separate nodes, so start with at least two eligible nodes (the example uses three). Allow additional cluster CPU, memory, and storage for the three Kafka nodes; the default Minikube size targets the singular example. See [deployment compositions and process trees](../../pkg/premiscale/daemon/README.md).

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `PREMISCALE_MINIKUBE_PROFILE` | `premiscale` | Minikube profile and kubectl context. |
| `PREMISCALE_MINIKUBE_NAMESPACE` | `premiscale` | Namespace for the addon. |
| `PREMISCALE_MINIKUBE_DRIVER` | `docker` | Minikube driver. |
| `PREMISCALE_MINIKUBE_NODES` | `1` | Node count at cluster start; use at least two for HA placement. |
| `PREMISCALE_MINIKUBE_CPUS` | `4` | CPUs assigned at cluster start. |
| `PREMISCALE_MINIKUBE_MEMORY` | `4096` | Memory in MiB assigned at cluster start. |
| `PREMISCALE_MINIKUBE_TIMEOUT` | `10m` | Startup and rollout timeout. |
| `PREMISCALE_MINIKUBE_CONFIG` | unset | Optional full controller YAML override; otherwise use structured Helm values. |
| `PREMISCALE_MINIKUBE_VALUES` | unset | Optional extra values for the addon chart. Controller values are nested under `premiscale`. |

The Helm release is named `premiscale`. Use a separate profile for each independent installation; the namespace setting chooses where to install within that profile. The operator and autoscaler also create cluster-scoped resources, so multiple installations in different namespaces of the same profile are not supported. Custom configuration files and values paths are interpreted relative to the caller's current directory. Minikube's normal `MINIKUBE_HOME` and `KUBECONFIG` settings remain available for isolated test runs.

## Addon format

This is a repository-local Helm addon for stock Minikube. Minikube's [contributor guide](https://minikube.sigs.k8s.io/docs/contrib/addons/) and [Helm addon guide](https://minikube.sigs.k8s.io/docs/contrib/helm-addons/) register named addons in Minikube's Go source and require rebuilding its binary. Consequently, `minikube addons enable premiscale` is not available with an unmodified binary; use this integration's `enable` command. The chart and lifecycle are kept together here so they can later be used by a compiled addon if desired.

In singular mode, Kopf serves `/healthz` on 8085; `/ready` and `/metrics` use 9090. HA pods serve their health and metrics on 9090, including standby controllers. The addon includes the PremiScale CRDs and namespaced status RBAC. Shared process observations use an ephemeral `emptyDir`; singular provider journals remain on the data PVC, while HA journals use PostgreSQL. ASG status is published for resources labelled `premiscale.com/cluster: minikube` in the addon namespace.

The parent chart supplies PDBs and topology spread constraints for controllers and workers. Singular's PDB blocks voluntary node drains unless disabled for maintenance. An installed Stakater Reloader is required to roll pods after live ConfigMap/Secret edits; Helm upgrades also use configuration checksums. See [reload and placement settings](../../charts/premiscale/README.md#configuration-reloads-placement-and-disruption).
