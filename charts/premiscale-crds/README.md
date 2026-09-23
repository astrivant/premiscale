# PremiScale configuration resources

This chart installs the `premiscale.com/v1alpha1` API and can create any number of custom resources from values.

| Kind | Values map | Purpose |
| --- | --- | --- |
| `ControllerConfig` | `controllerConfigs` | Controller, database, broker, API, and reconciliation settings; references to hosts and groups. |
| `Host` | `hosts` | Hypervisor address, transport, credentials, and resource limits. |
| `AutoscalingGroup` | `autoscalingGroups` | VM template, networking, replacement, and scaling settings; references to hosts. |

All three kinds are namespaced. Each map key becomes `metadata.name`; no release-name prefix is added. Entries contain `spec` and optional `namespace`, `labels`, and `annotations`. Namespace defaults to `global.namespace` when provided, otherwise the Helm release namespace. Resource metadata overrides `commonLabels` and `commonAnnotations`. Maps have no fixed resource-count limit; Kubernetes still applies its normal object-size and namespace quotas.

The chart's default values create no instances. Install the CRDs alone:

```shell
helm install premiscale-crds ./charts/premiscale-crds --namespace premiscale --create-namespace
```

Create multiple hosts using a values file:

```yaml
commonLabels:
  app.kubernetes.io/part-of: premiscale

hosts:
  hypervisor-a:
    spec:
      address: 192.0.2.10
      protocol: ssh
      port: 22
      hypervisor: qemu
      user: root
      sshKey: $PREMISCALE_SSH_KEY
  hypervisor-b:
    labels:
      location: rack-b
    spec:
      address: 192.0.2.11
      protocol: tls
      port: 16514
      hypervisor: qemu
```

The [complete example](examples/multiple-resources.yaml) creates **two ControllerConfigs, two Hosts, and two AutoscalingGroups**, with references connecting each set:

```shell
helm template resources ./charts/premiscale-crds \
  --namespace premiscale --include-crds \
  --values ./charts/premiscale-crds/examples/multiple-resources.yaml

helm upgrade --install resources ./charts/premiscale-crds \
  --namespace premiscale --create-namespace \
  --values ./charts/premiscale-crds/examples/multiple-resources.yaml
```

`ControllerConfig.spec.autoscale.hosts` and `.groups` are lists of resource names in the ControllerConfig's namespace. `AutoscalingGroup.spec.hosts` contains names of Host resources in the group's namespace. `Host` gets its name from metadata. `AutoscalingGroup.spec.name` retains its existing meaning: the source libvirt template domain name, independent of the custom resource's metadata name. Create any explicitly overridden namespaces before installing their resources.

This chart defines and creates configuration resources; it does not launch a controller Deployment. The parent chart can consume them by selecting `configMap.source: crds` and a `configMap.controllerConfig` from `premiscale-crds.controllerConfigs`. Helm constructs the initial runtime ConfigMap, then the elected Kopf process reconciles changes to the selected ControllerConfig and its referenced Hosts and AutoscalingGroups every five seconds. Resources must share the operator namespace, and ASG hosts must belong to the controller inventory. See the [parent chart's configuration and rollout instructions](../premiscale/README.md#runtime-configuration-and-crds) and [complete Helm example](../../.config/crds/values.yaml).

An installed Reloader rolls controller and worker Deployments when that ConfigMap changes. Invalid projections leave the previous file intact. Deployment-coupled settings such as listeners, runtime-state mounts, cluster/journal identity, and transport destinations require a coordinated Helm upgrade. Direct ConfigMap edits are overwritten by its selected CR source. Keep credentials in Secrets and use environment-variable references in specifications; the projected file preserves references without expanding secret contents.

## ControllerConfig status

The selected ControllerConfig has a status subresource with `observedGeneration`, `configurationDigest`, `configMapRef`, and a Ready condition. `ConfigurationProjected` means the validated file is present; `InvalidConfiguration` or `ProjectionUnavailable` explains why the previous file was retained. This describes configuration projection, not completion of the subsequent Deployment rollout. Other ControllerConfigs are left untouched. Apply updated CRD definitions before upgrading an existing installation so the new status subresource is available.

## AutoscalingGroup status

In values mode, enable status publication by adding `premiscale.com/cluster: <clusterName>` to an AutoscalingGroup's labels. The label must match `controller.kubernetes.clusterName`, its namespace must match `controller.kubernetes.namespace` (the Pod namespace by default), and its metadata name must match the group key in the controller's YAML. In CRD projection mode, the selected ControllerConfig’s group references determine status ownership instead of the label filter. Each controller must have a distinct namespace/cluster identity. The controller chart supplies a dedicated service account and namespaced Role for watches and status patches.

Kopf patches the `/status` subresource every five seconds from the provider's private shared snapshot. Status contains `observedGeneration`, `lastObservationTime`, `targetReplicas`, `runningReplicas`, `pendingReplicas`, `deletingReplicas`, `failedReplicas`, and `Configured`, `Ready`, `Progressing`, and `Degraded` conditions. Transition timestamps change only when a condition's True/False/Unknown value changes. `runningReplicas` means VM provisioning completed; Kubernetes Node readiness is not assessed. Failed deletions count toward both deleting and failed replicas.

`observedGeneration` identifies the CR specification evaluated for this status, not a claim that it was applied. A CR that differs from the running file configuration receives `ConfigurationMismatch` and is not Ready. A group absent from the provider receives `GroupNotConfigured`. Missing, corrupt, or older-than-15-second provider observations remove the counts and set conditions to Unknown; old successful status is not republished as current. Operator downtime can leave the last status in Kubernetes, so consumers should also check `lastObservationTime`.

The CRD enables the status subresource and adds Target, Running, and Ready printer columns. Status is controller-owned and cannot be supplied through chart values. For existing installations, apply the updated CRDs before upgrading the controller as described under CRD lifecycle below.

## Schema ownership and validation

The typed OpenAPI schemas in [`crds/`](crds/) replace the former Yamale schema as the field-definition source. Generate both charts’ runtime values schemas, whole-resource JSON schemas, and the packaged validator for existing local YAML files with:

```shell
poetry run python scripts/schemas/generate-config-schemas.py
poetry run python scripts/schemas/generate-config-schemas.py --check
```

Run those commands from the repository root. Generated schemas preserve local YAML's `version`/`controller` wrapper and inline host/group representations, while CRs use Kubernetes metadata and named references. YAML loading continues to use ruamel.yaml. JSON Schema validation checks types, required fields, limits, and unknown fields; the API server additionally enforces the CRDs' CEL rules for relationships such as minimum and maximum node counts. Kubernetes prunes unknown CR fields unless the API request uses strict field validation.

CI checks generated artifacts, renders zero and multiple instances of every kind, rejects malformed values, and runs hypothesis-helm with explicit custom-resource schemas from `.hypothesis-helm.yaml`. A CRD-only default installation intentionally produces no ordinary `helm template` resources; use `--include-crds` to inspect the definitions.

## CRD lifecycle

CRDs live in Helm's special `crds/` directory so Helm installs them before custom resources. Helm skips existing CRDs and does not upgrade or remove these definitions during normal release upgrades or uninstalls. Apply reviewed CRD changes explicitly before upgrading resources:

```shell
kubectl apply --server-side --field-manager=premiscale-crds -f charts/premiscale-crds/crds/
```

Uninstalling this release removes the custom resources it created while retaining the CRDs. Install the definitions once per cluster; subsequent resource releases can use `--skip-crds`. See [Helm's CRD lifecycle documentation](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/) and [Kubernetes structural schema documentation](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/).

## Parameters

### Resource metadata

| Name                | Description                                                                           | Value |
| ------------------- | ------------------------------------------------------------------------------------- | ----- |
| `global`            | Parent-chart values; global.namespace supplies the default custom-resource namespace. | `{}`  |
| `commonLabels`      | Labels added to every custom resource; individual resource labels override them.      | `{}`  |
| `commonAnnotations` | Annotations added to every custom resource; individual annotations override them.     | `{}`  |

### Configuration resources

| Name                | Description                                                                          | Value |
| ------------------- | ------------------------------------------------------------------------------------ | ----- |
| `controllerConfigs` | Named ControllerConfig resources with controller settings and host/group references. | `{}`  |
| `hosts`             | Named Host resources describing hypervisor connections.                              | `{}`  |
| `autoscalingGroups` | Named AutoscalingGroup resources describing VM templates and scaling policies.       | `{}`  |
