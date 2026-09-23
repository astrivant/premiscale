# Static configuration

Store static configuration for supporting services here, grouped by service (for example, Grafana configuration in `grafana/`).

`keda/` contains matching controller and chart examples for `ha` consolidated containers and `hha` separate collection/publication Deployments, with Kafka metrics transport and shared PostgreSQL journals. Supply metric and journal DSNs through the referenced Secret. See the [chart scaling instructions](../charts/premiscale/README.md#scale-collection-and-database-publishing-with-keda) and [process trees](../pkg/premiscale/daemon/README.md).

`crds/values.yaml` selects a ControllerConfig as the runtime source. The Helm chart resolves its Host and AutoscalingGroup references into a ConfigMap; Kopf keeps it current, and an installed Reloader rolls affected workloads. `keda/values.yaml` embeds the full runtime configuration directly in Helm values, with no separate `--set-file` argument. Use `config.controller` for runtime fields and chart-level settings for placement, disruption budgets, and rollout bounds.

`influxdb/values.yaml` adds an optional InfluxDB 2.x metrics publisher with a Secret-backed write token. Merge its environment entries with other database credentials when combining overlays. See the [metrics pipeline](../pkg/premiscale/metrics/README.md).
