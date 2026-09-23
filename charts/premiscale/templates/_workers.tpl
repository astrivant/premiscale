{{/* Keep worker names stable and disjoint from controller selectors and journals. */}}
{{- define "premiscale.workerName" -}}
{{- printf "%s-%s" (include "premiscale.fullname" .root | trunc 20 | trimSuffix "-") .workload -}}
{{- end -}}

{{/* Validate runtime prerequisites before rendering any distributed workload. */}}
{{- define "premiscale.validateWorkers" -}}
{{- if not .Values.configMap.enabled -}}
{{- fail "autoscaling.enabled requires configMap.enabled and a Kubernetes-mode controller configuration" -}}
{{- end -}}
{{- $config := include "premiscale.runtimeConfig" . | fromYaml -}}
{{- if ne (dig "controller" "mode" "" $config) "kubernetes" -}}
{{- fail "Worker autoscaling requires controller.mode: kubernetes in the runtime configuration" -}}
{{- end -}}
{{- if ne (dig "controller" "healthcheck" "host" "" $config) "0.0.0.0" -}}
{{- fail "Worker autoscaling requires controller.healthcheck.host: 0.0.0.0 in the runtime configuration" -}}
{{- end -}}
{{- if not (dig "controller" "kubernetes" "stateDsn" "" $config) -}}
{{- fail "HA requires controller.kubernetes.stateDsn for shared PostgreSQL journals" -}}
{{- end -}}
{{- if not (dig "controller" "kafka" "enabled" false $config) -}}
{{- fail "HA requires controller.kafka.enabled" -}}
{{- end -}}
{{- if not (dig "controller" "kafka" "bootstrapServers" "" $config) -}}
{{- fail "HA requires controller.kafka.bootstrapServers" -}}
{{- end -}}
{{- if (dig "controller" "databases" "timeseries" "dbfile" "" $config) -}}
{{- fail "HA primary metrics must be ephemeral; remove timeseries.dbfile and use a PostgreSQL subscriber for durable metrics" -}}
{{- end -}}
{{- if gt (int .Values.autoscaling.controller.minReplicaCount) (int .Values.autoscaling.controller.maxReplicaCount) -}}
{{- fail "autoscaling.controller.minReplicaCount must not exceed maxReplicaCount" -}}
{{- end -}}
{{- range $name, $_ := .Values.autoscaling.publishers -}}
{{- if not (has (dig "controller" "databases" "publishers" $name "type" "" $config) (list "postgresql" "influxdb")) -}}
{{- fail (printf "autoscaling.publishers.%s requires a matching PostgreSQL or InfluxDB subscriber in the runtime configuration" $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "premiscale.leaseName" -}}
{{- printf "%s-leader" (include "premiscale.fullname" . | trunc 56 | trimSuffix "-") -}}
{{- end -}}
