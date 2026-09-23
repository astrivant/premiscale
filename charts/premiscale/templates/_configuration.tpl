{{/* Resolve the single ConfigMap name used by every role, reloader, and RBAC. */}}
{{- define "premiscale.configMapName" -}}
{{- .Values.configMap.name | default .Chart.Name -}}
{{- end -}}

{{/* Standalone mode serves readiness and metrics on the health listener. */}}
{{- define "premiscale.apiPort" -}}
{{- $config := include "premiscale.runtimeConfig" . | fromYaml -}}
{{- if hasPrefix "kubernetes" $config.controller.mode -}}
{{- dig "controller" "healthcheck" "apiPort" 9090 $config -}}
{{- else -}}
{{- dig "controller" "healthcheck" "port" 8085 $config -}}
{{- end -}}
{{- end -}}

{{/* Resolve Helm-defined custom resources into the existing runtime file format. */}}
{{- define "premiscale.runtimeConfig" -}}
{{- if eq .Values.configMap.source "crds" -}}
{{- if or (not .Values.configMap.enabled) .Values.configMap.immutable .Values.configMap.config -}}
{{- fail "CRD projection requires a mutable enabled ConfigMap and no configMap.config override" -}}
{{- end -}}
{{- $namespace := .Values.global.namespace | default .Release.Namespace -}}
{{- $name := required "configMap.controllerConfig is required for CRD projection" .Values.configMap.controllerConfig -}}
{{- $crds := index .Values "premiscale-crds" -}}
{{- $resource := required (printf "ControllerConfig %s is missing from premiscale-crds.controllerConfigs" $name) (index $crds.controllerConfigs $name) -}}
{{- if ne ($resource.namespace | default $namespace) $namespace -}}
{{- fail "Projected ControllerConfig must be in the operator namespace" -}}
{{- end -}}
{{- $controller := deepCopy $resource.spec -}}
{{- if ne ((dig "kubernetes" "namespace" "" $controller) | default $namespace) $namespace -}}
{{- fail "ControllerConfig Kubernetes namespace must match its operator namespace" -}}
{{- end -}}
{{- if not (hasPrefix "kubernetes" $controller.mode) -}}
{{- fail "Live CRD projection requires a Kubernetes controller mode" -}}
{{- end -}}
{{- $hosts := list -}}
{{- $inventory := dict -}}
{{- range $host := $controller.autoscale.hosts -}}
{{- $entry := required (printf "Referenced Host %s is missing" $host) (index $crds.hosts $host) -}}
{{- if ne ($entry.namespace | default $namespace) $namespace -}}
{{- fail (printf "Host %s must be in the operator namespace" $host) -}}
{{- end -}}
{{- $_ := set $inventory $host true -}}
{{- $hosts = append $hosts (mergeOverwrite (deepCopy $entry.spec) (dict "name" $host)) -}}
{{- end -}}
{{- $groups := dict -}}
{{- range $group := $controller.autoscale.groups -}}
{{- $entry := required (printf "Referenced AutoscalingGroup %s is missing" $group) (index $crds.autoscalingGroups $group) -}}
{{- if ne ($entry.namespace | default $namespace) $namespace -}}
{{- fail (printf "AutoscalingGroup %s must be in the operator namespace" $group) -}}
{{- end -}}
{{- range $host := $entry.spec.hosts -}}
{{- if not (hasKey $inventory $host) -}}
{{- fail (printf "AutoscalingGroup %s references Host %s outside the ControllerConfig inventory" $group $host) -}}
{{- end -}}
{{- end -}}
{{- $_ := set $groups $group (deepCopy $entry.spec) -}}
{{- end -}}
{{- $_ := set $controller "autoscale" (dict "hosts" $hosts "groups" $groups) -}}
{{- toYaml (dict "version" "v1alpha1" "controller" $controller) -}}
{{- else if .Values.configMap.config -}}
{{- $config := fromYaml .Values.configMap.config -}}
{{- if or (hasKey $config "Error") (not (hasKey $config "controller")) -}}
{{- fail "configMap.config must contain a complete controller YAML mapping" -}}
{{- end -}}
{{- toYaml $config -}}
{{- else -}}
{{- toYaml .Values.config -}}
{{- end -}}
{{- end -}}

{{/* Reloader updates the Deployment pod template; its rollout strategy controls disruption. */}}
{{- define "premiscale.reloadAnnotations" -}}
{{- if and .Values.reload.enabled .Values.configMap.enabled -}}
configmap.reloader.stakater.com/reload: {{ include "premiscale.configMapName" . | quote }}
{{- if .Values.reload.secrets }}
secret.reloader.stakater.com/auto: "true"
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Reuse configurable probe timings for every role, targeting its actual health server. */}}
{{- define "premiscale.probes" -}}
{{- range $name := list "startupProbe" "livenessProbe" "readinessProbe" -}}
{{- $probe := index $.root.Values.deployment $name -}}
{{- if $probe.enabled }}
{{ $name }}:
  httpGet:
    path: {{ $probe.path }}
    port: {{ ternary "metrics" $probe.port (and $.metricsOnly (eq (toString $probe.port) "healthcheck")) }}
  {{- with $probe.config }}
  {{- toYaml . | nindent 2 }}
  {{- end }}
{{- end }}
{{- end }}
{{- end -}}
