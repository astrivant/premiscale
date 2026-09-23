{{/* Bind generated placement selectors to the exact workload, retaining user constraints. */}}
{{- define "premiscale.scheduling" -}}
{{- $settings := .settings -}}
{{- $affinity := deepCopy $settings.affinity -}}
{{- $anti := $affinity.podAntiAffinity | default dict -}}
{{- if .separateNodes -}}
{{- $required := $anti.requiredDuringSchedulingIgnoredDuringExecution | default list -}}
{{- $_ := set $anti "requiredDuringSchedulingIgnoredDuringExecution" (append $required (dict "topologyKey" "kubernetes.io/hostname" "labelSelector" (dict "matchLabels" .labels))) -}}
{{- end -}}
{{- if $settings.preferSeparateNodes -}}
{{- $preferred := $anti.preferredDuringSchedulingIgnoredDuringExecution | default list -}}
{{- $_ := set $anti "preferredDuringSchedulingIgnoredDuringExecution" (append $preferred (dict "weight" 100 "podAffinityTerm" (dict "topologyKey" "kubernetes.io/hostname" "labelSelector" (dict "matchLabels" .peers)))) -}}
{{- end -}}
{{- if $anti -}}
{{- $_ := set $affinity "podAntiAffinity" $anti -}}
{{- end -}}
{{- with $settings.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $settings.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $settings.topologySpreadConstraints }}
topologySpreadConstraints:
  {{- range . }}
  - {{- toYaml (mergeOverwrite (dict "labelSelector" (dict "matchLabels" $.labels)) (deepCopy .)) | nindent 4 }}
  {{- end }}
{{- end }}
{{- end -}}

{{- define "premiscale.disruptionBudget" -}}
{{- $budget := .budget -}}
{{- if $budget.enabled }}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ .name }}
  namespace: {{ .namespace }}
spec:
  {{- if hasKey $budget "minAvailable" }}
  minAvailable: {{ $budget.minAvailable | toJson }}
  {{- else }}
  maxUnavailable: {{ $budget.maxUnavailable | toJson }}
  {{- end }}
  unhealthyPodEvictionPolicy: {{ $budget.unhealthyPodEvictionPolicy }}
  selector:
    matchLabels:
      {{- toYaml .labels | nindent 6 }}
{{- end }}
{{- end -}}
