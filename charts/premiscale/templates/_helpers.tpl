{{/*
Expand the name of the chart.
*/}}
{{- define "premiscale.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Build an image reference, falling back to the chart application version. */}}
{{- define "premiscale.image" -}}
{{- $registry := .Values.global.image.registry | default (.Values.deployment.image.registry | default "") -}}
{{- $tag := .Values.deployment.image.tag | default .Chart.AppVersion -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .Values.deployment.image.name $tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.deployment.image.name $tag -}}
{{- end -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "premiscale.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "premiscale.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "premiscale.labels" -}}
helm.sh/chart: {{ include "premiscale.chart" . }}
{{ include "premiscale.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "premiscale.selectorLabels" -}}
app.kubernetes.io/name: {{ include "premiscale.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "premiscale.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "premiscale.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Resolve the broker address using the dependency's actual Service name. */}}
{{- define "premiscale.brokerUrl" -}}
{{- if .Values.controller.broker.url -}}
{{- .Values.controller.broker.url -}}
{{- else if .Values.dragonfly.enabled -}}
{{- $broker := index .Subcharts "dragonfly" -}}
{{- $scheme := ternary "rediss" "redis" $broker.Values.tls.enabled -}}
{{- printf "%s://%s.%s.svc:%v/0" $scheme (include "dragonfly.fullname" $broker) .Release.Namespace $broker.Values.service.port -}}
{{- else -}}
redis://dragonfly:6379/0
{{- end -}}
{{- end -}}
