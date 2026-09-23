{{/* Render explicit resource identity and merged metadata. */}}
{{- define "premiscale-crds.metadata" -}}
name: {{ .name | quote }}
namespace: {{ .resource.namespace | default (.root.Values.global.namespace | default .root.Release.Namespace) | quote }}
{{- with mergeOverwrite (dict) .root.Values.commonLabels (.resource.labels | default dict) }}
labels:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with mergeOverwrite (dict) .root.Values.commonAnnotations (.resource.annotations | default dict) }}
annotations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}
