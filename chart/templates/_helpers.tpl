{{/* Keep names release-scoped so two installs can share a namespace safely. */}}
{{- define "tx-monitoring.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tx-monitoring.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "tx-monitoring.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "tx-monitoring.appName" -}}
{{- printf "%s-%s" (include "tx-monitoring.fullname" .root) .name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tx-monitoring.labels" -}}
app.kubernetes.io/part-of: {{ include "tx-monitoring.name" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "tx-monitoring.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tx-monitoring.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "tx-monitoring.image" -}}
{{- $image := index .root.Values.images .name -}}
{{- required (printf "images.%s must be set" .name) $image -}}
{{- end -}}

{{- define "tx-monitoring.commonEnv" -}}
- name: DOTNET_ENVIRONMENT
  value: {{ .root.Values.platform.environment | quote }}
- name: OTel__ServiceName
  value: {{ .serviceName | quote }}
- name: OTel__Endpoint
  value: {{ required "platform.otelEndpoint must identify an existing OTLP collector" .root.Values.platform.otelEndpoint | quote }}
{{- end -}}

{{- define "tx-monitoring.istioPodAnnotation" -}}
{{- if .Values.istio.sidecarInjection }}
sidecar.istio.io/inject: "true"
{{- end }}
{{- end -}}
