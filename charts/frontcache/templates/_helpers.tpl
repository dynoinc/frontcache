{{- define "frontcache.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "frontcache.fullname" -}}
{{- if contains .Chart.Name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "frontcache.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- include "frontcache.selectorLabels" . | nindent 0 }}
{{- end }}

{{- define "frontcache.selectorLabels" -}}
app.kubernetes.io/name: {{ include "frontcache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "frontcache.serverSelectorLabels" -}}
{{ include "frontcache.selectorLabels" . }}
app.kubernetes.io/component: server
{{- end }}

{{- define "frontcache.routerSelectorLabels" -}}
{{ include "frontcache.selectorLabels" . }}
app.kubernetes.io/component: router
{{- end }}

{{- define "frontcache.serverLabelSelector" -}}
{{- printf "app.kubernetes.io/name=%s,app.kubernetes.io/instance=%s,app.kubernetes.io/component=server" (include "frontcache.name" .) .Release.Name }}
{{- end }}

{{- define "frontcache.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}
