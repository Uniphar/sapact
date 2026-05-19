{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "sapact.name" -}}
{{- default .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "sapact.regionalisedname" -}}
{{- (printf "%s-%s" (include "sapact.name" .) .Values.regioncode) | trunc 63 | trimSuffix "-"}}
{{- end }}