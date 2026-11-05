{{- define "modelgate.tag" -}}{{ .Values.image.tag | default .Chart.AppVersion }}{{- end -}}
{{- define "modelgate.labels" -}}
app.kubernetes.io/part-of: modelgate
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
{{- define "modelgate.analyzerURL" -}}http://modelgate-analyzer.{{ .Release.Namespace }}.svc{{- end -}}
