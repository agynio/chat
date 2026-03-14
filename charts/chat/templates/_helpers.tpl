{{- define "chat.configureEnv" -}}
{{- $env := list -}}

{{- $threadsAddress := required "chat.threadsAddress must be set" .Values.chat.threadsAddress -}}
{{- $env = append $env (dict "name" "THREADS_ADDRESS" "value" $threadsAddress) -}}

{{- $userEnv := .Values.env | default (list) -}}
{{- $_ := set .Values "env" (concat $env $userEnv) -}}
{{- end -}}
