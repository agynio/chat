{{- define "chat.configureEnv" -}}
{{- $env := list -}}

{{- $threadsAddress := .Values.chat.threadsAddress -}}
{{- $env = append $env (dict "name" "THREADS_ADDRESS" "value" $threadsAddress) -}}

{{- $runnersAddress := .Values.chat.runnersAddress -}}
{{- $env = append $env (dict "name" "RUNNERS_ADDRESS" "value" $runnersAddress) -}}

{{- $identityAddress := .Values.chat.identityAddress -}}
{{- $env = append $env (dict "name" "IDENTITY_ADDRESS" "value" $identityAddress) -}}

{{- $userEnv := .Values.env | default (list) -}}
{{- $_ := set .Values "env" (concat $env $userEnv) -}}
{{- end -}}
