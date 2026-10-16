{{/*
Egress to cluster DNS, which every policy in these charts needs.

The peers and ports come from `global.networkPolicy.dns`, so one setting
covers the umbrella chart and every signal chart:

  global:
    networkPolicy:
      dns:
        peers: [<NetworkPolicyPeer>, ...]   # default: kube-system k8s-app=kube-dns
        ports: [<NetworkPolicyPort>, ...]   # default: UDP and TCP 53

Unset or empty keeps the default, kube-system pods labelled
k8s-app=kube-dns on port 53. An empty `to` would allow egress anywhere, so
an empty list never reaches the rendered policy. Examples (NodeLocal
DNSCache, OpenShift) are in ../../README.md, "DNS egress".
*/}}
{{- define "common.dnsEgress" -}}
{{- $dns := dig "networkPolicy" "dns" dict (.Values.global | default dict) -}}
{{- $defaultPeers := list (dict "namespaceSelector" (dict "matchLabels" (dict "kubernetes.io/metadata.name" "kube-system")) "podSelector" (dict "matchLabels" (dict "k8s-app" "kube-dns"))) -}}
{{- $defaultPorts := list (dict "protocol" "UDP" "port" 53) (dict "protocol" "TCP" "port" 53) -}}
{{- $peers := $dns.peers | default $defaultPeers -}}
{{- $ports := $dns.ports | default $defaultPorts -}}
{{- if not (kindIs "slice" $peers) -}}
{{- fail "global.networkPolicy.dns.peers must be a list of NetworkPolicy peers" -}}
{{- end -}}
{{- if not (kindIs "slice" $ports) -}}
{{- fail "global.networkPolicy.dns.ports must be a list of NetworkPolicy ports" -}}
{{- end -}}
- to:
    {{- toYaml $peers | nindent 4 }}
  ports:
    {{- toYaml $ports | nindent 4 }}
{{- end -}}
