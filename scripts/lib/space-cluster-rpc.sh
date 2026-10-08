#!/usr/bin/env bash

space_cluster_rpc_configure() {
  local endpoint="${NODE_API_ENDPOINT:?NODE_API_ENDPOINT is required}"
  local insecure="${SPACE_INTERNAL_RPC_INSECURE_DEV:-false}"
  case "$insecure" in
    true|1|false|0) ;;
    *) echo "SPACE_INTERNAL_RPC_INSECURE_DEV must be true/false or 1/0" >&2; return 1 ;;
  esac
  cluster_rpc_transport=()
  case "$endpoint" in
    https://*) cluster_rpc_address="${endpoint#https://}" ;;
    http://*)
      if [[ "$insecure" != true && "$insecure" != 1 ]]; then
        echo "HTTP ClusterApi requires SPACE_INTERNAL_RPC_INSECURE_DEV=true" >&2
        return 1
      fi
      cluster_rpc_address="${endpoint#http://}"
      ;;
    *://*) echo "unsupported ClusterApi endpoint scheme" >&2; return 1 ;;
    *) cluster_rpc_address="$endpoint" ;;
  esac
  if [[ "$endpoint" != https://* && ( "$insecure" == true || "$insecure" == 1 ) ]]; then
    cluster_rpc_transport=(-plaintext)
  else
    if [[ ! -f "${SPACE_INTERNAL_RPC_TLS_CA:-}" || ! -r "$SPACE_INTERNAL_RPC_TLS_CA" ]]; then
      echo "SPACE_INTERNAL_RPC_TLS_CA must name a readable CA file" >&2
      return 1
    fi
    cluster_rpc_transport=(-cacert "$SPACE_INTERNAL_RPC_TLS_CA")
    if [[ -n "${SPACE_CLUSTER_API_TLS_SERVER_NAME:-}" ]]; then
      cluster_rpc_transport+=(-authority "$SPACE_CLUSTER_API_TLS_SERVER_NAME")
    fi
  fi
}

space_cluster_rpc() {
  local method="$1"
  shift
  grpcurl "${cluster_rpc_transport[@]}" "$@" "$cluster_rpc_address" "$method"
}
