#!/usr/bin/env bash
# Creates a kind cluster with Envoy Gateway as the Gateway API implementation.
set -euo pipefail
CLUSTER="${1:-modelgate}"
# v1.8.0 or later: earlier releases reject GRPCRoute RequestMirror filters
# (envoyproxy/gateway#8541), which shadow mode needs for gRPC models.
EG_VERSION="${EG_VERSION:-v1.9.3}"

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"

# Envoy Gateway's chart installs the Gateway API CRDs, including the
# experimental RequestMirror percent field used for shadow sampling.
helm upgrade --install eg oci://docker.io/envoyproxy/gateway-helm --version "$EG_VERSION" \
  -n envoy-gateway-system --create-namespace --wait

kubectl apply -f - <<YAML
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: eg
spec:
  controllerName: gateway.envoyproxy.io/gatewayclass-controller
YAML
echo "cluster $CLUSTER ready: next run 'make kind-load deploy demo'"
