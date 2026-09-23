#!/usr/bin/env bash
# Manage the repository's local Minikube addon with stock Minikube and Helm.

set -euo pipefail

INTEGRATION_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$INTEGRATION_DIR/../.." && pwd)"
PROFILE="${PREMISCALE_MINIKUBE_PROFILE:-premiscale}"
NAMESPACE="${PREMISCALE_MINIKUBE_NAMESPACE:-premiscale}"
DRIVER="${PREMISCALE_MINIKUBE_DRIVER:-docker}"
KUBERNETES_VERSION=v1.35.0
TIMEOUT="${PREMISCALE_MINIKUBE_TIMEOUT:-10m}"
CONTROLLER_CONFIG="${PREMISCALE_MINIKUBE_CONFIG:-}"
CHART="$INTEGRATION_DIR/chart"
HELM_CACHE="$PROJECT_ROOT/.cache/minikube/helm"
IMAGE_REPOSITORY=premiscale/premiscale
IMAGE_TAG=minikube

usage() {
    cat << 'EOF'
Usage: integrations/minikube/minikube.sh COMMAND

  start    Start the profile, build/load the image, enable the addon, and smoke-test it.
  enable   Rebuild/load the image and install or upgrade the addon on a running profile.
  test     Check rollouts and exercise HTTP, gRPC, and Dragonfly inside the cluster.
  status   Show the profile and addon workloads.
  render   Build locked chart dependencies and print manifests without accessing a cluster.
  disable  Uninstall the addon, retaining its namespace, PVCs, and operator CRDs.
  stop     Stop this profile, preserving its workloads and storage.
  delete   Delete this profile and its local storage.

See integrations/minikube/README.md for prerequisites and environment settings.
EOF
}

fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

require() {
    local executable
    for executable in "$@"; do
        command -v "$executable" > /dev/null 2>&1 || fail "Executable '$executable' is required"
    done
}

kube() {
    kubectl --context "$PROFILE" --namespace "$NAMESPACE" "$@"
}

helm_local() {
    helm --repository-config "$HELM_CACHE/repositories.yaml" --repository-cache "$HELM_CACHE/repository-cache" "$@"
}

prepare_chart() {
    require helm
    if [[ -n "$CONTROLLER_CONFIG" ]]; then
        [[ -f "$CONTROLLER_CONFIG" ]] || fail "Controller config not found: $CONTROLLER_CONFIG"
    fi
    mkdir -p "$HELM_CACHE/repository-cache"
    helm_local repo add autoscaler https://kubernetes.github.io/autoscaler --force-update >&2
    helm_local repo add cnpg https://cloudnative-pg.github.io/charts --force-update >&2
    helm_local repo add kedacore https://kedacore.github.io/charts --force-update >&2
    helm_local repo add strimzi https://strimzi.io/charts/ --force-update >&2
    helm_local dependency build "$PROJECT_ROOT/charts/premiscale" --skip-refresh >&2
    helm_local dependency build "$CHART" --skip-refresh >&2
}

build_image() {
    local image_id base_image
    base_image="$IMAGE_REPOSITORY:minikube-$PROFILE"
    # Local content tags should not change solely because provenance has a new timestamp.
    docker buildx build --load --provenance=false --target develop --tag "$base_image" "$PROJECT_ROOT"
    image_id="$(docker image inspect --format '{{.Id}}' "$base_image")"
    [[ "$image_id" =~ ^sha256:[a-f0-9]{64}$ ]] || fail 'Docker returned an invalid image ID'
    IMAGE_TAG="local-${image_id#sha256:}"
    docker tag "$base_image" "$IMAGE_REPOSITORY:$IMAGE_TAG"
    minikube --profile "$PROFILE" image load --daemon "$IMAGE_REPOSITORY:$IMAGE_TAG"
}

chart_options() {
    CHART_OPTIONS=(
        --namespace "$NAMESPACE"
        --set-string "premiscale.deployment.image.tag=$IMAGE_TAG"
    )
    if [[ -n "$CONTROLLER_CONFIG" ]]; then
        CHART_OPTIONS+=(--set-file "premiscale.configMap.config=$CONTROLLER_CONFIG")
    fi
    if [[ -n "${PREMISCALE_MINIKUBE_VALUES:-}" ]]; then
        [[ -f "$PREMISCALE_MINIKUBE_VALUES" ]] || fail "Values file not found: $PREMISCALE_MINIKUBE_VALUES"
        CHART_OPTIONS+=(--values "$PREMISCALE_MINIKUBE_VALUES")
    fi
}

enable() {
    require minikube docker helm kubectl
    minikube --profile "$PROFILE" status
    prepare_chart
    build_image
    chart_options
    helm_local upgrade --install premiscale "$CHART" --kube-context "$PROFILE" \
        --create-namespace --wait --timeout "$TIMEOUT" "${CHART_OPTIONS[@]}"
}

smoke_test() {
    require minikube kubectl
    minikube --profile "$PROFILE" status
    kube rollout status statefulset/premiscale-dragonfly --timeout "$TIMEOUT"
    local deployment
    for deployment in premiscale premiscale-externalgrpc-cluster-autoscaler premiscale-cloudnative-pg; do
        kube rollout status "deployment/$deployment" --timeout "$TIMEOUT"
    done
    kube exec -i deployment/premiscale -- poetry run python - < "$INTEGRATION_DIR/smoke.py"
}

if [[ $# -ne 1 ]]; then
    usage >&2
    exit 2
fi
case "$1" in
    -h | --help | help)
        usage
        exit 0
        ;;
    start | enable | test | status | render | disable | stop | delete) ;;
    *)
        usage >&2
        exit 2
        ;;
esac

for name in "$PROFILE" "$NAMESPACE"; do
    [[ "$name" =~ ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$ && ${#name} -le 63 ]] || fail 'Profile and namespace must be DNS labels of at most 63 characters'
done

case "$1" in
    start)
        require minikube docker helm kubectl
        minikube --profile "$PROFILE" start --driver "$DRIVER" --keep-context \
            --kubernetes-version "$KUBERNETES_VERSION" --container-runtime containerd \
            --addons storage-provisioner,default-storageclass,metrics-server \
            --cpus "${PREMISCALE_MINIKUBE_CPUS:-4}" --memory "${PREMISCALE_MINIKUBE_MEMORY:-4096}" \
            --nodes "${PREMISCALE_MINIKUBE_NODES:-1}" --disk-size 30g --wait-timeout "$TIMEOUT"
        enable
        smoke_test
        ;;
    enable) enable ;;
    test) smoke_test ;;
    render)
        prepare_chart
        chart_options
        helm_local template premiscale "$CHART" --kube-version "$KUBERNETES_VERSION" --include-crds "${CHART_OPTIONS[@]}"
        ;;
    status)
        require minikube kubectl
        minikube --profile "$PROFILE" status
        kube get deployments,statefulsets,pods,services,pvc
        ;;
    disable)
        require minikube helm
        minikube --profile "$PROFILE" status
        helm uninstall premiscale --kube-context "$PROFILE" --namespace "$NAMESPACE" \
            --ignore-not-found --wait --timeout "$TIMEOUT"
        ;;
    stop | delete)
        require minikube
        minikube --profile "$PROFILE" "$1"
        ;;
esac
