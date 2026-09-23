#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
MODULE=pkg/premiscale/cluster-autoscaler
PYTHON_OUT="$MODULE/integrations/python/premiscale_cluster_autoscaler/protos"
GO_MODULE=github.com/premiscale/premiscale/cluster-autoscaler

# Install these pinned plugins into a user-owned bin directory beforehand:
# go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.8
# go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
python -m grpc_tools.protoc -I "$MODULE/proto" \
    --python_out="$PYTHON_OUT" --pyi_out="$PYTHON_OUT" --grpc_python_out="$PYTHON_OUT" \
    --go_out="$MODULE" --go_opt="module=$GO_MODULE" \
    --go_opt="Mexternalgrpc.proto=$GO_MODULE/protocol;protocol" \
    --go-grpc_out="$MODULE" --go-grpc_opt="module=$GO_MODULE" \
    --go-grpc_opt="Mexternalgrpc.proto=$GO_MODULE/protocol;protocol" \
    "$MODULE/proto/externalgrpc.proto" "$MODULE/proto/templates.proto"

python scripts/format-autoscaler-protos.py "$PYTHON_OUT"
