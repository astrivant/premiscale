#! /usr/bin/env bash
# Add a Helm repository.

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

REPOSITORY="${1:-helm}"
LOCAL_REFERENCE="${2:-premiscale}"

if grep -ioq "${LOCAL_REFERENCE}" <(helm repo list | tail -n +2 | awk -v column=1 -f "$PROJECT_ROOT/pkg/premiscale/support/awk/column.awk"); then
    helm repo remove "$LOCAL_REFERENCE"
fi

pass show premiscale/nexus/password | awk -f "$PROJECT_ROOT/pkg/premiscale/support/awk/nonempty.awk" | helm repo add "$LOCAL_REFERENCE" https://repo.ops.premiscale.com/repository/"$REPOSITORY"/ --username "$(pass show premiscale/nexus/username)" --password-stdin
