#!/usr/bin/env bash

set -euo pipefail

# Git may inherit an unrelated active environment from a shell or editor.
if command -v pyenv > /dev/null 2>&1 && project_python_prefix="$(pyenv prefix premiscale 2> /dev/null)"; then
    exec "$project_python_prefix/bin/python" "$@"
fi

# CI and installations without pyenv use the project's Poetry environment.
exec poetry run python "$@"
