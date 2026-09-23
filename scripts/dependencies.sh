#! /bin/bash

set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

if ! asdf plugin list | grep -qx nodejs; then
    asdf plugin add nodejs https://github.com/asdf-vm/asdf-nodejs.git
fi
if ! asdf plugin list | grep -qx devspace; then
    asdf plugin add devspace https://github.com/virtualstaticvoid/asdf-devspace.git
fi

asdf install

sudo apt install -y bats libvirt-dev pkg-config

PYTHON_VERSION="$(cat .python-version)"
pyenv install --skip-existing "$PYTHON_VERSION"
if ! pyenv virtualenvs --bare | grep -qx premiscale; then
    pyenv virtualenv "$PYTHON_VERSION" premiscale
fi
export PYENV_VERSION=premiscale
VIRTUAL_ENV="$(pyenv prefix)"
export VIRTUAL_ENV
export PATH="$VIRTUAL_ENV/bin:$PATH"
poetry install
