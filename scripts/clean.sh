#! /bin/bash
# Clean up installed files.

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

python setup.py clean --all

##
# Clear all containers and unused images. Useful after you've just tested a Dockerfile build a bunch of times.
clear_containers() {
    # Clear containers.
    for c in $(docker ps -a | awk -v column=1 -f "$PROJECT_ROOT/pkg/premiscale/support/awk/column.awk" | tail -n +2); do
        docker rm "$c"
    done

    # Clear images.
    for im in $(docker images | awk -v column=3 -f "$PROJECT_ROOT/pkg/premiscale/support/awk/column.awk" | tail -n +2); do
        docker rmi "$im"
    done

    return 0
}

clear_containers

unset -f clear_containers
