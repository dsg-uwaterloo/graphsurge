#!/bin/bash

set -euo pipefail; shopt -s nullglob

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docker_dir=$(echo $PWD | sed "s|$HOME|/opt|")

mkdir -p $root/bin
exe=$root/bin/${1:-gs}
image=${2:-graphsurge}

docker run -ti --rm --name gsbuild -e "CARGO_HOME=/opt/.cargo" -v $HOME:/opt --user graphsurge:graphsurge $image bash -c "cd $docker_dir && cargo build --release" \
    && { [[ -f $exe ]] && mv -v $exe $exe.$(date -d @$(stat -c %Y $exe) +'%Y-%m-%d-%H-%M-%S'); cp -v target/release/graphsurge "$exe"; }
