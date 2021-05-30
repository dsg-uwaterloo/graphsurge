#!/bin/bash

set -euo pipefail; shopt -s nullglob

docker_dir=$(echo $PWD | sed "s|$HOME|/opt|")
hostname=$(hostname -s)
[[ "$hostname" == "himrod-big-1" || "$hostname" == "himrod-big-2"  ]] && mem=480G || mem=245G
if [[ "$1" == "image" ]];then
    image=$2
    shift
    shift
else
    image=graphsurge
fi

docker run --rm --name gsrun --memory $mem --network host --hostname $hostname \
    --add-host h0:192.168.224.112 \
    --add-host h1:192.168.224.113 \
    --add-host h2:192.168.224.114 \
    --add-host h3:192.168.224.115 \
    --add-host h4:192.168.224.118 \
    --add-host h5:192.168.224.103 \
    --add-host h6:192.168.224.104 \
    --add-host h7:192.168.224.106 \
    --add-host h8:192.168.224.107 \
    --add-host h9:192.168.224.108 \
    --add-host h10:192.168.224.110 \
    --add-host h11:192.168.224.202 \
    -v $HOME:/opt --user graphsurge:graphsurge $image bash -c "cd $docker_dir && docker_log='mem_limit=$mem | image = $image' $*"
