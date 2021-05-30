#!/bin/bash

set -euo pipefail; shopt -s nullglob

rt=0 gl="()"
if [[ "$1" == "gb" ]];then
    shift
    root=$rt gls=$gl exec /home/s3sahu/graphsurge/gb_computation.sh $* 
else
    root=$rt gls=$gl exec /opt/graphsurge/gs_computation.sh $*
fi

