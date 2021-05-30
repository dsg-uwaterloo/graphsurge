#!/bin/bash

set -euo pipefail; shopt -s nullglob

gc=$1
n=$2
tp=manual

root_dir=/home/s3sahu
vc=vcol_${tp}_order
dd=vcol_data/${vc}_ascii
exe=${root_dir}/installations/graphbolt/apps/$gc

if [[ "$gc" == "PageRank" ]];then
    mi=${mi:-10}
    cmf="-maxIters $mi"
    lgf="${mi}"
elif [[ "$gc" == "SSSP" ]];then
    src=$root
    wc=${wc:-10}
    cmf="-source $src -weight_cap $wc"
    lgf="${src}.${wc}"
fi

cm="$exe -numberOfUpdateBatches $n -nEdges 100000000 -streamPath $dd -prefix fcube-${vc}-diff- $cmf $dd/initial.adj";
lg="gb.${gc}.${n}.${lgf}.${vc}.log"
[[ -f $lg ]] && mv $lg "${lg}.$(date -d @$(stat -c %Y $lg) +'%Y-%m-%d-%H-%M-%S')";
touch $lg || { echo "Log file could not be created"; exit 1; }

ext=0
bash -c "LD_PRELOAD=${root_dir}/installations/graphbolt/lib/mimalloc/out/release/libmimalloc.so $cm" 2>&1 | tee $lg || { echo "Command exited with error"; ext=1; }
echo -e "$cm\n$(hostname) : md5sum = $(md5sum $exe | cut -d' ' -f1)" | tee -a $lg
echo "Logged to $lg"
exit $ext

