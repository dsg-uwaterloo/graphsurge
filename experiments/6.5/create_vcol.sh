#!/bin/bash

set -euo pipefail; shopt -s nullglob

a=$1
d=$2
i=$3
v=$4
ds="$5"
ctp=${6:-manual}
ex=${7:-gs}

serde=vcol_data/vcol_${ctp}_order_serde
vcol=vcol_${ctp}_order
ascii=vcol_data/vcol_${ctp}_order_ascii

root=$HOME/graphsurge/experiments
exe=${root}/bin/$ex
exe2=${root}/graphbolt/tools/converters/SNAPtoAdjConverter
cm="echo -e \"generate cube $vcol $i $a $d $v initial '$ds';\nserialize cube $vcol to '$serde' threads 10;\nsave cube $vcol to '$ascii' 10 true;\" | $exe | tee create_vcol.log && $exe2 $ascii/fcube-$vcol-diff-0.txt $ascii/initial.adj"
lg=vcol.log

ext=0
mkdir -p $serde $ascii
bash -c "$cm" 2>&1 | tee $lg || { echo "Command exited with error"; ext=1; }

echo -e "$cm\n$(hostname) : md5sum = $(md5sum $exe | cut -d' ' -f1) : md5sum2 = $(md5sum $exe2 | cut -d' ' -f1) : mem_limit = $mem_limit" | tee -a $lg
echo "Logged to $lg"
exit $ext
