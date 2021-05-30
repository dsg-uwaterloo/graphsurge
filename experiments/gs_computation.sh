#!/bin/bash

set -euo pipefail; shopt -s nullglob

#provide: root=70 ;gls='(1, 1283152), (1, 870029), (1, 771398), (1, 992134), (1, 17426)'

c="$1"
ctp="$2"
tp=${3:-manual}
sv="${4:-false}"
ex="${5:-gs}"
host_total=${6:-}
host=${7:-}

wc=${owc:-10}
itr=${oitr:-10}

[[ "${sv:-}" == "true" ]] && qprefix="save_results." || qprefix=""
if [[ -n "${host:-}" ]];then
    qf="h${host}.vcol_${tp}_order.${c}.${ctp}.txt"
    lg="${ex}.${host_total}.${qf}.${root}.${wc}.${itr}.log"
    qf="${host_total}/$qf"
else
    qf="${qprefix}vcol_${tp}_order.${c}.${ctp}.txt"
    lg="${ex}.${qf}.${root}.${wc}.${itr}.log"
fi

exe=/opt/graphsurge/bin/$ex
cm="$exe <(cat /opt/graphsurge/computation_queries/$qf | sed -r -e 's/ROOT/${root}/' -e 's/WEIGHT_CAP/${wc}/' -e 's/GOALS/${gls}/' -e 's/ITER/${itr}/')"

[[ -f "$lg" ]] && mv "$lg" "${lg}.$(date -d @$(stat -c %Y $lg) +'%Y-%m-%d-%H-%M-%S')"
touch "$lg" || { echo "Log file could not be created"; exit 1; }

ext=0
exec bash -c "exec $cm $PWD/$qf" 2>&1 | tee $lg || { echo "Command exited with error"; ext=1; }

echo -e "$cm\n$(hostname) : md5sum = $(md5sum $exe | cut -d' ' -f1) : mem_limit = $mem_limit" | tee -a $lg
echo "Logged to $lg"
exit $ext

