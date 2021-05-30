#!/bin/bash

set -euo pipefail; shopt -s nullglob

root=0 gls='(0, 794448), (0, 1577931), (0, 2297501), (0, 2248241), (0, 1046952)' exec /opt/graphsurge/experiments/gs_computation.sh $*

