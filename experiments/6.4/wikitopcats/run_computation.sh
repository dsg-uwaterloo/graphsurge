#!/bin/bash

set -euo pipefail; shopt -s nullglob

root=0 gls='(0, 1463762), (0, 1516135), (0, 758332), (0, 1587794), (0, 922370)' exec /opt/graphsurge/experiments/gs_computation.sh $*

