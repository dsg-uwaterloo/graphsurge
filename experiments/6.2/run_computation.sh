#!/bin/bash

set -euo pipefail; shopt -s nullglob

root=1 gls="(1, 1283152), (1, 870029), (1, 771398), (1, 992134), (1, 17426)" exec /opt/graphsurge/gs_computation.sh $*

