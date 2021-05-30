#!/bin/bash

set -euo pipefail; shopt -s nullglob

root=0 gls='(0, 145937515), (0, 83097223), (0, 115545038), (0, 69591295), (0, 110127892)' exec /opt/graphsurge/gs_computation.sh $*

