#!/bin/bash

set -euo pipefail; shopt -s nullglob

root=12 gls='()' exec /opt/graphsurge/gs_computation.sh $*

