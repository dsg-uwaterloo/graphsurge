#!/bin/bash

homedir=/opt/graphsurge
gsexec=$homedir/bin/gs

mkdir -p graph/serde

$gsexec $homedir/csv_to_graph_queries/edges_vertices_comma.txt | tee load_graph.log

