#!/usr/bin/env bash

source $HOME/go/src/P2-ljendrusch/sh/source.sh

echo "Starting Storage Nodes..."

for (( i = 0; i < nodes_len; i++ )); do
    node_i=$(( i % ${#nodes[@]} ))
    node=${nodes[${node_i}]}
    port=$(( 14000 + i ))

    ssh ${node} "$HOME/go/bin/storage_node ${data_dir}/${node}_${port} ${controller} ${cmp_manager} ${port}" &> "${log_dir}/${node}_${port}.log" &
done

echo "Done"
