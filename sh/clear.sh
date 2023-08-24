#!/usr/bin/env bash

source $HOME/go/src/P2-ljendrusch/sh/source.sh

echo "Clearing Storage Nodes..."

for (( i = 0; i < nodes_len; i++ )); do
    node_i=$(( i % ${#nodes[@]} ))
    node=${nodes[${node_i}]}
    port=$(( 14000 + i ))

    ssh ${node} "rm -rf /bigdata/students/ljendrusch/${node}_${port}"
done

echo "Done"
