#!/usr/bin/env bash

source $HOME/go/src/P2-ljendrusch/sh/source.sh

echo "Stopping controller..."
ssh "${controller}" 'pkill -u "$(whoami)" controller'

echo "Stopping cmp_manager..."
ssh "${cmp_manager}" 'pkill -u "$(whoami)" cmp_manager'

echo "Stopping Storage Nodes..."
for node in ${nodes[@]}; do
    echo "${node}"
    ssh "${node}" 'pkill -u "$(whoami)" storage_node'
done

echo "Done"
