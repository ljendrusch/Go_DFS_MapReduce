#!/usr/bin/env bash

source $HOME/go/src/P2-ljendrusch/sh/source.sh

# link, compile, install
${proj_dir}/sh/install.sh

# start controller node
${proj_dir}/sh/controller.sh

# start cmp_manager node
${proj_dir}/sh/cmp_manager.sh

# start storage nodes
${proj_dir}/sh/node_cluster.sh

# start client
${proj_dir}/sh/client.sh
