#!/usr/bin/env bash

source $HOME/go/src/P2-ljendrusch/sh/source.sh

echo "Starting Comp Manager..."
ssh "${cmp_manager}" "$HOME/go/bin/cmp_manager ${data_dir}" &> "${log_dir}/cmp_manager.log" &
