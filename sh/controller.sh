#!/usr/bin/env bash

source $HOME/go/src/P2-ljendrusch/sh/source.sh

echo "Starting Controller..."
ssh "${controller}" "$HOME/go/bin/controller" &> "${log_dir}/controller.log" &
