#!/usr/bin/env bash

source $HOME/go/src/P2-ljendrusch/sh/source.sh

echo "Copying example plugins..."

mkdir ${data_dir}/p2/job_plugins
cp -r ${proj_dir}/example_plugins/* ${data_dir}/p2/job_plugins

echo "Done"
