#!/usr/bin/env bash

source $HOME/go/src/P2-ljendrusch/sh/source.sh

cd "${proj_dir}"

echo "Installing..."
go build ./wire || exit 1
go build ./util || exit 1
go build ./cl || exit 1
go build ./cn || exit 1
go build ./sn || exit 1
go install ./client/client.go || exit 1
go install ./controller/controller.go || exit 1
go install ./cmp_manager/cmp_manager.go || exit 1
go install ./storage_node/storage_node.go || exit 1
echo "Done installing"

echo "Creating log directory: ${log_dir}"
mkdir -pv "${log_dir}"
