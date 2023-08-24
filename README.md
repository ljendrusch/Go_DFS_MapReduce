# Project 2: MapReduce Distributed Computation / File System

## Usage
Open a terminal and run the following commands:
- mkdir -pv $HOME/go/src
- cd $HOME/go/src
- git clone https://github.com/usf-cs677-sp23/P2-a_guy.git
- $HOME/go/src/P2-a_guy/sh/go.sh

Details on go.sh and other shell scripts:
- Linking: 'go build' must be ran on all custom packages
    go build ./wire
    go build ./util
    go build ./cl
    go build ./cn
    go build ./sn
- Compiling: 'go install'
    go install ./client/client.go
    go install ./controller/controller.go
    go install ./cmp_manager/cmp_manager.go
    go install ./storage_node/storage_node.go
- Create log directory
    mkdir $HOME/p2/logs
- Start the controller
    ssh orion02 $HOME/go/bin/controller &> $HOME/p2/logs/controller.log &
- Start the computation manager
    ssh orion03 $HOME/go/bin/cmp_manager &> $HOME/p2/logs/cmp_manager.log &
- Make data directories and start storage nodes (for each, where node is name of machine, e.g. orion07, and port is a unique integer between 14000 and 14099, inclusive)
    ssh ${node} mkdir -pv /bigdata/students/${whoami}/${node}_${port}
    ssh ${node} "$HOME/go/bin/storage_node ${data_dir}/${node}_${port} ${controller} ${cmp_manager} ${port}" \
    &> $HOME/p2/logs/${node}_${port}.log &
- Start the client
    $HOME/go/bin/client orion02 orion03 ${data_dir}/p2

## System Features: Distributed File System
- Resilient HDFS-like Distributed File System
- Exhaustive logging, error handling, error recovery
- Maintains state on shutdown / restart
- Decentralized node and file registry maintenance
- Storage node failure recognition and recovery
- Data corruption recognition and recovery

## System Features: Distributed Computation System
- Modular MapReduce Distributed Computation System
- Utilizes Golang Plugins to define jobs
- Jobs are compiled by the system, not the client,
  so client needs no extra software
- Regular progress update communication
- Exhaustive logging and error handling
- Data corruption recognition and recovery

## Controller Features
- Maintains up to 100 storage nodes
- Graceful handling of storage node failure and new node entry
- Passively establishes connection with storage nodes
- Actively pings for updates from storage nodes
- Maintains a file / chunk registry, storage node status registry
- Updates file / chunk registry based on storage node messages
- Maintains a chunk replication level of 3

## Storage Node Features
- Actively establishes link with controller
- Passively updates controller on status
- Maintains map of chunks stored locally
- Recognizes chunk corruption and deletes respective chunk

## Client Program Features
- Simple command-line interface
- Interacts with the controller, computation manager,
  and storage nodes dynamically based on user command
- Commands:
  (flag -cm with list, activity, or info contacts the computation manager rather than the controller)
    mapreduce [job_plugin]
    store [file]
    retrieve [file]
    delete [file]
    list [-v] (list files stored in the system; list files and chunk locations with -v)
    activity (display active nodes)
    info [node_number] (display status of node with number [node_number])
    help (display usage)
    exit
