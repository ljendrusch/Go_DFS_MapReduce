package main

import (
	"dfs/sn"
	"dfs/wire"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 5 {
		log.Printf("Insufficient command-line arguments\n  Needed:   ./storage_node [storage_directory] [controller_hostname] [cmp_manager_hostname] [port]\n  Example:  ./storage_node /data/dir orion02 orion03 14003\n  Provided: %s\n", os.Args)
		os.Exit(1)
	}

	s_node := sn.Init_sn(os.Args)
	go s_node.HeartCtrl()
	go s_node.HeartCmMn()

	listen_addr := fmt.Sprintf(":%d", 14100+s_node.Port_suffix)
	var listener net.Listener
	var err error
	for {
		if listener != nil {
			listener.Close()
		}

		if listener, err = net.Listen("tcp", listen_addr); err == nil {
			for {
				if conn, err := listener.Accept(); err == nil {
					wh := wire.Construct_wirehandler(conn)

					if s_node.Busy_epoch < 1 {
						s_node.Busy_epoch = time.Now().UnixMilli()
					}

					go func(l_sn *sn.Storage_node, l_wh *wire.WireHandler) {
						defer l_wh.Close()

						res, err := l_wh.Receive()
						if err != nil {
							log.Println(err)
							return
						}

						switch t := res.Msg.(type) {
						case *wire.Wrapper_Cul:
							fmt.Printf("Upload %s_%s%d...\n", t.Cul.ChunkId.FName, t.Cul.ChunkId.FType, t.Cul.ChunkId.CIdx)
							err = l_sn.Handle_chunk_upload(l_wh, t.Cul)
							fmt.Printf("Upload done\n")

						case *wire.Wrapper_Cdl:
							fmt.Printf("Download %s_%s%d...\n", t.Cdl.ChunkId.FName, t.Cdl.ChunkId.FType, t.Cdl.ChunkId.CIdx)
							err = l_sn.Handle_chunk_download(l_wh, t.Cdl)
							fmt.Printf("Download done\n")

						case *wire.Wrapper_Crp:
							fmt.Printf("Replicate %s_%s%d...\n", t.Crp.ChunkId.FName, t.Crp.ChunkId.FType, t.Crp.ChunkId.CIdx)
							err = l_sn.Handle_chunk_replicate(l_wh, t.Crp)
							fmt.Printf("Replicate done\n")

						case *wire.Wrapper_Dl:
							fmt.Printf("Delete %s...\n", t.Dl.Delete)
							err = l_sn.Handle_file_delete(l_wh, t.Dl)
							fmt.Printf("Delete done\n")

						case *wire.Wrapper_Mrm:
							fmt.Printf("Map job %s, mapper %d...\n", t.Mrm.JobName, t.Mrm.MId)
							err = l_sn.Handle_map_req(l_wh, t.Mrm)
							fmt.Printf("Map done\n")

						case *wire.Wrapper_Mrmr:
							fmt.Printf("Map result job %s, mapper %d...\n", t.Mrmr.JobName, t.Mrmr.MId)
							err = l_sn.Handle_map_result(l_wh, t.Mrmr)
							fmt.Printf("Map result done\n")

						case *wire.Wrapper_Mrr:
							fmt.Printf("Reduce %s...\n", t.Mrr.JobName)
							err = l_sn.Handle_reduce_req(l_wh, t.Mrr)
							fmt.Printf("Reduce done\n")

						case *wire.Wrapper_Mrrr:
							fmt.Printf("Reduce result %s...\n", t.Mrrr.JobName)
							err = l_sn.Handle_reduce_result(l_wh, t.Mrrr)
							fmt.Printf("Reduce result done\n")

						default:
							log.Println(errors.New("malformed request"))
						}

						if err != nil {
							log.Println(err)
						} else {
							l_sn.Served++
						}
						fmt.Println()

						wh.Close()
					}(s_node, wh)

					s_node.Busy_epoch = 0
				}
			}
		}
	}
}
