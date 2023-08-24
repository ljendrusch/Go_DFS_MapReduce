package main

import (
	"dfs/cn"
	"dfs/wire"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path"
)

func main() {
	if len(os.Args) < 2 {
		log.Printf("Insufficient command-line arguments\n  Needed:   ./cmp_manager [data_dir]\n  Example:  ./cmp_manager /bigdata/students/ljendrusch\n  Provided: %s\n", os.Args)
		os.Exit(1)
	}

	JOBS_MAP := make(map[string]*cn.Job_map)
	DIR := path.Clean(os.Args[1])

	err := os.MkdirAll(path.Join(DIR, "job_plugins"), 0755)
	if err != nil {
		log.Fatalln(err)
	}

	s := cn.Init_server(wire.ServType_CMPMGR, cn.MAX_NODES)
	go s.Node_maintenance()

	listen_addr := fmt.Sprintf(":%d", 14990)
	var listener net.Listener
	for {
		if listener != nil {
			listener.Close()
		}

		if listener, err = net.Listen("tcp", listen_addr); err == nil {
			for {
				if conn, err := listener.Accept(); err == nil {
					wh := wire.Construct_wirehandler(conn)

					res, err := wh.Receive()
					if err != nil {
						wh.Close()
						log.Println(err)
						continue
					}

					switch t := res.Msg.(type) {
					case *wire.Wrapper_Mr:
						fmt.Printf(" *** Starting mapreduce job %s...\n", t.Mr.JobName)
						err = s.Map_reduce(wh, DIR, JOBS_MAP, t.Mr)
						fmt.Printf(" *** List files done\n")

					case *wire.Wrapper_Ls:
						fmt.Printf(" *** Requesting list files...\n")
						err = s.List(wh, t.Ls)
						fmt.Printf(" *** List files done\n")

					case *wire.Wrapper_If:
						fmt.Printf(" *** Requesting info on %s...\n", t.If.Info)
						err = s.Info(wh, t.If)
						fmt.Printf(" *** File info done\n")

					case *wire.Wrapper_Ac:
						fmt.Printf(" *** Requesting activity report...\n")
						err = s.Activity(wh, t.Ac)
						fmt.Printf(" *** Activity report done\n")

					default:
						log.Println(errors.New("***unknown request***"))
						err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
							Message: "unknown request",
						}}})
					}

					if err != nil {
						log.Println(err)
					}
					fmt.Println()

					wh.Close()
				}
			}
		}
	}
}
