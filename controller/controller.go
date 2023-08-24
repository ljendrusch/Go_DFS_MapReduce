package main

import (
	"dfs/cn"
	"dfs/wire"
	"errors"
	"fmt"
	"log"
	"net"
)

func main() {
	s := cn.Init_server(wire.ServType_CNTRLR, cn.MAX_NODES)
	go s.Node_maintenance()

	listen_addr := fmt.Sprintf(":%d", 14980)
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

					res, err := wh.Receive()
					if err != nil {
						wh.Close()
						log.Println(err)
						continue
					}

					switch t := res.Msg.(type) {
					case *wire.Wrapper_St:
						fmt.Printf(" *** Requesting store %s...\n", t.St.Name)
						err = s.Store(wh, t.St)
						fmt.Printf(" *** Store done\n")

					case *wire.Wrapper_Rtq:
						fmt.Printf(" *** Requesting retrieve %s...\n", t.Rtq.Filename)
						err = s.Retrieve(wh, t.Rtq)
						fmt.Printf(" *** Retrieve done\n")

					case *wire.Wrapper_Dl:
						fmt.Printf(" *** Requesting delete %s...\n", t.Dl.Delete)
						err = s.Delete(wh, t.Dl)
						fmt.Printf(" *** Delete done\n")

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
