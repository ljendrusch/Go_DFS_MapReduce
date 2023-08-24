package cn

import (
	"dfs/wire"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MAX_NODES int32 = 100

type Server struct {
	SType     wire.ServType
	Files_map map[string]*File_map // filename to filetype and array of chunk indeces to node_port (name:141xx) strings to chunk size
	Nodes     []*Node_socket
}

type File_map struct {
	F_type wire.FileType
	F_map  []*Chunk_loc
}

type Chunk_loc struct {
	C_size int64
	Ports  []string
}

type Node_socket struct {
	Name140     string // node:140xx
	Name141     string // node:141xx
	Name142     string // node:142xx
	Port_suffix int32
	Served      int32
	Space       uint64
	Hb_epoch    int64
	Busy_epoch  int64
	Chunkmap    map[string]*wire.ChunkMap // filename to file type + chunk indeces to chunk size
}

func Init_server(st wire.ServType, max_nodes int32) *Server {
	return &Server{
		SType:     st,
		Files_map: make(map[string]*File_map),
		Nodes:     make([]*Node_socket, max_nodes),
	}
}

func (s *Server) Node_maintenance() {
	for node_number := int32(0); node_number < int32(len(s.Nodes)); node_number++ {
		go s.node_socket_listen(node_number)
	}

	for {
		s.stethoscope()             // dial !nil node sockets, parse Heartbeats
		s.flatline_check()          // kill nodes as needed, delete their node_socket
		s.update_filesmap()         // add values to filesmap based on recent heartbeats
		s.check_replication_level() // do replicate reqs for all chunks that are on less than 3 nodes
		s.report()                  // print server info to log

		time.Sleep(5 * time.Second)
	}
}

func (s *Server) node_socket_listen(node_number int32) {
	var listen_addr string
	if s.SType == wire.ServType_CNTRLR {
		listen_addr = fmt.Sprintf(":%d", 14000+node_number)
	} else {
		listen_addr = fmt.Sprintf(":%d", 14200+node_number)
	}

	var listener net.Listener
	var err error
	for {
		if listener != nil {
			listener.Close()
		}

		if listener, err = net.Listen("tcp", listen_addr); err == nil {
			defer listener.Close()

			for {
				if conn, err := listener.Accept(); err == nil {
					wh := wire.Construct_wirehandler(conn)

					res, err := wh.Receive()
					if err != nil {
						log.Println(err)
						wh.Close()
						continue
					}

					switch t := res.Msg.(type) {
					case *wire.Wrapper_Df:
						s.Nodes[node_number] = &Node_socket{
							Name140:     fmt.Sprintf("%s:%d", t.Df.Name, 14000+node_number),
							Name141:     fmt.Sprintf("%s:%d", t.Df.Name, 14100+node_number),
							Name142:     fmt.Sprintf("%s:%d", t.Df.Name, 14200+node_number),
							Port_suffix: node_number,
							Chunkmap:    make(map[string]*wire.ChunkMap),
						}
						fmt.Printf("***** opened ns %s%s\n", t.Df.Name, listen_addr)
						wh.Close()
						return
					}
					wh.Close()
				}
			}
		}
	}
}

// dial !nil node sockets, parse Heartbeats
func (s *Server) stethoscope() {
	var wg sync.WaitGroup
	for node_number, ns := range s.Nodes {
		if ns == nil {
			continue
		}

		wg.Add(1)
		go func(nn int, ns *Node_socket, stype wire.ServType) {
			defer wg.Done()

			var dial_addr string
			if stype == wire.ServType_CNTRLR {
				dial_addr = ns.Name140
			} else {
				dial_addr = ns.Name142
			}

			conn, err := net.DialTimeout("tcp", dial_addr, 4*time.Second)
			if err != nil {
				if conn != nil {
					conn.Close()
				}
				log.Println(err)
				return
			}

			wh := wire.Construct_wirehandler(conn)
			defer wh.Close()

			res, err := wh.Receive()
			if err != nil {
				log.Println(err)
				return
			}

			switch t := res.Msg.(type) {
			case *wire.Wrapper_Hb:
				var busy_epoch int64
				if t.Hb.Busy < 1 {
					busy_epoch = 0
				} else {
					busy_epoch = time.Now().UnixMilli() - t.Hb.Busy
				}

				ns.Space = t.Hb.Space
				ns.Served = t.Hb.Served
				ns.Hb_epoch = time.Now().UnixMilli()
				ns.Busy_epoch = busy_epoch
				ns.Chunkmap = t.Hb.ChunkMap
			}
		}(node_number, ns, s.SType)
	}
	wg.Wait()
}

func (s *Server) flatline_check() {
	for nn, ns := range s.Nodes {
		if ns == nil {
			continue
		}

		elapsed := time.Now().UnixMilli() - ns.Hb_epoch
		if elapsed > 22000 {
			s.Nodes[nn] = nil
			go s.node_socket_listen(int32(nn))
		}
	}
}

func (s *Server) update_filesmap() {
	t_filesmap := make(map[string]*File_map)
	for _, ns := range s.Nodes {
		if ns == nil {
			continue
		}

		for fn, cis := range ns.Chunkmap {
			ft := cis.FType
			for ci, c_size := range cis.CMap {
				if _, ok := t_filesmap[fn]; !ok {
					t_filesmap[fn] = &File_map{
						F_type: ft,
						F_map:  make([]*Chunk_loc, ci+1),
					}
					t_filesmap[fn].F_map[ci] = &Chunk_loc{
						C_size: c_size,
						Ports:  make([]string, 0, 3),
					}
				} else if dif := (ci + 1) - int32(len(t_filesmap[fn].F_map)); dif > 0 {
					for i := int32(1); i <= dif; i++ {
						if i == dif {
							t_filesmap[fn].F_map = append(t_filesmap[fn].F_map, &Chunk_loc{
								C_size: c_size,
								Ports:  make([]string, 0, 3),
							})
						} else {
							t_filesmap[fn].F_map = append(t_filesmap[fn].F_map, nil)
						}
					}
				} else if t_filesmap[fn].F_map[ci] == nil {
					t_filesmap[fn].F_map[ci] = &Chunk_loc{
						C_size: c_size,
						Ports:  make([]string, 0, 3),
					}
				}

				t_filesmap[fn].F_map[ci].Ports = append(t_filesmap[fn].F_map[ci].Ports, ns.Name141)
			}
		}
	}

	s.Files_map = t_filesmap
}

func (s *Server) check_replication_level() {
	if s.SType != wire.ServType_CNTRLR {
		return
	}

	var wg sync.WaitGroup
	wait_queue := 0
	// wait_queue := make(chan bool, 10)
	for fn, cis := range s.Files_map {
		if cis == nil {
			continue
		}

		if wait_queue >= 16 {
			wait_queue = 0
			wg.Wait()
		}

		ft := cis.F_type
		for ci, pr := range cis.F_map {

			if pr == nil {
				continue
			}

			ports := pr.Ports

			if len(ports) >= 3 {
				continue
			}

			if len(ports) == 0 {
				log.Printf("___ERROR no nodes contain %s_%d___\n", fn, ci)
				continue
			}

			wg.Add(1)
			wait_queue++
			// wait_queue <- true

			i := int64(0)
			nss := make([]*Node_socket, len(s.Nodes))
			for _, ns := range s.Nodes {
				if ns != nil {
					nss[i] = ns
					i++
				}
			}
			step := i / 3

			if len(ports) == 1 {
				var src_port string
				var src_port_n int64
				for _, p := range ports {
					src_port = p
					src_port_n, _ = strconv.ParseInt(p[strings.LastIndex(p, ":")+1:], 10, 64)
				}

				port1 := nss[(src_port_n+step)%i].Name141
				port2 := nss[(src_port_n+(2*step))%i].Name141

				go func(fn string, ft wire.FileType, ci int, src_port string, dest_ports ...string) {
					defer wg.Done()
					// defer func() {
					// 	<-wait_queue
					// }()

					err := s.replicate_chunk(fn, ft, int32(ci), src_port, dest_ports...)
					if err != nil {
						log.Printf("___FAILED to replicate chunk %s_%d from lone node %s___\n", fn, ci, src_port)
					}
				}(fn, ft, ci, src_port, port1, port2)
				continue
			} else {
				first := true
				var node1idx, node2idx int64
				for _, p := range ports {
					if first {
						first = false
						node1idx, _ = strconv.ParseInt(p[strings.LastIndex(p, ":")+1:], 10, 64)
					} else {
						node2idx, _ = strconv.ParseInt(p[strings.LastIndex(p, ":")+1:], 10, 64)
						break
					}
				}

				dest_port := nss[(node1idx+((2*(node2idx-node1idx))%i))%i].Name141

				go func(dest_port string, fn string, ft wire.FileType, ci int) {
					defer wg.Done()
					// defer func() {
					// 	<-wait_queue
					// }()

					fails := 0
					cap := len(s.Files_map[fn].F_map[ci].Ports)
					for _, port := range s.Files_map[fn].F_map[ci].Ports {
						err := s.replicate_chunk(fn, ft, int32(ci), port, dest_port)
						if err != nil {
							fails++
						} else {
							break
						}
					}

					if fails == cap {
						log.Printf("___FAILED to replicate chunk %s_%d to 3rd node___\n", fn, ci)
					}
				}(dest_port, fn, ft, ci)
			}
		}
	}

	wg.Wait()
}

func (s *Server) replicate_chunk(fn string, ft wire.FileType, ci int32, src_port string, dest_ports ...string) error {
	conn, err := net.DialTimeout("tcp", src_port, time.Second*2)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		log.Println(err)
		return err
	}
	wh := wire.Construct_wirehandler(conn)

	err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Crp{Crp: &wire.ChunkReplicate{
		ChunkId: &wire.ChunkId{
			FName: fn,
			FType: ft,
			CIdx:  ci,
			CSize: int64(0),
		},
		Nodes: dest_ports,
	}}})
	if err != nil {
		wh.Close()
		log.Println(err)
		return err
	}

	res, err := wh.Receive()
	if err != nil {
		wh.Close()
		log.Println(err)
		return err
	}

	switch res.Msg.(type) {
	case *wire.Wrapper_Ms:
		return nil
	case *wire.Wrapper_Rp:
		return errors.New("")
	default:
		log.Println(errors.New("malformed response from server"))
		return err
	}
}

func (s *Server) report() {
	alive, busy := 0, 0

	for _, ns := range s.Nodes {
		if ns == nil {
			continue
		}

		alive++

		if ns.Busy_epoch > 0 {
			busy++
		}
	}

	log.Printf("\n")
	fmt.Printf("\n  --------------------------------------------------------------------\n")
	fmt.Printf(" %3d Alive Nodes ", alive)
	fmt.Printf(" %3d Busy Nodes\n  Alive:", busy)
	for _, ns := range s.Nodes {
		if ns != nil {
			elapsed := time.Now().UnixMilli() - ns.Hb_epoch
			fmt.Printf("\n    node%02d -- %2d.%ds -- %dmib", ns.Port_suffix, elapsed/1000, (elapsed/100)%10, ns.Space/1048576)
		}
	}
	if busy > 0 {
		fmt.Printf("  Busy:\n")
		for _, ns := range s.Nodes {
			if ns != nil && ns.Busy_epoch > 0 {
				elapsed := time.Now().UnixMilli() - ns.Busy_epoch
				fmt.Printf("    node%02d -- %2d.%ds  ", ns.Port_suffix, elapsed/1000, (elapsed/100)%10)
			}
		}
	}
	fmt.Print("\n  --------------------------------------------------------------------\n\n")
}
