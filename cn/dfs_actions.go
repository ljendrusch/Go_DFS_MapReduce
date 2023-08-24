package cn

import (
	"dfs/wire"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (s *Server) Store(wh *wire.WireHandler, st *wire.Store) error {
	filename, num_chunks, f_size, c_size := st.Name, st.NChunks, st.FSize, st.CSize

	if _, ok := s.Files_map[filename]; ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
			Message: fmt.Sprintf("file %s already exists in server; duplicate files not allowed\n", filename),
		}}})
		return fmt.Errorf("file %s already exists in server; duplicate files not allowed", filename)
	}

	fmt.Printf(" *** %s, %d chunks, filesize: %db, chunk size: %db\n", filename, num_chunks, f_size, c_size)

	sum_nodes_space := uint64(0)
	for _, ns := range s.Nodes {
		if ns != nil {
			sum_nodes_space += ns.Space
		}
	}

	fmt.Printf(" *** server space %20d\n *** file size    %20d\n", sum_nodes_space, f_size)
	if sum_nodes_space < uint64(f_size) {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
			Message: fmt.Sprintf("not enough space left on the server to store file %s\n  server space %20d\n  file size    %20d\n", filename, sum_nodes_space, f_size),
		}}})
		return fmt.Errorf("not enough space left on the server to store file %s\n  server space %20d\n  file size    %20d", filename, sum_nodes_space, f_size)
	}

	c_nodeports := make([]*wire.StringTripleCSize, num_chunks)
	nodes_by_space := make([]*Node_socket, len(s.Nodes))

	i := 0
	for _, ns := range s.Nodes {
		if ns != nil && ns.Space > 0 {
			nodes_by_space[i] = ns
			i++
		}
	}
	nodes_by_space = nodes_by_space[:i]
	fmt.Printf(" *** length of node space slice: %d\n", i)

	sort.SliceStable(nodes_by_space, func(i, j int) bool {
		return nodes_by_space[i].Space > nodes_by_space[j].Space
	})

	// if the two highest-space nodes have f_size more free space than any other node
	if (len(nodes_by_space) > 3) && ((nodes_by_space[1].Space - nodes_by_space[2].Space) >= (uint64(f_size))) {
		s3_a := [2]string{nodes_by_space[0].Name141, nodes_by_space[1].Name141}
		nodes_by_space = nodes_by_space[2:]
		num_nodes := len(nodes_by_space)
		step := num_nodes / 2
		for i = 0; i < int(num_chunks); i++ {
			c_nodeports[i] = &wire.StringTripleCSize{
				S1:    nodes_by_space[i%num_nodes].Name141,
				S2:    nodes_by_space[(i+step)%num_nodes].Name141,
				S3:    s3_a[i%2],
				CSize: 0,
			}
		}
	} else {
		num_nodes := len(nodes_by_space)
		step := num_nodes / 3
		for i = 0; i < int(num_chunks); i++ {
			c_nodeports[i] = &wire.StringTripleCSize{
				S1:    nodes_by_space[i%num_nodes].Name141,
				S2:    nodes_by_space[(i+step)%num_nodes].Name141,
				S3:    nodes_by_space[(i+(2*step))%num_nodes].Name141,
				CSize: 0,
			}
		}
	}

	fmt.Printf(" *** Sending chunkmap\n")
	for _, t := range c_nodeports {
		fmt.Printf("    %s, %s, %s\n", t.S1, t.S2, t.S3)
	}
	fmt.Println()

	return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Cnp{Cnp: &wire.ChunkNodePorts{
		Nodes: c_nodeports,
	}}})
}

func (s *Server) Retrieve(wh *wire.WireHandler, rtq *wire.RetrieveRq) error {
	chunk_v, ok := s.Files_map[rtq.Filename]
	if !ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: fmt.Sprintf("file '%s' not found in system", rtq.Filename)}}})
		return fmt.Errorf("file '%s' not found in system", rtq.Filename)
	}

	ft := chunk_v.F_type
	ports_arr := make([]*wire.StringTripleCSize, len(chunk_v.F_map))
	for c_idx, cl := range chunk_v.F_map {
		stc := wire.StringTripleCSize{
			S1: "",
			S2: "",
			S3: "",
		}

		if cl != nil {
			stc.CSize = cl.C_size

			i := 0
			for _, p := range cl.Ports {
				if p == "" {
					continue
				}

				if i == 0 {
					stc.S1 = p
				} else if i == 1 {
					stc.S2 = p
				} else {
					stc.S3 = p
				}
				i++

				if i == 3 {
					break
				}
			}
		}

		ports_arr[c_idx] = &stc
	}

	fmt.Printf(" *** Ports:\n")
	for i, p := range ports_arr {
		fmt.Printf("    [%d] %s %s %s\n", i, p.S1, p.S2, p.S3)
	}

	return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rtr{Rtr: &wire.RetrieveRs{
		FType:    ft,
		Retrieve: ports_arr,
	}}})
}

func (s *Server) Delete(wh *wire.WireHandler, dl *wire.Delete) error {
	filename := dl.Delete
	if _, ok := s.Files_map[filename]; !ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: fmt.Sprintf("filename '%s' not found", filename)}}})
		return fmt.Errorf("filename '%s' not found", filename)
	}

	for _, ns := range s.Nodes {
		if ns == nil {
			continue
		}

		go func(filename string, ns *Node_socket) {
			conn, l_err := net.Dial("tcp", ns.Name141)
			if l_err != nil {
				log.Println(l_err)
				return
			}

			l_wh := wire.Construct_wirehandler(conn)
			l_err = l_wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Dl{Dl: &wire.Delete{
				Delete: filename,
			}}})
			if l_err != nil {
				log.Println(l_err)
			}
		}(filename, ns)
	}

	delete(s.Files_map, filename)
	return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Dl{Dl: &wire.Delete{Delete: fmt.Sprintf("'%s' deleted", filename)}}})
}

func (s *Server) List(wh *wire.WireHandler, ls *wire.List) error {
	if len(s.Files_map) == 0 {
		return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ls{Ls: &wire.List{List: "  *No files in system*"}}})
	}

	fns := make([]string, len(s.Files_map))
	i := 0
	for k := range s.Files_map {
		fns[i] = k
		i++
	}

	sort.Strings(fns)

	var stb strings.Builder
	if ls.List == "false" {
		for _, fn := range fns {
			fmt.Fprintf(&stb, "\n    %s", fn)
		}
	} else {
		for _, fn := range fns {
			fmt.Fprintf(&stb, "\n%    s\n  ----------------\n  [", fn)

			_, ok := s.Files_map[fn]
			if ok && s.Files_map[fn] != nil {
				for ci, cls := range s.Files_map[fn].F_map {
					if cls != nil && cls.Ports != nil {
						fmt.Fprintf(&stb, "\n    %d :", ci)
						for _, p := range cls.Ports {
							fmt.Fprintf(&stb, "  %s", p)
						}
					}
				}
				fmt.Fprintf(&stb, "  ]\n")
			}
		}
	}

	fmt.Printf(" *** List files:\n%s\n\n", stb.String())

	return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ls{Ls: &wire.List{List: stb.String()}}})
}

func (s *Server) Info(wh *wire.WireHandler, info *wire.Info) error {
	node_number, err := strconv.ParseInt(info.Info, 10, 32)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: fmt.Sprintf("'%s' not a node number", info.Info)}}})
		return fmt.Errorf("'%s' not a node number", info.Info)
	}
	nn := int32(node_number)

	if nn < 0 || nn >= int32(len(s.Nodes)) {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: fmt.Sprintf("'%s' not a valid node number", info.Info)}}})
		return fmt.Errorf("'%s' not a valid node number", info.Info)
	}
	if s.Nodes[nn] == nil {
		return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_If{If: &wire.Info{Info: fmt.Sprintf("  *Node %d inactive", nn)}}})
	}

	ns := s.Nodes[nn]

	var stb strings.Builder
	fmt.Fprintf(&stb, "\n  Node %d -- %s", nn, ns.Name141)
	fmt.Fprintf(&stb, "\n    free space: %dMiB", ns.Space/1048576)
	hb_elapsed := time.Now().UnixMilli() - ns.Hb_epoch
	fmt.Fprintf(&stb, "\n    last update %2d.%ds ago", hb_elapsed/1000, (hb_elapsed/100)%10)
	if ns.Busy_epoch > 0 {
		busy_elapsed := time.Now().UnixMilli() - ns.Busy_epoch
		fmt.Fprintf(&stb, "\n    busy for %d.%ds", busy_elapsed/1000, (busy_elapsed/100)%10)
	} else {
		fmt.Fprintf(&stb, "\n    currently not busy")
	}
	fmt.Fprintf(&stb, "\n    requests served: %d", ns.Served)

	if len(ns.Chunkmap) == 0 {
		fmt.Fprintf(&stb, "\n  No files stored")
	} else {
		fmt.Fprintf(&stb, "\n  File Chunk Repository:")
		for fn, cm := range ns.Chunkmap {
			if len(cm.CMap) < 1 {
				delete(ns.Chunkmap, fn)
				continue
			}

			first := true
			for ci := range cm.CMap {
				if first {
					fmt.Fprintf(&stb, "\n    filename: %s -- chunk(s) [ %d", fn, ci)
					first = false
				} else {
					fmt.Fprintf(&stb, ", %d", ci)
				}
			}
			fmt.Fprintf(&stb, " ]")
		}
	}

	fmt.Printf(" *** info call output\n%s\n\n", stb.String())
	return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_If{If: &wire.Info{Info: stb.String()}}})
}

// print out a list of active nodes
// total disk space available in the cluster (in GB),
// number of requests handled by each node
func (s *Server) Activity(wh *wire.WireHandler, ac *wire.Activity) error {
	total_space := uint64(0)
	nss := make([]*Node_socket, len(s.Nodes))

	var stb strings.Builder
	fmt.Fprint(&stb, "  Active Nodes:\n    ")
	i := 0
	for _, ns := range s.Nodes {
		if ns == nil {
			continue
		}

		total_space += ns.Space
		nss[i] = ns

		if i > 0 && (i%5) == 0 {
			fmt.Fprintf(&stb, "\n    ")
		}
		fmt.Fprintf(&stb, "%s  ", ns.Name141)
		i++
	}
	nss = nss[:i]

	gibs := total_space * 100 / 1073741824
	fmt.Fprintf(&stb, "\n\n  Available disk space:  %d.%02d GiB\n\n  Number of Requests Serviced per Node:", gibs/100, gibs%100)

	// sort.SliceStable(nss, func(i, j int) bool {
	// 	return nss[i].Served > nss[j].Served
	// })

	for j, ns := range nss {
		fmt.Fprintf(&stb, "\n    node %d -- %s, %d requests served", j, ns.Name141, ns.Served)
	}

	fmt.Printf(" *** Activity report:\n%s\n\n", stb.String())

	return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ac{Ac: &wire.Activity{Activity: stb.String()}}})
}
