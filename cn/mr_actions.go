package cn

import (
	"bytes"
	"dfs/util"
	"dfs/wire"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"plugin"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Job_map struct {
	m, r   int32 // num mappers, num reducers
	mc, rc int32 // num mappers / reducers completed (fails and successes summed)
	// mcc, rcc chan bool // mapper / reducer completion blocking mechanisms
	mf, rf []string  // addr of mappers / result_filename reducers that failed
	rs     []*string // addrs of reducers that succeeded, in order
}

func min3(m_map map[string][]string, addrs []string) string {
	if len(addrs) == 1 {
		return addrs[0]
	} else if len(addrs) == 2 {
		if len(m_map[addrs[0]]) <= len(m_map[addrs[1]]) {
			return addrs[0]
		} else {
			return addrs[1]
		}
	} else {
		a := len(m_map[addrs[0]])
		b := len(m_map[addrs[1]])
		c := len(m_map[addrs[2]])

		if a <= b {
			if a <= c {
				return addrs[0]
			}
		} else if b <= c {
			return addrs[1]
		}
		return addrs[2]
	}
}

func (s *Server) Map_reduce(wh *wire.WireHandler, dir string, jobs_map map[string]*Job_map, mrr *wire.MRRequest) error {
	// dir = /bigdata/students/ljendrusch
	if _, ok := jobs_map[mrr.JobName]; ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: fmt.Sprintf("*** there's already a job named %s running", mrr.JobName)}}})
		return fmt.Errorf("there's already a job named %s running", mrr.JobName)
	}

	job_name, all_files, file_names := mrr.JobName, mrr.FileNames == nil || len(mrr.FileNames) == 0, mrr.FileNames
	plugin_bytes, sent_checksum := mrr.PluginGo, mrr.Checksum

	job_map := &Job_map{}
	jobs_map[job_name] = job_map
	defer delete(jobs_map, job_name)

	var filename_map map[string]bool
	if !all_files {
		missing_fns := make([]string, 0, 16)
		for _, fn := range file_names {
			if _, ok := s.Files_map[fn]; !ok {
				missing_fns = append(missing_fns, fn)
			}
		}
		if len(missing_fns) > 0 {
			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: fmt.Sprintf("*** file(s) '%s' not in system", strings.Join(missing_fns, "', '"))}}})
			return fmt.Errorf("file(s) '%s' not in system", strings.Join(missing_fns, "', '"))
		}

		filename_map = make(map[string]bool)
		for _, fn := range file_names {
			filename_map[fn] = true
		}
	}

	err := os.MkdirAll(path.Join(dir, "job_plugins", job_name), 0755)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: err.Error()}}})
		return err
	}

	path_go := path.Join(dir, "job_plugins", job_name, fmt.Sprintf("%s.go", job_name))
	path_so := path.Join(dir, "job_plugins", job_name, fmt.Sprintf("%s.so", job_name))

	f_go, err := os.OpenFile(path_go, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		if f_go != nil {
			f_go.Close()
		}
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: err.Error()}}})
		return err
	}

	calcd_checksum, err := util.WriteAndHash(bytes.NewReader(plugin_bytes), int64(len(plugin_bytes)), f_go)
	f_go.Close()
	if err != nil {
		os.Remove(path_go)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: err.Error()}}})
		return err
	}

	err = util.VerifyChecksum(sent_checksum, calcd_checksum)
	if err != nil {
		os.Remove(path_go)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: err.Error()}}})
		return err
	}

	args := strings.Fields(fmt.Sprintf("go build -buildmode=plugin -o %s %s", path_so, path_go))

	cmd := exec.Command(args[0], args[1:]...)
	var errb bytes.Buffer
	cmd.Stderr = &errb
	err = cmd.Run()
	if err != nil {
		os.Remove(path_go)
		os.Remove(path_so)
		if len(errb.String()) > 0 {
			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: fmt.Sprintf("*** error(s) found in plugin file %s:\n***   %s", path_go, errb.String())}}})
			return fmt.Errorf("*** error(s) found in plugin file %s:\n***   %s", path_go, errb.String())
		} else {
			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: err.Error()}}})
			return err
		}
	}

	plg, err := plugin.Open(path_so)
	if err != nil {
		os.Remove(path_go)
		os.Remove(path_so)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: err.Error()}}})
		return err
	}

	r_a, err := plg.Lookup("R")
	if err != nil {
		os.Remove(path_go)
		os.Remove(path_so)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: "*** plugin file missing an 'R' int value"}}})
		return err
	}

	_, err = plg.Lookup("F_map")
	if err != nil {
		os.Remove(path_go)
		os.Remove(path_so)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: "*** plugin file missing an 'F_map()' function"}}})
		return err
	}

	_, err = plg.Lookup("F_reduce")
	if err != nil {
		os.Remove(path_go)
		os.Remove(path_so)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: "*** plugin file missing an 'F_reduce()' function"}}})
		return err
	}

	f_so, err := os.OpenFile(path_so, os.O_RDONLY, 0777)
	if err != nil {
		if f_so != nil {
			f_so.Close()
		}
		os.Remove(path_go)
		os.Remove(path_so)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: err.Error()}}})
		return err
	}
	defer f_so.Close()

	so_stat, err := f_so.Stat()
	if err != nil {
		os.Remove(path_go)
		os.Remove(path_so)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: err.Error()}}})
		return err
	}

	so_buf := bytes.NewBuffer(make([]byte, 0, so_stat.Size()))
	so_checksum, err := util.WriteAndHash(f_so, so_stat.Size(), so_buf)
	if err != nil {
		os.Remove(path_go)
		os.Remove(path_so)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: err.Error()}}})
		return err
	}

	r := *r_a.(*int)
	var nss []*Node_socket
	for _, ns := range s.Nodes {
		if ns != nil {
			nss = append(nss, ns)
		}
	}
	m := len(nss)

	job_map.m = int32(m)
	job_map.r = int32(r)
	job_map.mc = int32(0)
	job_map.rc = int32(0)
	// job_map.mcc = make(chan bool, m)
	// job_map.rcc = make(chan bool, r)
	job_map.rs = make([]*string, r)

	m_map := make(map[string][]string, m)
	for _, ns := range nss {
		m_map[ns.Name141] = make([]string, 0, 32)
	}

	for fn, f_map := range s.Files_map {
		if f_map == nil || f_map.F_type != wire.FileType_TXT || f_map.F_map == nil || len(f_map.F_map) == 0 {
			continue
		}

		if !all_files {
			if _, ok := filename_map[fn]; !ok {
				continue
			}
		}

		for ci, c_loc := range f_map.F_map {
			if c_loc == nil || len(c_loc.Ports) == 0 {
				continue
			}

			min_idx := min3(m_map, c_loc.Ports)
			m_map[min_idx] = append(m_map[min_idx], fmt.Sprintf("%s_t%d", fn, ci))
		}
	}

	sort.Slice(nss, func(i, j int) bool {
		return len(m_map[nss[i].Name141]) > len(m_map[nss[j].Name141])
	})

	r_map := make([]string, r)
	for i := 0; i < r; i++ {
		r_map[i] = nss[i%m].Name141
	}

	var fin sync.WaitGroup
	fin.Add(1)
	go update_client(wh, &fin, job_name, job_map)

	// order of sending: mappers, reducers, client
	// wh.Send(wire.MRMapReq) to mappers
	// mappers wh.Send(wire.MR?) (presorted) to reducers, reducers save the blob and external sort the contents if other files are in /jobname

	dispatch_mappers(job_name, job_map, m_map, r_map, so_buf.Bytes(), so_checksum)
	// err = dispatch_mappers(job_map, m_map, r_map, so_buf.Bytes(), so_checksum)
	// if err != nil {
	// 	os.Remove(path_go)
	// 	os.Remove(path_so)
	// 	wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: "*** failed to dispatch mappers\n***   " + err.Error()}}})
	// 	return err
	// }

	// after all mappers report completion
	dispatch_reducers(job_name, job_map, r_map, so_buf.Bytes(), so_checksum)
	// err = dispatch_reducers(job_map, r_map, so_buf.Bytes(), so_checksum)
	// if err != nil {
	// 	os.Remove(path_go)
	// 	os.Remove(path_so)
	// 	wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: "*** failed to dispatch reducers\n***   " + err.Error()}}})
	// 	return err
	// }

	fin.Wait()
	return nil
}

// get job_name, filenames from client
// compile plugin.go with -buildmode=plugin -o plugin.so and check for errors
// if errors send message as such and close
// get R from plugin.so, decide which nodes map which chunks and which nodes will be which reducer

// send reducer_addrs to client (client dials, reducers listen)
// send plugin.so, chunknames (sn should prob do a corruption check), and reducer addrs to each mapper, monitor progress
// send num_mappers, job_name to each reducer, monitor progress (should get a data message from all mappers, even if mapper found no kv's for that reducer)
// reducers send results to client

// sn: add map() and reduce()
func update_client(wh *wire.WireHandler, fin *sync.WaitGroup, job_name string, job_map *Job_map) {
	defer fin.Done()

	for {
		var stb strings.Builder
		fmt.Fprintf(&stb, "  Job %s\n  ----------------\n  Mapper progress:\n    %d / %d\n  Reducer progress:\n    %d / %d\n", job_name, job_map.mc, job_map.m, job_map.rc, job_map.r)
		if job_map.mf != nil && len(job_map.mf) > 0 {
			fmt.Fprintf(&stb, "  ----------------\n  Mapper failures:\n")
			for _, mf := range job_map.mf {
				fmt.Fprintf(&stb, "    %s\n", mf)
			}
		}
		if job_map.rf != nil && len(job_map.rf) > 0 {
			fmt.Fprintf(&stb, "  ----------------\n  Reducer failures:\n")
			for _, rf := range job_map.rf {
				fmt.Fprintf(&stb, "    %s\n", rf)
			}
		}

		ras := make([]*wire.ReducerAddr, 0, job_map.r)
		if len(job_map.rs) > 0 {
			for i, ra := range job_map.rs {
				if ra == nil {
					continue
				}

				ras = append(ras, &wire.ReducerAddr{Idx: int32(i), Addr: *ra})
			}
		}

		if job_map.rc == job_map.r {
			time.Sleep(2 * time.Second)
			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Mu{Mu: &wire.MRUpdate{
				R:            job_map.r,
				Update:       stb.String(),
				ReducerAddrs: ras,
			}}})

			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: ""}}})
			return
		} else {
			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Mu{Mu: &wire.MRUpdate{
				R:            job_map.r,
				Update:       stb.String(),
				ReducerAddrs: ras,
			}}})
		}

		time.Sleep(3 * time.Second)
	}
}

func dispatch_mappers(job_name string, job_map *Job_map, m_map map[string][]string, r_map []string, so_buf []byte, so_checksum []byte) {
	i := int32(0)
	var wg sync.WaitGroup
	wait_queue := 0
	// wait_queue := make(chan bool, 10)
	for m_addr, chunk_arr := range m_map {
		if len(chunk_arr) == 0 {
			continue
		}

		if wait_queue >= 16 {
			wait_queue = 0
			wg.Wait()
		}

		wg.Add(1)
		wait_queue++
		// wait_queue <- true
		go func(m_id int32, addr string, chunks []string, r_addrs []string, so []byte, check []byte) {
			defer wg.Done()
			defer atomic.AddInt32(&job_map.mc, int32(1))
			// defer func() {
			// 	<-wait_queue
			// }()

			// success := false
			// defer func(sc *bool) {
			// 	job_map.mcc <- *sc
			// 	fmt.Printf("mcc %d <-\n", job_map.mc)
			// }(&success)

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Println(err)
				job_map.mf = append(job_map.mf, addr)
				return
			}

			wh := wire.Construct_wirehandler(conn)
			defer wh.Close()

			err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Mrm{Mrm: &wire.MRMapReq{
				JobName:      job_name,
				MId:          m_id,
				ChunkNames:   chunks,
				ReducerAddrs: r_addrs,
				PluginSo:     so,
				Checksum:     check,
			}}})
			if err != nil {
				log.Println(err)
				job_map.mf = append(job_map.mf, addr)
				return
			}

			res, err := wh.Receive()
			if err != nil {
				log.Println(err)
				job_map.mf = append(job_map.mf, addr)
				return
			}

			switch res.Msg.(type) {
			case *wire.Wrapper_Ms:
				// success = true
				return
			case *wire.Wrapper_Rp:
				job_map.mf = append(job_map.mf, addr)
				log.Printf("*** mapper %s failure\n", addr)
				return
			default:
				job_map.mf = append(job_map.mf, addr)
				log.Printf("*** malformed response from mapper %s\n", addr)
				return
			}
		}(i, m_addr, chunk_arr, r_map, so_buf, so_checksum)
		i++
	}

	wg.Wait()
}

func dispatch_reducers(job_name string, job_map *Job_map, r_map []string, so_buf []byte, so_checksum []byte) {
	// for i := int32(0); i < job_map.m; i++ {
	// 	<-job_map.mcc
	// 	fmt.Printf("<- mcc %d\n", i)
	// }

	var wg sync.WaitGroup
	wait_queue := 0
	// wait_queue := make(chan bool, 10)
	for i, r_addr := range r_map {
		if wait_queue >= 16 {
			wait_queue = 0
			wg.Wait()
		}

		wg.Add(1)
		wait_queue++
		// wait_queue <- true
		go func(idx int32, addr string, so []byte, check []byte) {
			defer wg.Done()
			// defer func() {
			// 	<-wait_queue
			// }()
			defer atomic.AddInt32(&job_map.rc, int32(1))

			// success := false
			// defer func(sc *bool) {
			// 	job_map.rcc <- *sc
			// }(&success)

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Println(err)
				job_map.rf = append(job_map.rf, addr)
				return
			}

			wh := wire.Construct_wirehandler(conn)
			defer wh.Close()

			err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Mrr{Mrr: &wire.MRReduceReq{
				JobName:  job_name,
				RId:      idx,
				M:        job_map.m,
				PluginSo: so,
				Checksum: check,
			}}})
			if err != nil {
				log.Println(err)
				job_map.rf = append(job_map.rf, addr)
				return
			}

			res, err := wh.Receive()
			if err != nil {
				log.Println(err)
				job_map.rf = append(job_map.rf, addr)
				return
			}

			switch res.Msg.(type) {
			case *wire.Wrapper_Ms:
				job_map.rs[idx] = &addr
				// success = true
				return
			case *wire.Wrapper_Rp:
				job_map.rf = append(job_map.rf, addr)
				log.Printf("*** reducer %s failure\n", addr)
			default:
				job_map.rf = append(job_map.rf, addr)
				log.Printf("*** malformed response from reducer %s\n", addr)
			}
		}(int32(i), r_addr, so_buf, so_checksum)
	}

	wg.Wait()
}
