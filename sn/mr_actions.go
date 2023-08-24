package sn

import (
	"bufio"
	"bytes"
	"dfs/util"
	"dfs/wire"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"plugin"
	"strings"
	"sync"
)

func send_results(job_name string, m_id int32, r int, reducer_addrs []string, bufs [][]byte) {
	var wg sync.WaitGroup
	wait_queue := 0
	// wait_queue := make(chan bool, 10)
	for i := 0; i < r; i++ {
		if wait_queue >= 16 {
			wait_queue = 0
			wg.Wait()
		}

		wg.Add(1)
		wait_queue++
		// wait_queue <- true
		go func(ri int32) {
			defer wg.Done()
			// defer func() {
			// 	<-wait_queue
			// }()

			buf := util.KVBufSort(bufs[ri])

			check, err := util.Hash(bytes.NewReader(buf), int64(len(buf)))
			if err != nil {
				log.Println("err hashing out_file %s%d :: %s\n", job_name, ri, err.Error())
				return
			}

			conn, err := net.Dial("tcp", reducer_addrs[ri])
			if err != nil {
				if conn != nil {
					conn.Close()
				}
				log.Println("err dialing %s :: %s\n", reducer_addrs[ri], err.Error())
				return
			}

			l_wh := wire.Construct_wirehandler(conn)
			defer l_wh.Close()

			fmt.Printf("___SEND Mrmr %s %d; %d bytes___\n", job_name, m_id, len(buf))
			err = l_wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Mrmr{Mrmr: &wire.MRMapResult{
				JobName:  job_name,
				RId:      ri,
				MId:      m_id,
				Kvs:      buf,
				Checksum: check,
			}}})
			if err != nil {
				log.Println("err sending %s :: %s\n", reducer_addrs[ri], err.Error())
			}
		}(int32(i))
	}

	wg.Wait()
}

// called by cmp_man, computed in node, tells cmp_man when done, sends files to respective reducers
// dir string, r int32, in_file_s string, out_file_s string
func (sn *Storage_node) Handle_map_req(wh *wire.WireHandler, mrm *wire.MRMapReq) error {
	job_name, m_id, chunk_names := mrm.JobName, mrm.MId, mrm.ChunkNames
	r, reducer_addrs := len(mrm.ReducerAddrs), mrm.ReducerAddrs
	plugin_so, sent_checksum := mrm.PluginSo, mrm.Checksum

	var err error
	defer func(e error) {
		if e == nil {
			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
				Message: "",
			}}})
		} else {
			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		}
	}(err)

	bufs := make([][]byte, r)
	defer send_results(job_name, m_id, r, reducer_addrs, bufs)

	err = os.MkdirAll(path.Join(sn.Dir, job_name), 0755)
	if err != nil {
		return err
	}

	// /bigdata/students/ljendrusch/orion04_14000/word_count/word_count.so
	plugin_fn := path.Join(sn.Dir, job_name, fmt.Sprintf("%s.so", job_name))

	f_so, err := os.OpenFile(plugin_fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		if f_so != nil {
			f_so.Close()
		}
		os.Remove(plugin_fn)
		return err
	}

	calcd_checksum, err := util.WriteAndHash(bytes.NewReader(plugin_so), int64(len(plugin_so)), f_so)
	if err != nil {
		f_so.Close()
		os.Remove(plugin_fn)
		return err
	}
	f_so.Close()

	err = util.VerifyChecksum(sent_checksum, calcd_checksum)
	if err != nil {
		os.Remove(plugin_fn)
		return err
	}

	plg, err := plugin.Open(plugin_fn)
	if err != nil {
		os.Remove(plugin_fn)
		return err
	}

	fm_a, err := plg.Lookup("F_map")
	if err != nil {
		os.Remove(plugin_fn)
		return err
	}

	// file_name string, line_number int, line_text string, output *[]string
	f_map := fm_a.(func(string, int, string, *[]string))

	map_job_dir := path.Join(sn.Dir, job_name, fmt.Sprintf("m%d", m_id))
	err = os.MkdirAll(map_job_dir, 0755)
	if err != nil {
		return err
	}

	// /bigdata/students/ljendrusch/orion04_14000/word_count/m#/m_raw_r#
	var raw_out_fs []*os.File
	for i := 0; i < r; i++ {
		f, err := os.OpenFile(path.Join(map_job_dir, fmt.Sprintf("m_raw_r%d", i)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			log.Println(err)
			continue
		}
		raw_out_fs = append(raw_out_fs, f)
	}
	defer func(fs []*os.File) {
		for _, f := range fs {
			if f != nil {
				f.Close()
			}
		}
	}(raw_out_fs)

	var outer_wg sync.WaitGroup
	out_chans := make([]chan string, r)
	for i := 0; i < r; i++ {
		out_chans[i] = make(chan string)

		outer_wg.Add(1)
		go func(wri int, oc chan string, of *os.File) {
			defer outer_wg.Done()
			wr := bufio.NewWriter(raw_out_fs[wri])
			defer wr.Flush()

			for s := range oc {
				wr.WriteString(s)
			}
		}(i, out_chans[i], raw_out_fs[i])
	}

	var inner_wg sync.WaitGroup
	wait_queue := 0
	// wait_queue := make(chan bool, 10)
	for _, cn := range chunk_names {
		if wait_queue >= 16 {
			wait_queue = 0
			inner_wg.Wait()
		}

		inner_wg.Add(1)
		wait_queue++
		// wait_queue <- true
		go func(c_path string, cn string) {
			defer inner_wg.Done()
			// defer func() {
			// 	<-wait_queue
			// }()

			f, l_err := os.OpenFile(c_path, os.O_RDONLY, 0666)
			if l_err != nil {
				if f != nil {
					f.Close()
				}
				log.Println(l_err)
				return
			}
			defer f.Close()

			scanner := bufio.NewScanner(f)
			line_num := 0
			for scanner.Scan() {
				var out_arr []string

				// file_name string, line_number int, line_text string, output *[]string
				f_map(cn, line_num, scanner.Text(), &out_arr)
				if len(out_arr)%2 != 0 {
					if out_arr[len(out_arr)-1] == "\n" {
						out_arr = out_arr[:len(out_arr)-2]
					} else {
						out_arr = append(out_arr, "\n")
					}
				}

				for j := 0; j < len(out_arr); j += 2 {
					idx := int(strings.ToLower(out_arr[j][0:1])[0]) % r
					out_chans[idx] <- fmt.Sprintf("%s%s", out_arr[j], out_arr[j+1])
				}
				line_num++
			}

			l_err = scanner.Err()
			if l_err != nil {
				log.Println(l_err)
			}
		}(path.Join(sn.Dir, cn), cn)
	}
	inner_wg.Wait()

	for i := 0; i < r; i++ {
		close(out_chans[i])
	}
	outer_wg.Wait()

	for i := 0; i < r; i++ {
		fr, l_err := os.OpenFile(raw_out_fs[i].Name(), os.O_RDONLY, 0666)
		if l_err != nil {
			log.Println(l_err)
			continue
		}

		bufs[i], l_err = io.ReadAll(fr)
		fr.Close()
		if l_err != nil {
			log.Println(l_err)
			continue
		}
	}

	return nil
}

func (sn *Storage_node) Handle_map_result(wh *wire.WireHandler, mrmr *wire.MRMapResult) error {
	job_name, r_id, m_id := mrmr.JobName, mrmr.RId, mrmr.MId
	kvs, sent_checksum := mrmr.Kvs, mrmr.Checksum

	save_mres_dir := path.Join(sn.Dir, job_name, fmt.Sprintf("r%d", r_id))
	err := os.MkdirAll(save_mres_dir, 0755)
	if err != nil {
		log.Printf("error creating dir %s :: %s\n", save_mres_dir, err.Error())
		return err
	}

	f_path := path.Join(save_mres_dir, fmt.Sprintf("m%d", m_id))

	// /bigdata/students/ljendrusch/orion04_14000/word_count/r#/m#
	f, err := os.OpenFile(f_path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		if f != nil {
			f.Close()
		}
		log.Printf("error saving map result %s :: %s\n", f_path, err.Error())
		return err
	}
	defer f.Close()

	calcd_checksum, err := util.WriteAndHash(bytes.NewReader(kvs), int64(len(kvs)), f)
	if err != nil {
		log.Printf("error hashing map result %s :: %s\n", f_path, err.Error())
		os.Remove(f_path)
		return err
	}

	err = util.VerifyChecksum(sent_checksum, calcd_checksum)
	if err != nil {
		log.Printf("checksum mismatch on %s :: %s\n", f_path, err.Error())
		os.Remove(f_path)
		return err
	}

	return nil
}

func (sn *Storage_node) Handle_reduce_req(wh *wire.WireHandler, mrr *wire.MRReduceReq) error {
	job_name, r_id, m := mrr.JobName, mrr.RId, mrr.M
	plugin_so, sent_checksum := mrr.PluginSo, mrr.Checksum

	plugin_fn := path.Join(sn.Dir, job_name, fmt.Sprintf("%s.so", job_name))

	if _, err_on_missing := os.OpenFile(plugin_fn, os.O_RDONLY, 0666); err_on_missing != nil {
		err := os.MkdirAll(path.Join(sn.Dir, job_name), 0755)
		if err != nil {
			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
			return err
		}

		so_f, err := os.OpenFile(plugin_fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
		if err != nil {
			os.Remove(plugin_fn)
			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
			return err
		}

		calcd_checksum, err := util.WriteAndHash(bytes.NewReader(plugin_so), int64(len(plugin_so)), so_f)
		if err != nil {
			so_f.Close()
			os.Remove(plugin_fn)
			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
			return err
		}
		so_f.Close()

		err = util.VerifyChecksum(sent_checksum, calcd_checksum)
		if err != nil {
			os.Remove(plugin_fn)
			wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
			return err
		}
	}

	plg, err := plugin.Open(plugin_fn)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return err
	}

	fr_a, err := plg.Lookup("F_reduce")
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return err
	}

	// keys []string, values []string, output *[]string
	f_reduce := fr_a.(func([]string, []string, *[]string))

	// /bigdata/students/ljendrusch/orion04_14000/word_count/r#/m#
	var map_results []*os.File
	r_path := path.Join(sn.Dir, job_name, fmt.Sprintf("r%d", r_id))
	for i := int32(0); i < m; i++ {
		f, err := os.OpenFile(path.Join(r_path, fmt.Sprintf("m%d", i)), os.O_RDONLY, 0666)
		if err != nil {
			log.Printf("err opening map_results %s/mr%d\n", job_name, i)
			continue
		}
		map_results = append(map_results, f)
	}

	// /bigdata/students/ljendrusch/orion04_14000/word_count/r#/sorted
	sorted_mrs_wr, err := os.OpenFile(path.Join(r_path, "sorted"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		log.Printf("err creating map_results %s/sorted\n", job_name)
		return err
	}

	err = sort_map_results(job_name, sorted_mrs_wr, map_results)
	if err != nil {
		sorted_mrs_wr.Close()
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		log.Printf("err sorting / writing map_results %s/sorted\n", job_name)
		return err
	}
	sorted_mrs_wr.Close()

	sorted_mrs_rd, err := os.OpenFile(path.Join(r_path, "sorted"), os.O_RDONLY, 0666)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		log.Printf("err opening map_results %s/sorted\n", job_name)
		return err
	}

	keys, values := sorted_to_kvs(sorted_mrs_rd)
	sorted_mrs_rd.Close()

	var output []string
	f_reduce(keys, values, &output)

	reduce_results, err := os.OpenFile(path.Join(r_path, "reduced"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		log.Printf("err opening reduce_results %s :: %s\n", job_name, err.Error())
		return err
	}
	defer reduce_results.Close()

	wrt := bufio.NewWriter(reduce_results)
	for _, s := range output {
		_, err = wrt.WriteString(fmt.Sprintln(s))
		if err != nil {
			log.Printf("err writing reduce_results %s :: %s\n", job_name, err.Error())
			break
		}
	}
	err = wrt.Flush()
	if err != nil {
		log.Printf("err on flush writing reduce_results %s :: %s\n", job_name, err.Error())
	}

	return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
		Message: "",
	}}})
}

func sort_map_results(job_name string, sorted_map_results *os.File, map_results []*os.File) error {
	wrt := bufio.NewWriter(sorted_map_results)

	var scs []*bufio.Scanner
	for _, mrf := range map_results {
		scs = append(scs, bufio.NewScanner(mrf))
	}

	l := len(scs)
	for i := 0; i < l; i++ {
		if !scs[i].Scan() {
			scs = append(scs[:i], scs[i+1:]...)
			i--
			l--
		}
	}

	var top_strs []string
	for _, sc := range scs {
		top_strs = append(top_strs, sc.Text())
	}

	for len(top_strs) > 0 {
		minarg := 0
		for i, s := range top_strs {
			if s < top_strs[minarg] {
				minarg = i
			}
		}

		_, err := wrt.WriteString(fmt.Sprintln(top_strs[minarg]))
		if err != nil {
			return err
		}
		if scs[minarg].Scan() {
			_, err := wrt.WriteString(fmt.Sprintln(scs[minarg].Text()))
			if err != nil {
				return err
			}
		} else {
			_, err := wrt.WriteString("\n")
			if err != nil {
				return err
			}
			scs[minarg] = nil
		}

		if scs[minarg] == nil || !scs[minarg].Scan() {
			top_strs = append(top_strs[:minarg], top_strs[minarg+1:]...)
			scs = append(scs[:minarg], scs[minarg+1:]...)
		} else {
			top_strs[minarg] = scs[minarg].Text()
		}
	}

	err := wrt.Flush()
	if err != nil {
		log.Printf("non-fatal err writing sorted_map_results %s :: %s\n", job_name, err.Error())
	}
	return nil
}

func sorted_to_kvs(sorted_map_results *os.File) ([]string, []string) {
	var keys, vals []string
	scn := bufio.NewScanner(sorted_map_results)
	for scn.Scan() {
		keys = append(keys, scn.Text())
		if scn.Scan() {
			vals = append(vals, scn.Text())
		} else {
			vals = append(vals, "")
			break
		}
	}

	if len(keys) != len(vals) {
		if len(keys) < len(vals) {
			vals = vals[:len(keys)]
		} else {
			keys = keys[:len(vals)]
		}
	}
	return keys, vals
}

func (sn *Storage_node) Handle_reduce_result(wh *wire.WireHandler, mrrr *wire.MRReduceResult) error {
	job_name, r_id := mrrr.JobName, mrrr.RId

	r_path := path.Join(sn.Dir, job_name, fmt.Sprintf("r%d", r_id))

	f, err := os.OpenFile(path.Join(r_path, "reduced"), os.O_RDONLY, 0666)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		log.Printf("err opening reduce_results %s :: %s\n", job_name, err.Error())
		return err
	}
	defer f.Close()

	f_stat, err := f.Stat()
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		log.Printf("err statting reduce_results %s :: %s\n", job_name, err.Error())
		return err
	}

	buf := bytes.NewBuffer(make([]byte, 0, f_stat.Size()))
	check, err := util.WriteAndHash(f, f_stat.Size(), buf)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		log.Printf("err hashing reduce_results %s :: %s\n", job_name, err.Error())
		return err
	}

	return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Mrrr{Mrrr: &wire.MRReduceResult{
		JobName:  job_name,
		RId:      r_id,
		Data:     buf.Bytes(),
		Checksum: check,
	}}})
}
