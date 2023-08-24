package cl

import (
	"bytes"
	"dfs/util"
	"dfs/wire"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"strings"
)

func open_files(dir string, job_name string, r int32) []*os.File {
	fs := make([]*os.File, 0, r)
	fp := path.Join(dir, "job_results", job_name)
	for i := int32(0); i < r; i++ {
		f, err := os.OpenFile(path.Join(fp, fmt.Sprintf("r%d", i)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			log.Printf("*** error creating file %s:\n***   %s\n", path.Join(fp, fmt.Sprintf("r%d", i)), err)
		}
		fs = append(fs, f)
	}
	return fs
}

func close_files(fs *[]*os.File) {
	for i, f := range *fs {
		if f != nil {
			f.Close()
			(*fs)[i] = nil
		}
	}
}

func Map_reduce(cmp_manager_addr string, dir string, tokens ...string) {
	// dir: /bigdata/students/ljendrusch
	// tokens: mr job_name [file1 file2 file3]
	if len(tokens) < 2 {
		fmt.Printf("\nMapReduce job request requires: 1) job name, and optionally 2) names of text files in the system to consider (default: all), e.g,\n    mapreduce word_count file1 file2.txt AnotherFile\n")
		return
	}

	job_name := tokens[1]
	if strings.HasSuffix(job_name, ".go") {
		job_name = job_name[:len(job_name)-4]
	}

	var files_incl []string
	if len(tokens) > 2 {
		files_incl = tokens[2:]
	}

	err := os.MkdirAll(path.Join(dir, "job_results", job_name), 0755)
	if err != nil {
		fmt.Printf("\n*** Error checking job_results directory %s:\n***   %s\n", path.Join(dir, "job_results", job_name), err)
		return
	}

	err = os.MkdirAll(path.Join(dir, "job_plugins", job_name), 0755)
	if err != nil {
		fmt.Printf("\n*** Error checking job_plugins directory %s:\n***   %s\n", path.Join(dir, "job_results", job_name), err)
		return
	}

	f, err := os.OpenFile(path.Join(dir, "job_plugins", job_name, fmt.Sprintf("%s.go", job_name)), os.O_RDONLY, 0777)
	if err != nil {
		fmt.Printf("\n*** Error opening plugin file %s:\n***   %s\n", path.Join(dir, "job_plugins", job_name, fmt.Sprintf("%s.go", job_name)), err)
		return
	}
	defer f.Close()

	f_info, err := f.Stat()
	if err != nil {
		fmt.Printf("\n*** Error statting plugin file %s:\n***   %s\n", path.Join(dir, "job_plugins", job_name, fmt.Sprintf("%s.go", job_name)), err)
		return
	}
	f_size := f_info.Size()

	buf := bytes.NewBuffer(make([]byte, 0, f_size))
	checksum, err := util.WriteAndHash(f, f_size, buf)
	if err != nil {
		fmt.Printf("\n*** Error copy / hashing plugin file %s:\n***   %s\n", path.Join(dir, "job_plugins", job_name, fmt.Sprintf("%s.go", job_name)), err)
		return
	}

	conn, err := net.Dial("tcp", cmp_manager_addr)
	if err != nil {
		log.Println(err)
		return
	}

	wh := wire.Construct_wirehandler(conn)
	defer wh.Close()

	err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Mr{Mr: &wire.MRRequest{
		JobName:   job_name,
		FileNames: files_incl,
		PluginGo:  buf.Bytes(),
		Checksum:  checksum,
	}}})
	if err != nil {
		log.Println(err)
		return
	}

	fmt.Printf(" *** mr req sent\n")

	var result_files []*os.File
	defer close_files(&result_files)

	var r_tracker []chan int8
	var res *wire.Wrapper
	for err == nil {
		res, err = wh.Receive()
		if err != nil {
			log.Println(err)
			return
		}

		switch t := res.Msg.(type) {
		case *wire.Wrapper_Mu:
			fmt.Println(t.Mu.Update)
			mr_update(dir, job_name, t.Mu.R, &result_files, &r_tracker, t.Mu.ReducerAddrs)
		case *wire.Wrapper_Ms:
			mr_wrapup(t.Ms, result_files, r_tracker)
			return
		case *wire.Wrapper_Rp:
			continue
		default:
			err = fmt.Errorf("malformed response from server")
			log.Println(err)
		}
	}
}

func mr_update(dir string, job_name string, r int32, result_files *[]*os.File, r_tracker *[]chan int8, update_addrs []*wire.ReducerAddr) {
	// dir: data_dir, e.g. /bigdata/students/ljendrusch
	if r_tracker == nil || *r_tracker == nil || len(*r_tracker) == 0 {
		*r_tracker = make([]chan int8, r)
		*result_files = open_files(dir, job_name, r)
	}

	for _, ad := range update_addrs {
		if ad == nil || (*r_tracker)[ad.Idx] != nil {
			continue
		}

		(*r_tracker)[ad.Idx] = make(chan int8)
		go func(f *os.File, ri int32, r_addr string, rt chan int8) {
			conn, err := net.Dial("tcp", r_addr)
			if err != nil {
				log.Println(err)
				rt <- -1
				return
			}

			wh := wire.Construct_wirehandler(conn)
			defer wh.Close()

			err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Mrrr{Mrrr: &wire.MRReduceResult{
				JobName:  job_name,
				RId:      ri,
				Data:     nil,
				Checksum: nil,
			}}})
			if err != nil {
				log.Println(err)
				rt <- -1
				return
			}

			res, err := wh.Receive()
			if err != nil {
				log.Println(err)
				rt <- -1
				return
			}

			switch t := res.Msg.(type) {
			case *wire.Wrapper_Mrrr:
				err = get_blob(f, t.Mrrr)
				if err != nil {
					log.Println(err)
					rt <- -1
				} else {
					rt <- 1
				}
				return
			default:
				log.Printf("*** malformed response from reducer %s\n", r_addr)
				rt <- -1
				return
			}
		}((*result_files)[ad.Idx], ad.Idx, ad.Addr, (*r_tracker)[ad.Idx])
	}
}

func get_blob(f *os.File, result *wire.MRReduceResult) error {
	sent_checksum := result.Checksum
	calcd_checksum, err := util.WriteAndHash(bytes.NewReader(result.Data), int64(len(result.Data)), f)
	if err != nil {
		fn := f.Name()
		f.Close()
		os.Remove(fn)
		return err
	}

	err = util.VerifyChecksum(sent_checksum, calcd_checksum)
	if err != nil {
		fn := f.Name()
		f.Close()
		os.Remove(fn)
		return err
	}

	return nil
}

func mr_wrapup(msg *wire.Message, result_files []*os.File, r_tracker []chan int8) {
	if msg != nil && len(msg.Message) > 0 {
		fmt.Println("*** Error processing MapReduce request:")
		fmt.Println(msg)
		return
	}

	for i := 0; i < len(result_files); i++ {
		fmt.Printf("    result %d... ", i)
		status := <-r_tracker[i]
		if status > 0 {
			size := int64(0)
			if f_stat, err := result_files[i].Stat(); err == nil {
				size += f_stat.Size()
			}
			fmt.Printf("SUCCESS, %d bytes\n", size)
		} else if status < 1 {
			if result_files[i] != nil {
				fn := result_files[i].Name()
				result_files[i].Close()
				os.Remove(fn)
			}
			fmt.Printf("FAILED\n")
		}
	}
}
