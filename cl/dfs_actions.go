package cl

import (
	"bytes"
	"dfs/util"
	"dfs/wire"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// request store
//
//		./client store {file_name} {optional: chunk_size in mib}
//		send file name, file size (by statting the file), chunk size
//		get response where to put chunks (if file name exists then cancel request)
//	    send chunks in parallel (don't send a chunk more than once; nodes handle replication)
func Store(controller_port string, tokens ...string) {
	if len(tokens) < 2 {
		fmt.Printf("\nStore request requires a filename\n")
		return
	}

	filename := path.Base(tokens[1])
	var dir string
	if path.IsAbs(tokens[1]) {
		dir = path.Dir(tokens[1])
	} else {
		dir, _ = os.Getwd()
	}

	fmt.Printf("\nRequesting storage of file '%s' - full path: '%s'\n", filename, path.Join(dir, filename))

	csize_str := ""
	if len(tokens) > 2 {
		csize_str = tokens[2]
		fmt.Printf("specified chunk size: %s\n", csize_str)
	}

	f, err := os.OpenFile(path.Join(dir, filename), os.O_RDONLY, 0666)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		log.Println(err)
		return
	}
	if stat.IsDir() {
		log.Println(fmt.Errorf("%s is a directory", path.Join(dir, filename)))
		return
	}

	file_size := stat.Size()
	is_txt := false

	buf := bytes.NewBuffer(make([]byte, 0, 512))
	_, err = io.CopyN(buf, f, 512)
	if err == nil {
		s := http.DetectContentType(buf.Bytes())
		is_txt = strings.HasPrefix(s, "text") && strings.HasSuffix(s, "utf-8")
	}

	var chunk_size int64
	if s, err := strconv.ParseInt(csize_str, 10, 64); err == nil && s > 0 {
		chunk_size = int64(s * 1048576)
	} else {
		chunk_size = int64(64 * 1048576) // default chunk size, MiB*bytes_per_MiB
	}

	conn, err := net.Dial("tcp", controller_port)
	if err != nil {
		log.Println(err)
		return
	}

	wh := wire.Construct_wirehandler(conn)
	defer wh.Close()

	num_chunks := file_size / chunk_size
	if file_size%chunk_size > 0 {
		num_chunks++
	}

	err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_St{St: &wire.Store{
		Name:    filename,
		NChunks: num_chunks,
		FSize:   file_size,
		CSize:   chunk_size,
	}}})
	if err != nil {
		log.Println(err)
		return
	}

	fmt.Printf(" *** store req sent\n")

	res, err := wh.Receive()
	if err != nil {
		log.Println(err)
		return
	}

	var chunk_map []*wire.StringTripleCSize
	switch t := res.Msg.(type) {
	case *wire.Wrapper_Cnp:
		chunk_map = t.Cnp.Nodes
		fmt.Printf(" *** chunkmap:\n")
		for _, t := range chunk_map {
			fmt.Printf("    %s, %s, %s\n", t.S1, t.S2, t.S3)
		}
		fmt.Printf(" *** chunk size: %dmib\n", chunk_size/1048576)
		fmt.Printf(" *** num chunks: %d\n", num_chunks)
	case *wire.Wrapper_Ms:
		log.Println(t.Ms.Message)
		return
	default:
		log.Println(errors.New("malformed response from server"))
		return
	}

	if len(chunk_map) == 0 {
		log.Println(errors.New("server unable to perform request at this time"))
		return
	}
	wh.Close()

	i := int64(0)
	off := int64(0)

	var wg sync.WaitGroup
	wait_queue := 0
	// wait_queue := make(chan bool, 10)
	for off < file_size {
		if wait_queue >= 16 {
			wait_queue = 0
			wg.Wait()
		}

		wg.Add(1)
		wait_queue++
		// wait_queue <- true
		if off+chunk_size >= file_size {
			go func(f *os.File, trip *wire.StringTripleCSize, c_idx int64, c_off int64, c_size int64) {
				defer wg.Done()
				// defer func() {
				// 	wait_queue <- true
				// }()

				nodes := [3]string{trip.S1, trip.S2, trip.S3}

				if is_txt {
					send_chunk(f, nodes, filename, wire.FileType_TXT, c_idx, c_off, c_size)
				} else {
					send_chunk(f, nodes, filename, wire.FileType_BIN, c_idx, c_off, c_size)
				}
			}(f, chunk_map[i], i, off, file_size-off)

			off += chunk_size
		} else if is_txt {
			dist_to_rt := int64(0)
			b := make([]byte, 1)
			for {
				_, err = f.ReadAt(b, off+chunk_size+dist_to_rt-1)
				if err != nil || string(b) == "\n" {
					break
				}

				dist_to_rt++
			}

			go func(f *os.File, trip *wire.StringTripleCSize, c_idx int64, c_off int64, c_size int64) {
				defer wg.Done()
				// defer func() {
				// 	wait_queue <- true
				// }()

				nodes := [3]string{trip.S1, trip.S2, trip.S3}

				send_chunk(f, nodes, filename, wire.FileType_TXT, c_idx, c_off, c_size)
			}(f, chunk_map[i], i, off, chunk_size+dist_to_rt)

			off += chunk_size + dist_to_rt
		} else {
			go func(f *os.File, trip *wire.StringTripleCSize, c_idx int64, c_off int64, c_size int64) {
				defer wg.Done()
				// defer func() {
				// 	wait_queue <- true
				// }()

				nodes := [3]string{trip.S1, trip.S2, trip.S3}

				send_chunk(f, nodes, filename, wire.FileType_BIN, c_idx, c_off, c_size)
			}(f, chunk_map[i], i, off, chunk_size)

			off += chunk_size
		}
		i++
	}

	wg.Wait()
}

// f, nodes, filename, wire.FileType_TXT, c_idx, c_off, c_size
func send_chunk(f *os.File, ports [3]string, filename string, f_type wire.FileType, c_idx int64, c_off int64, c_size int64) {
	var err error
	for i := 0; i < 3; i++ {
		if err != nil {
			log.Println(err)
		}

		port := ports[i]

		fmt.Printf(" *** send chunk %d to %s, size %d off %d\n", c_idx, port, c_size, c_off)

		conn, err := net.Dial("tcp", port)
		if err != nil {
			log.Println(err)
			continue
		}

		wh := wire.Construct_wirehandler(conn)
		defer wh.Close()

		buf := make([]byte, c_size)
		_, err = f.ReadAt(buf, c_off)
		if err != nil {
			log.Println(err)
			continue
		}

		checksum, err := util.Hash(bytes.NewReader(buf), c_size)
		if err != nil {
			log.Println(err)
			continue
		}

		fmt.Printf(" ***** chunk       %d\n ***** buf size    %d\n ***** checksum    %x\n\n", c_idx, c_size, checksum)

		err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Cul{Cul: &wire.ChunkUpload{
			ChunkId: &wire.ChunkId{
				FName: filename,
				FType: f_type,
				CIdx:  int32(c_idx),
				CSize: c_size,
			},
			Checksum: checksum,
			Data:     buf,
		}}})
		if err != nil {
			log.Println(err)
			continue
		}

		res, err := wh.Receive()
		if err != nil {
			log.Println(err)
			continue
		}

		switch res.Msg.(type) {
		case *wire.Wrapper_Ms:
			log.Printf("___success reported on cul %s_%s%d to port %s___\n", filename, f_type, c_idx, port)
			return
		case *wire.Wrapper_Rp:
			log.Printf("___repeat on cul %s_%s%d from port %s___\n", filename, f_type, c_idx, port)
			continue
		default:
			log.Println(errors.New("malformed response from server"))
			continue
		}
	}
	if err != nil {
		log.Printf("___FAILED to send chunk %s_%d to ports %s %s %s___\n", path.Base(f.Name()), c_idx, ports[0], ports[1], ports[2])
	}
}

// request retrieve
//
//	./client retrieve {file_name} {optional: dl_dir}
//	send file name
//	get where chunks are (or if file name doesn't exist cancel request)
//	send chunk download requests to respective nodes
//	receive chunks in parallel and reconstruct
func Retrieve(controller_port string, dir string, tokens ...string) {
	if len(tokens) < 2 {
		fmt.Printf("\nRetrieve request requires a filename\n")
		return
	}

	filename := path.Base(tokens[1])

	f, err := os.OpenFile(path.Join(dir, filename), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		log.Printf("file or directory already exists for path '%s'", path.Join(dir, filename))
		return
	}
	defer f.Close()

	fmt.Printf("\nRequesting retrieval of file '%s' - downloading to: '%s'\n", filename, dir)

	conn, err := net.Dial("tcp", controller_port)
	if err != nil {
		log.Println(err)
		return
	}

	wh := wire.Construct_wirehandler(conn)
	err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rtq{Rtq: &wire.RetrieveRq{
		Filename: filename,
	}}})
	if err != nil {
		log.Println(err)
		return
	}

	res, err := wh.Receive()
	if err != nil {
		log.Println(err)
		return
	}

	var f_type wire.FileType
	var node_ports []*wire.StringTripleCSize
	switch t := res.Msg.(type) {
	case *wire.Wrapper_Rtr:
		f_type = t.Rtr.FType
		node_ports = t.Rtr.Retrieve
	case *wire.Wrapper_Ms:
		log.Println(t.Ms.Message)
		return
	default:
		log.Println(errors.New("malformed response from server"))
		return
	}
	if node_ports == nil || len(node_ports) < 1 {
		log.Println(fmt.Errorf("server sent faulty port map"))
		return
	}

	for i, p := range node_ports {
		if i > 0 && (i%3) == 0 {
			fmt.Println()
		}
		fmt.Printf("    %d: %s %s %s", i, p.S1, p.S2, p.S3)
	}
	fmt.Println()

	filesize := int64(0)
	for _, np := range node_ports {
		filesize += np.CSize
	}

	f.Seek(filesize-1, 0)
	f.Write([]byte{0})

	fail_flag := false
	off := int64(0)

	var wg sync.WaitGroup
	wait_queue := 0
	// wait_queue := make([]chan bool, 10)
	for c_idx, ports_csize := range node_ports {
		if fail_flag {
			break
		}

		if wait_queue >= 16 {
			wait_queue = 0
			wg.Wait()
		}

		wg.Add(1)
		wait_queue++
		// wait_queue <- true
		go func(f *os.File, ports_csize *wire.StringTripleCSize, filename string, f_type wire.FileType, c_idx int, c_off int64) {
			defer wg.Done()
			// defer func() {
			// 	<-wait_queue
			// }()

			ports := [3]string{ports_csize.S1, ports_csize.S2, ports_csize.S3}

			l_err := get_chunk(f, ports, filename, f_type, c_idx, c_off, ports_csize.CSize)
			if l_err != nil {
				fail_flag = true
				log.Printf("___FAILED to retrieve chunk %s_%d from system___\n", filename, c_idx)
			}
		}(f, ports_csize, filename, f_type, c_idx, off)

		off += ports_csize.CSize
	}

	wg.Wait()

	if fail_flag {
		f.Close()
		os.Remove(path.Join(dir, filename))
	}
}

func get_chunk(f *os.File, sn_ports [3]string, filename string, ft wire.FileType, c_idx int, c_off int64, c_size int64) error {
	for i := 0; i < 3; i++ {
		sn_port := sn_ports[i]

		if sn_port == "" {
			continue
		}

		var f_type string
		switch ft {
		case wire.FileType_BIN:
			f_type = "b"
		case wire.FileType_TXT:
			f_type = "t"
		default:
			f_type = "b"
		}

		conn, err := net.DialTimeout("tcp", sn_port, time.Second*2)
		if err != nil {
			log.Printf("failed to download chunk %d :: net.Dial :: iter %d port %s\n", c_idx, i, sn_port)
			continue
		}

		fmt.Printf(" *** chunk %s_%s%d, dl from %s\n", filename, f_type, c_idx, sn_port)

		wh := wire.Construct_wirehandler(conn)
		err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Cdl{Cdl: &wire.ChunkDownload{
			ChunkId: &wire.ChunkId{
				FName: filename,
				FType: ft,
				CIdx:  int32(c_idx),
				CSize: int64(0),
			},
			Checksum: []byte{},
			Data:     []byte{},
		}}})
		if err != nil {
			log.Printf("failed to download chunk %s_%s%d :: wh.Send :: iter %d port %s\n", filename, f_type, c_idx, i, sn_port)
			continue
		}

		res, err := wh.Receive()
		if err != nil {
			log.Printf("failed to download chunk %s_%s%d :: wh.Receive :: iter %d port %s\n", filename, f_type, c_idx, i, sn_port)
			continue
		}

		switch t := res.Msg.(type) {
		case *wire.Wrapper_Cdl:
			calcd_checksum, err := util.Hash(bytes.NewReader(t.Cdl.Data), t.Cdl.ChunkId.CSize)
			if err != nil {
				log.Printf("failed to download chunk %s_%s%d :: util.Hash :: iter %d port %s\n", filename, f_type, c_idx, i, sn_port)
				continue
			}

			err = util.VerifyChecksum(t.Cdl.Checksum, calcd_checksum)
			if err != nil {
				log.Printf("failed to download chunk %s_%s%d :: util.VerifyChecksum :: iter %d port %s\n  in-memory checksum:  %x\n  calculated checksum: %x\n", filename, f_type, c_idx, i, sn_port, t.Cdl.Checksum, calcd_checksum)
				continue
			}

			fmt.Printf(" ***** dl %s_%s%d size %d offset %d\n", filename, f_type, c_idx, len(t.Cdl.Data), c_off)

			_, err = f.WriteAt(t.Cdl.Data, c_off)
			if err != nil {
				log.Printf("failed to download chunk %s_%s%d :: util.Hash :: iter %d port %s\n", filename, f_type, c_idx, i, sn_port)
				continue
			}

			return nil

		case *wire.Wrapper_Ms:
			log.Printf("failed to download chunk %s_%s%d :: %s :: iter %d port %s\n", filename, f_type, c_idx, t.Ms.Message, i, sn_port)
			continue
		default:
			log.Printf("failed to download chunk %s_%s%d :: %s :: iter %d port %s\n", filename, f_type, c_idx, "malformed response from server", i, sn_port)
			continue
		}
	}
	return errors.New("")
}

// request delete
//
//	./client delete {file_name}
func Delete(controller_port string, tokens ...string) {
	if len(tokens) < 2 {
		fmt.Printf("\nDelete request requires a filename\n")
		return
	}

	filename := path.Clean(tokens[1])

	fmt.Printf("\nRequesting deletion of file '%s'\n", filename)

	conn, err := net.Dial("tcp", controller_port)
	if err != nil {
		log.Println(err)
		return
	}

	wh := wire.Construct_wirehandler(conn)
	defer wh.Close()

	err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Dl{Dl: &wire.Delete{
		Delete: filename,
	}}})
	if err != nil {
		log.Println(err)
		return
	}

	res, err := wh.Receive()
	if err != nil {
		log.Println(err)
		return
	}

	switch t := res.Msg.(type) {
	case *wire.Wrapper_Dl:
		fmt.Println(t.Dl.Delete)
	case *wire.Wrapper_Ms:
		log.Println(t.Ms.Message)
	default:
		log.Println(errors.New("malformed response from server"))
	}
}

// request ls
//
//	./client ls
func List(controller_port string, tokens ...string) {
	fmt.Printf("\nList request\n")
	verbose := false
	if len(tokens) > 1 && strings.EqualFold(tokens[1], "-v") {
		verbose = true
	}

	conn, err := net.Dial("tcp", controller_port)
	if err != nil {
		log.Println(err)
		return
	}

	wh := wire.Construct_wirehandler(conn)
	defer wh.Close()

	err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ls{Ls: &wire.List{List: fmt.Sprintf("%t", verbose)}}})
	if err != nil {
		log.Println(err)
		return
	}

	res, err := wh.Receive()
	if err != nil {
		log.Println(err)
		return
	}

	switch t := res.Msg.(type) {
	case *wire.Wrapper_Ls:
		fmt.Println(t.Ls.List)
	default:
		log.Println(errors.New("malformed response from server"))
	}
}

func Info(controller_port string, tokens ...string) {
	if len(tokens) < 2 {
		fmt.Printf("\nInfo request requires a node number\n")
		return
	}

	node_num := tokens[1]
	if _, err := strconv.Atoi(node_num); err != nil {
		fmt.Printf("\nInfo request requires a node number\n%s not a number\n", node_num)
		return
	}

	fmt.Printf("\nRequesting info on node '%s'\n", node_num)

	conn, err := net.Dial("tcp", controller_port)
	if err != nil {
		log.Println(err)
		return
	}

	wh := wire.Construct_wirehandler(conn)
	defer wh.Close()

	err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_If{If: &wire.Info{
		Info: node_num,
	}}})
	if err != nil {
		log.Println(err)
		return
	}

	res, err := wh.Receive()
	if err != nil {
		log.Println(err)
		return
	}

	switch t := res.Msg.(type) {
	case *wire.Wrapper_If:
		fmt.Println(t.If.Info)
	case *wire.Wrapper_Ms:
		log.Println(t.Ms.Message)
	default:
		log.Println(errors.New("malformed response from server"))
	}
}

// request node activity
//
//	./client report
func Activity(controller_port string) {
	fmt.Printf("\nActivity request\n")

	conn, err := net.Dial("tcp", controller_port)
	if err != nil {
		log.Println(err)
		return
	}

	wh := wire.Construct_wirehandler(conn)
	defer wh.Close()

	err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ac{Ac: &wire.Activity{Activity: ""}}})
	if err != nil {
		log.Println(err)
		return
	}

	res, err := wh.Receive()
	if err != nil {
		log.Println(err)
		return
	}

	switch t := res.Msg.(type) {
	case *wire.Wrapper_Ac:
		fmt.Println(t.Ac.Activity)
	default:
		log.Println(errors.New("malformed response from server"))
	}
}
