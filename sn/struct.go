package sn

import (
	"bytes"
	"dfs/wire"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type Storage_node struct {
	Name           string
	ControllerName string
	CmpManagerName string
	CtrlPort       string
	CmMnPort       string
	Dir            string
	Port_suffix    int32
	Busy_epoch     int64
	Chunkmap       map[string]*wire.ChunkMap // filename to file_type + chunk idx to chunk size
	Checksums      map[string]map[int32][]byte
	Space          uint64
	Served         int32
}

// ./storage_node /bigdata/students/ljendrusch/${port} orion01 ${port}
func Init_sn(args []string) *Storage_node {
	name, err := os.Hostname()
	if err != nil {
		log.Fatalln(err)
	}
	name = strings.SplitN(name, ".", 2)[0]

	err = os.MkdirAll(path.Join(path.Clean(args[1]), "chunks_tmp"), 0755)
	if err != nil {
		log.Fatalln(err)
	}

	var port_suffix int32
	if ps, err := strconv.Atoi(args[4]); err == nil {
		port_suffix = int32(ps) % 100
	} else {
		log.Fatalln(err)
	}

	sn := &Storage_node{
		Name:           name,
		ControllerName: args[2],                                                       // orion02
		CmpManagerName: args[3],                                                       // orion03
		CtrlPort:       args[4],                                                       // 140xx
		CmMnPort:       fmt.Sprintf("%s%c%s", args[4][:2], args[4][2]+2, args[4][3:]), // 142xx
		Dir:            path.Clean(args[1]),                                           // /bigdata/students/ljendrusch/orionxx_140yy
		Port_suffix:    port_suffix,
		Busy_epoch:     -1,
		Chunkmap:       make(map[string]*wire.ChunkMap),
		Checksums:      make(map[string]map[int32][]byte),
		Space:          0,
		Served:         0,
	}

	return sn
}

func (sn *Storage_node) HeartCtrl() {
	for {
		time.Sleep(4 * time.Second)

		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", sn.ControllerName, sn.CtrlPort), time.Second*2)
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			log.Println(err)
			continue
		}
		wh := wire.Construct_wirehandler(conn)

		err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Df{Df: &wire.Defibrillate{Name: sn.Name}}})
		wh.Close()
		if err != nil {
			log.Println(err)
			continue
		}
		break
	}

	var listener net.Listener
	var err error
	for {
		if listener != nil {
			listener.Close()
		}

		if listener, err = net.Listen("tcp", fmt.Sprintf(":%s", sn.CtrlPort)); err == nil { // 140xx
			for {
				if conn, err := listener.Accept(); err == nil {
					wh := wire.Construct_wirehandler(conn)

					err = sn.heartbeatCtrl(wh)
					if err != nil {
						log.Println(err)
					}
					wh.Close()
				}
			}
		}
	}
}

func (sn *Storage_node) HeartCmMn() {
	for {
		time.Sleep(4 * time.Second)

		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", sn.CmpManagerName, sn.CmMnPort), time.Second*2)
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			log.Println(err)
			continue
		}
		wh := wire.Construct_wirehandler(conn)

		err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Df{Df: &wire.Defibrillate{Name: sn.Name}}})
		wh.Close()
		if err != nil {
			log.Println(err)
			continue
		}
		break
	}

	var listener net.Listener
	var err error
	for {
		if listener != nil {
			listener.Close()
		}

		if listener, err = net.Listen("tcp", fmt.Sprintf(":%s", sn.CmMnPort)); err == nil { // 142xx
			for {
				if conn, err := listener.Accept(); err == nil {
					wh := wire.Construct_wirehandler(conn)

					err = sn.heartbeatCmMn(wh)
					if err != nil {
						log.Println(err)
					}
					wh.Close()
				}
			}
		}
	}
}

func (sn *Storage_node) heartbeatCtrl(wh *wire.WireHandler) error {
	var stat unix.Statfs_t
	err := unix.Statfs(sn.Dir, &stat)
	if err != nil {
		return err
	}
	sn.Space = stat.Bfree * uint64(stat.Bsize)

	dir, err := os.OpenFile(sn.Dir, os.O_RDONLY, 0666)
	if err != nil {
		if dir != nil {
			dir.Close()
		}
		return err
	}
	defer dir.Close()

	raw_files, err := dir.ReadDir(0)
	if err != nil {
		return err
	}

	t_chunkmap := make(map[string]*wire.ChunkMap)
	for _, f := range raw_files {
		if f.IsDir() || strings.HasSuffix(f.Name(), "h") {
			continue
		}

		f_info, err := f.Info()
		if err != nil {
			log.Println(err)
			continue
		}

		ci, err := strconv.ParseInt(f.Name()[strings.LastIndex(f.Name(), "_")+2:], 10, 32)
		if err != nil {
			log.Println(err)
			continue
		}

		f_name := f.Name()[:strings.LastIndex(f.Name(), "_")]
		c_idx := int32(ci)
		c_size := f_info.Size()

		if _, ok := t_chunkmap[f_name]; !ok {
			var f_type wire.FileType
			switch f.Name()[strings.LastIndex(f.Name(), "_")+1 : strings.LastIndex(f.Name(), "_")+2] {
			case "b":
				f_type = wire.FileType_BIN
			case "t":
				f_type = wire.FileType_TXT
			default:
				f_type = wire.FileType_BIN
			}

			t_chunkmap[f_name] = &wire.ChunkMap{
				FType: f_type,
				CMap:  make(map[int32]int64),
			}
		}

		t_chunkmap[f_name].CMap[c_idx] = c_size

		if _, ok := sn.Checksums[f_name]; !ok {
			sn.Checksums[f_name] = make(map[int32][]byte)
		}
		if _, ok := sn.Checksums[f_name][c_idx]; !ok {
			checksum := bytes.NewBuffer(make([]byte, 0, 16))
			fh, err := os.OpenFile(path.Join(sn.Dir, fmt.Sprintf("%sh", f.Name())), os.O_RDONLY, 0666)
			if err == nil {
				_, err = io.CopyN(checksum, fh, 16)
				if err == nil {
					sn.Checksums[f_name][c_idx] = checksum.Bytes()
				}
			}
		}
	}

	sn.Chunkmap = t_chunkmap

	fmt.Printf("    node %s\n    heartbeat: %s\n    ms busy: %d\n    served: %d\n    space: %dMiB\n    files: [", sn.Name, time.Now().String(), sn.Busy_epoch, sn.Served, sn.Space/1048576)
	for f := range t_chunkmap {
		fmt.Printf(" %s ", f)
	}
	fmt.Printf("]\n\n")

	return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Hb{Hb: &wire.Heartbeat{
		Space:    sn.Space,
		Served:   sn.Served,
		Busy:     sn.Busy_epoch,
		ChunkMap: sn.Chunkmap,
	}}})
}

func (sn *Storage_node) heartbeatCmMn(wh *wire.WireHandler) error {
	return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Hb{Hb: &wire.Heartbeat{
		Space:    sn.Space,
		Served:   sn.Served,
		Busy:     sn.Busy_epoch,
		ChunkMap: sn.Chunkmap,
	}}})
}
