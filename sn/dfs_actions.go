package sn

import (
	"bytes"
	"dfs/util"
	"dfs/wire"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
)

func (sn *Storage_node) Handle_chunk_upload(wh *wire.WireHandler, cul *wire.ChunkUpload) error {
	f_name, f_type, c_idx, c_size := cul.ChunkId.FName, cul.ChunkId.FType, cul.ChunkId.CIdx, cul.ChunkId.CSize
	sent_checksum, data := cul.Checksum, cul.Data

	// cul.mapping is a string like {filename_tchunkidx}, e.g., exefile_b2 txtfile_t17
	var ft string
	switch f_type {
	case wire.FileType_BIN:
		ft = "b"
	case wire.FileType_TXT:
		ft = "t"
	default:
		ft = "b"
	}

	cfn := fmt.Sprintf("%s_%s%d", f_name, ft, c_idx)
	c_fn_path := path.Join(sn.Dir, cfn)
	ch_fn_path := path.Join(sn.Dir, fmt.Sprintf("%sh", cfn))

	c_fn_path_t := path.Join(sn.Dir, "chunks_tmp", cfn)
	ch_fn_path_t := path.Join(sn.Dir, "chunks_tmp", fmt.Sprintf("%sh", cfn))

	_, err := os.OpenFile(c_fn_path, os.O_RDONLY, 0666)
	if err == nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return fmt.Errorf("file [%s] already exists", c_fn_path)
	}

	f_tmp, err := os.OpenFile(c_fn_path_t, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return err
	}
	defer f_tmp.Close()

	fh_tmp, err := os.OpenFile(ch_fn_path_t, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return err
	}
	defer fh_tmp.Close()

	calcd_checksum, err := util.WriteAndHash(bytes.NewReader(data), c_size, f_tmp)
	if err != nil {
		f_tmp.Close()
		fh_tmp.Close()
		os.Remove(c_fn_path_t)
		os.Remove(ch_fn_path_t)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return err
	}
	f_tmp.Close()

	err = util.VerifyChecksum(sent_checksum, calcd_checksum)
	if err != nil {
		fh_tmp.Close()
		os.Remove(c_fn_path_t)
		os.Remove(ch_fn_path_t)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return err
	}

	_, err = io.CopyN(fh_tmp, bytes.NewReader(calcd_checksum), int64(len(calcd_checksum)))
	if err != nil {
		fh_tmp.Close()
		os.Remove(c_fn_path_t)
		os.Remove(ch_fn_path_t)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return err
	}
	fh_tmp.Close()

	os.Rename(ch_fn_path_t, ch_fn_path)
	_, err = os.OpenFile(ch_fn_path, os.O_RDONLY, 0666)
	if err != nil {
		os.Remove(ch_fn_path)
		os.Remove(c_fn_path_t)
		os.Remove(ch_fn_path_t)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return fmt.Errorf("failed to move [%s] to [%s]", ch_fn_path_t, ch_fn_path)
	}

	if sn.Checksums[f_name] == nil {
		sn.Checksums[f_name] = make(map[int32][]byte)
	}

	sn.Checksums[f_name][c_idx] = calcd_checksum

	os.Rename(c_fn_path_t, c_fn_path)
	_, err = os.OpenFile(c_fn_path, os.O_RDONLY, 0666)
	if err != nil {
		os.Remove(c_fn_path)
		os.Remove(ch_fn_path)
		os.Remove(c_fn_path_t)
		os.Remove(ch_fn_path_t)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return fmt.Errorf("failed to move [%s] to [%s]", c_fn_path_t, c_fn_path)
	}

	wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: ""}}})
	return nil
}

func (sn *Storage_node) Handle_chunk_download(wh *wire.WireHandler, cdl *wire.ChunkDownload) error {
	f_name, f_type, c_idx := cdl.ChunkId.FName, cdl.ChunkId.FType, cdl.ChunkId.CIdx

	chunkmap_v, ok := sn.Chunkmap[f_name]
	if !ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
			Message: fmt.Sprintf("no chunk %s, 1", f_name),
		}}})
		return fmt.Errorf("no chunk %s, 1", f_name)
	}

	c_size, ok := chunkmap_v.CMap[cdl.ChunkId.CIdx]
	if !ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
			Message: fmt.Sprintf("no chunk %s_%d, 2", f_name, c_idx),
		}}})
		return fmt.Errorf("no chunk %s_%d, 2", f_name, c_idx)
	}

	checkmap_v, ok := sn.Checksums[f_name]
	if !ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
			Message: fmt.Sprintf("no checksum for chunks under %s", f_name),
		}}})
		return fmt.Errorf("no checksum for chunks under %s", f_name)
	}

	inmem_checksum, ok := checkmap_v[c_idx]
	if !ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
			Message: fmt.Sprintf("no checksum for chunk %s_%d, 2", f_name, c_idx),
		}}})
		return fmt.Errorf("no checksum for chunk %s_%d, 2", f_name, c_idx)
	}

	var ft string
	switch f_type {
	case wire.FileType_BIN:
		ft = "b"
	case wire.FileType_TXT:
		ft = "t"
	default:
		ft = "b"
	}

	c_filename := fmt.Sprintf("%s_%s%d", f_name, ft, c_idx)
	f, err := os.OpenFile(path.Join(sn.Dir, c_filename), os.O_RDONLY, 0666)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
			Message: err.Error(),
		}}})
		return err
	}
	defer f.Close()

	buf := bytes.NewBuffer(make([]byte, 0, c_size))
	calcd_checksum, err := util.WriteAndHash(f, c_size, buf)
	if err != nil {
		f.Close()
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
			Message: fmt.Sprintf("failed to hash file %s", path.Join(sn.Dir, c_filename)),
		}}})
		return err
	}

	err = util.VerifyChecksum(inmem_checksum, calcd_checksum)
	if err != nil {
		f.Close()
		os.Remove(path.Join(sn.Dir, c_filename))
		os.Remove(path.Join(sn.Dir, fmt.Sprintf("%sh", c_filename)))
		delete(sn.Chunkmap[f_name].CMap, c_idx)
		delete(sn.Checksums[f_name], c_idx)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{
			Message: fmt.Sprintf("corruption detected in file %s\n  in-memory checksum:  %x\n  calculated checksum: %x", path.Join(sn.Dir, c_filename), inmem_checksum, calcd_checksum),
		}}})
		return fmt.Errorf("corruption detected in file %s\n  in-memory checksum:  %x\n  calculated checksum: %x", path.Join(sn.Dir, c_filename), inmem_checksum, calcd_checksum)
	}

	fmt.Printf(" *** %s_%s%d, c_size %d, bytes in buf %d\n", f_name, ft, c_idx, c_size, buf.Len())
	fmt.Printf(" *** in-memory checksum:  %x\n *** calculated checksum: %x\n", inmem_checksum, calcd_checksum)

	return wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Cdl{Cdl: &wire.ChunkDownload{
		ChunkId: &wire.ChunkId{
			FName: f_name,
			FType: f_type,
			CIdx:  c_idx,
			CSize: c_size,
		},
		Checksum: calcd_checksum,
		Data:     buf.Bytes(),
	}}})
}

func (sn *Storage_node) Handle_chunk_replicate(wh *wire.WireHandler, crp *wire.ChunkReplicate) error {
	f_name, f_type, c_idx := crp.ChunkId.FName, crp.ChunkId.FType, crp.ChunkId.CIdx
	node_addrs, n := crp.Nodes, len(crp.Nodes)

	chunkmap_v, ok := sn.Chunkmap[f_name]
	if !ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return fmt.Errorf("no chunk %s_x", f_name)
	}

	var ft string
	switch f_type {
	case wire.FileType_BIN:
		ft = "b"
	case wire.FileType_TXT:
		ft = "t"
	default:
		ft = "b"
	}

	c_filename := fmt.Sprintf("%s_%s%d", f_name, ft, c_idx)
	c_size, ok := chunkmap_v.CMap[c_idx]
	if !ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return fmt.Errorf("no chunk %s", c_filename)
	}

	checkmap_v, ok := sn.Checksums[f_name]
	if !ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return fmt.Errorf("no checksum for chunks under '%s'", f_name)
	}

	inmem_checksum, ok := checkmap_v[c_idx]
	if !ok {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return fmt.Errorf("no checksum for chunk %s", c_filename)
	}

	f, err := os.OpenFile(path.Join(sn.Dir, c_filename), os.O_RDONLY, 0666)
	if err != nil {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return err
	}
	defer f.Close()

	buf := bytes.NewBuffer(make([]byte, 0, c_size))
	calcd_checksum, err := util.WriteAndHash(f, c_size, buf)
	if err != nil {
		f.Close()
		log.Printf("error hashing file %s: err %s\n", path.Join(sn.Dir, c_filename), err)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return err
	}

	err = util.VerifyChecksum(inmem_checksum, calcd_checksum)
	if err != nil {
		f.Close()
		os.Remove(path.Join(sn.Dir, c_filename))
		os.Remove(path.Join(sn.Dir, fmt.Sprintf("%sh", c_filename)))
		sn.Chunkmap[f_name] = nil
		delete(sn.Checksums[f_name], c_idx)
		log.Printf("corruption detected in file %s\n  in-memory checksum:  %x\n  calculated checksum: %x\nfile deleted\n", path.Join(sn.Dir, c_filename), inmem_checksum, calcd_checksum)
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return err
	}

	fail_count := 0
	var pers_err *error
	for _, node_addr := range node_addrs {
		conn, err := net.Dial("tcp", node_addr)
		if err != nil {
			fail_count++
			*pers_err = err
			continue
		}

		l_wh := wire.Construct_wirehandler(conn)
		defer l_wh.Close()

		err = l_wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Cul{Cul: &wire.ChunkUpload{
			ChunkId: &wire.ChunkId{
				FName: f_name,
				FType: f_type,
				CIdx:  c_idx,
				CSize: c_size,
			},
			Checksum: calcd_checksum,
			Data:     buf.Bytes(),
		}}})

		if err != nil {
			fail_count++
			*pers_err = err
			continue
		}

		res, err := l_wh.Receive()
		if err != nil {
			fail_count++
			*pers_err = err
			continue
		}

		switch res.Msg.(type) {
		case *wire.Wrapper_Ms:
			break
		case *wire.Wrapper_Rp:
			*pers_err = fmt.Errorf("error at upload node")
			continue
		default:
			*pers_err = fmt.Errorf("malformed response from upload node")
			continue
		}
	}

	if fail_count == n {
		wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Rp{Rp: &wire.Repeat{}}})
		return *pers_err
	}

	err = wh.Send(&wire.Wrapper{Msg: &wire.Wrapper_Ms{Ms: &wire.Message{Message: ""}}})
	return err
}

func (sn *Storage_node) Handle_file_delete(wh *wire.WireHandler, dl *wire.Delete) error {
	f_name := dl.Delete

	chunkmap_v, ok := sn.Chunkmap[f_name]
	if !ok || chunkmap_v == nil {
		return fmt.Errorf("%s:%s - no chunks for file %s", sn.Name, sn.CtrlPort, f_name)
	}

	var ft string
	switch chunkmap_v.FType {
	case wire.FileType_BIN:
		ft = "b"
	case wire.FileType_TXT:
		ft = "t"
	default:
		ft = "b"
	}

	var err error
	for ci := range chunkmap_v.CMap {
		err = os.Remove(path.Join(sn.Dir, fmt.Sprintf("%s_%s%d", f_name, ft, ci)))
		if err != nil {
			log.Println(err)
		}
		err = os.Remove(path.Join(sn.Dir, fmt.Sprintf("%s_%s%dh", f_name, ft, ci)))
		if err != nil {
			log.Println(err)
		}
	}

	delete(sn.Chunkmap, f_name)
	return nil
}
