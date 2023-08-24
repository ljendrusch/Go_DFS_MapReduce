package wire

import (
	"encoding/binary"
	"net"

	"google.golang.org/protobuf/proto"
)

type WireHandler struct {
	conn net.Conn
}

func Construct_wirehandler(conn net.Conn) *WireHandler {
	r := &WireHandler{
		conn: conn,
	}
	return r
}

func (wh *WireHandler) Conn() net.Conn {
	return wh.conn
}

func (wh *WireHandler) readN(buf []byte) error {
	bytesRead := uint64(0)
	for bytesRead < uint64(len(buf)) {
		n, err := wh.conn.Read(buf[bytesRead:])
		if err != nil {
			return err
		}
		bytesRead += uint64(n)
	}
	return nil
}

func (wh *WireHandler) writeN(buf []byte) error {
	bytesWritten := uint64(0)
	for bytesWritten < uint64(len(buf)) {
		n, err := wh.conn.Write(buf[bytesWritten:])
		if err != nil {
			return err
		}
		bytesWritten += uint64(n)
	}
	return nil
}

func (wh *WireHandler) Receive() (*Wrapper, error) {
	prefix := make([]byte, 8)
	wh.readN(prefix)

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	wh.readN(payload)

	data := &Wrapper{}
	err := proto.Unmarshal(payload, data)
	return data, err
}

func (wh *WireHandler) Send(w *Wrapper) error {
	serialized, err := proto.Marshal(w)
	if err != nil {
		return err
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	wh.writeN(prefix)
	wh.writeN(serialized)

	return nil
}

func (m *WireHandler) Close() {
	if m.conn != nil {
		m.conn.Close()
	}
}
