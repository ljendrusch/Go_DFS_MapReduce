package util

import (
	"crypto/md5"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
)

func WriteAndHash(src io.Reader, n int64, dsts ...io.Writer) ([]byte, error) {
	var writer io.Writer

	h := md5.New()
	dsts = append(dsts, h)
	writer = io.MultiWriter(dsts...)

	_, err := io.CopyN(writer, src, n)
	if err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func Hash(src io.Reader, n int64) ([]byte, error) {
	h := md5.New()
	_, err := io.CopyN(h, src, n)
	if err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func VerifyChecksum(sentCheck []byte, calcdCheck []byte) error {
	if reflect.DeepEqual(calcdCheck, sentCheck) {
		return nil
	} else {
		return fmt.Errorf("checksum mismatch")
	}
}

func KVBufSort(buf []byte) []byte {
	slurp := string(buf)

	var kvs []string
	left := 0
	second_nl := false
	for i, ch := range slurp {
		if ch == '\n' {
			if !second_nl {
				second_nl = true
			} else {
				second_nl = false

				kvs = append(kvs, slurp[left:i+1])
				left = i + 1
			}
		}
	}

	sort.Strings(kvs)

	return []byte(strings.Join(kvs, ""))
}
