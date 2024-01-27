package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

const lenWidth = 8

type store struct {
	file *os.File // store file
	mu   sync.Mutex
	buf  *bufio.Writer // store buffer
	size uint64        // store file size
}

func newStore(f *os.File) (*store, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return &store{
		file: f,
		buf:  bufio.NewWriter(f),
		size: uint64(fi.Size()),
	}, nil
}

func (s *store) Append(data []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// get position in store file
	pos = s.size

	// write data size to store buffer
	if err = binary.Write(s.buf, enc, uint64(len(data))); err != nil {
		return 0, 0, err
	}

	// write data to store buffer
	w, err := s.buf.Write(data)
	if err != nil {
		return 0, 0, err
	}

	// update size of store file
	w += lenWidth
	s.size += uint64(w)

	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) (data []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush store buffer to store file before reading from store file
	if err = s.buf.Flush(); err != nil {
		return nil, err
	}

	// Read data size from store file
	wb := make([]byte, lenWidth)
	err = binary.Read(s.file, enc, wb)
	if _, err = s.file.ReadAt(wb, int64(pos)); err != nil {
		return nil, err
	}
	w := enc.Uint64(wb)

	// Read data from store file
	data = make([]byte, w)
	if _, err = s.file.ReadAt(data, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return data, nil
}

func (s *store) ReadAt(b []byte, off int64) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush store buffer to store file before reading from store file
	if err = s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.file.ReadAt(b, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush store buffer to store file before closing store file
	if err := s.buf.Flush(); err != nil {
		return err
	}

	//Close store file
	return s.file.Close()
}
