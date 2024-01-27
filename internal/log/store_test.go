package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write) + lenWidth)
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "test_store_append_read")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := 0; i < 3; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, width, n)
		require.Equal(t, pos, uint64(i)*width)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	for i := 0; i < 3; i++ {
		data, err := s.Read(uint64(i) * width)
		require.NoError(t, err)
		require.Equal(t, write, data)
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	var off int64
	for i := 0; i < 3; i++ {
		size := make([]byte, lenWidth)
		n, err := s.ReadAt(size, int64(i)*int64(width))
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		require.Equal(t, uint64(len(write)), enc.Uint64(size))

		off += lenWidth

		data := make([]byte, len(write))
		n, err = s.ReadAt(data, off)
		require.NoError(t, err)
		require.Equal(t, len(write), n)
		require.Equal(t, write, data)

		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
