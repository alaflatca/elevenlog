package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp("./", "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Equal(t, err, io.EOF)
	require.Equal(t, idx.Name(), f.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, entrie := range entries {
		err = idx.Write(entrie.Off, entrie.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(entrie.Off))
		require.NoError(t, err)
		require.Equal(t, pos, entrie.Pos)
	}

	_, _, err = idx.Read(2)
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)

	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
	f.Close()
	idx.Close()

	data, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	t.Log("data: ", len(data))
	t.Logf("data: % 0x", data)
}
