package log

import (
	"fmt"
	"io"
	"os"
	"testing"

	api "github.com/alaflca/elevenlog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegemnt(t *testing.T) {
	dir, err := os.MkdirTemp("./", "segemnt_test")
	require.NoError(t, err)

	c := Config{}
	c.Segment.MaxIndexBytes = entWidth * 4
	c.Segment.MaxStoreBytes = 1024
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)

	require.False(t, s.isMaxed())
	for i := 0; i < 4; i++ {
		record := &api.Record{Value: []byte(fmt.Sprintf("hello, world %d", i))}
		off, err := s.Append(record)
		require.NoError(t, err)

		record, err = s.Read(off)
		require.NoError(t, err)
		require.Equal(t, off, record.Offset)
	}

	record := &api.Record{Value: []byte("hello, world 4")}
	_, err = s.Append(record)
	require.Equal(t, err, io.EOF)
	require.True(t, s.isMaxed())

	err = s.Close()
	require.NoError(t, err)

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	record, err = s.Read(15)
	require.NoError(t, err)
	require.Equal(t, uint64(19), record.Offset)
	require.Equal(t, []byte("hello, world 3"), record.Value)
}
