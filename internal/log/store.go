package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (uint64, uint64, error) {
	// 데이터를 추가하기 전에 store의 크기 제한을 초과하지는 않는지 확인
	// 데이터를 추가하는 곳은 size의 다음 인덱스
	// 데이터 추가 시 big Endian
	s.mu.Lock()
	defer s.mu.Unlock()

	pos := s.size
	err := binary.Write(s.buf, enc, uint64(len(p))) // s.buf 사용 이유: 데이터를 버퍼에 저장한 후 한 번에 파일에 저장 ( 시스템콜 호출 횟수를 줄여 성능 개선 )
	if err != nil {
		return 0, 0, err
	}

	w, err := s.buf.Write(p) // s.buf 사용 이유: 데이터를 버퍼에 저장한 후 한 번에 파일에 저장 ( 시스템콜 호출 횟수를 줄여 성능 개선 )
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)

	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) ReadAt(b []byte, off int64) (int, error) {
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(b, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
