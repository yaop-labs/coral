package journal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

var ErrFull = errors.New("journal byte limit exceeded")

type Journal struct {
	mu       sync.Mutex
	f        *os.File
	maxBytes int64
	size     int64
}

type Envelope struct {
	Signal, Tenant string
	Payload        []byte
}

func EncodeEnvelope(e Envelope) []byte {
	b := make([]byte, 0, 5+len(e.Signal)+len(e.Tenant)+len(e.Payload))
	b = append(b, 1, byte(len(e.Signal)))
	b = append(b, e.Signal...)
	b = append(b, byte(len(e.Tenant)))
	b = append(b, e.Tenant...)
	b = append(b, e.Payload...)
	return b
}

func DecodeEnvelope(b []byte) (Envelope, error) {
	if len(b) < 3 || b[0] != 1 {
		return Envelope{}, fmt.Errorf("unsupported journal envelope")
	}
	i := 2
	ns := int(b[1])
	if i+ns+1 > len(b) {
		return Envelope{}, fmt.Errorf("truncated journal envelope")
	}
	e := Envelope{Signal: string(b[i : i+ns])}
	i += ns
	nt := int(b[i])
	i++
	if i+nt > len(b) {
		return Envelope{}, fmt.Errorf("truncated journal envelope")
	}
	e.Tenant = string(b[i : i+nt])
	i += nt
	e.Payload = append([]byte(nil), b[i:]...)
	return e, nil
}

func Open(path string, maxBytes int64) (*Journal, error) {
	if maxBytes <= 0 {
		maxBytes = 1 << 30
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if st.Size() > maxBytes {
		_ = f.Close()
		return nil, ErrFull
	}
	return &Journal{f: f, maxBytes: maxBytes, size: st.Size()}, nil
}

func (j *Journal) Append(payload []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	recordSize := int64(8 + len(payload))
	if j.size+recordSize > j.maxBytes {
		return ErrFull
	}
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(payload))
	if _, err := j.f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := j.f.Write(payload); err != nil {
		return err
	}
	if err := j.f.Sync(); err != nil {
		return err
	}
	j.size += recordSize
	return nil
}

func (j *Journal) Replay(fn func([]byte) error) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(j.f)
	for {
		var hdr [8]byte
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		if n > uint32(j.maxBytes) {
			return fmt.Errorf("journal record too large")
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(hdr[4:]) {
			return fmt.Errorf("journal checksum mismatch")
		}
		if err := fn(payload); err != nil {
			return err
		}
	}
	return nil
}

// Recover replays valid records and truncates an incomplete/corrupt tail from
// an interrupted append. Checksum failures in a complete record remain errors.
func (j *Journal) Recover(fn func([]byte) error) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(j.f)
	var offset int64
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err == io.EOF {
			j.size = offset
			return nil
		} else if err != nil {
			_ = j.f.Truncate(offset)
			j.size = offset
			return nil
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			_ = j.f.Truncate(offset)
			j.size = offset
			return nil
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(hdr[4:]) {
			return fmt.Errorf("journal checksum mismatch")
		}
		if err := fn(payload); err != nil {
			return err
		}
		offset += int64(8 + n)
	}
}

func (j *Journal) Close() error { j.mu.Lock(); defer j.mu.Unlock(); return j.f.Close() }

func (j *Journal) Stats() (bytes, maxBytes int64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.size, j.maxBytes
}

// Compact removes all records after the caller has durably replayed them.
func (j *Journal) Compact() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.f.Truncate(0); err != nil {
		return err
	}
	if _, err := j.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if err := j.f.Sync(); err != nil {
		return err
	}
	j.size = 0
	return nil
}
