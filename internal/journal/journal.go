package journal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var ErrFull = errors.New("journal byte limit exceeded")

type Journal struct {
	mu       sync.Mutex
	f        *os.File
	path     string
	syncFn   func() error
	maxBytes int64
	size     int64
}

type Envelope struct {
	Signal, Tenant  string
	Payload         []byte
	CreatedUnixNano int64
}

func EncodeEnvelope(e Envelope) []byte {
	b := make([]byte, 0, 16+len(e.Signal)+len(e.Tenant)+len(e.Payload))
	b = append(b, 2, byte(len(e.Signal)))
	b = append(b, e.Signal...)
	b = append(b, byte(len(e.Tenant)))
	b = append(b, e.Tenant...)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(e.CreatedUnixNano))
	b = append(b, ts[:]...)
	b = append(b, e.Payload...)
	return b
}

func DecodeEnvelope(b []byte) (Envelope, error) {
	if len(b) < 3 || (b[0] != 1 && b[0] != 2) {
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
	if b[0] == 2 {
		if i+8 > len(b) {
			return Envelope{}, fmt.Errorf("truncated journal envelope")
		}
		e.CreatedUnixNano = int64(binary.BigEndian.Uint64(b[i : i+8]))
		i += 8
	}
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
	return &Journal{f: f, path: path, syncFn: f.Sync, maxBytes: maxBytes, size: st.Size()}, nil
}

func (j *Journal) Append(payload []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if int64(len(payload)) > j.maxBytes-8 {
		return ErrFull
	}
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
	if err := j.syncFn(); err != nil {
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
	tmp, err := os.CreateTemp(filepath.Dir(j.path), ".coral-journal-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = j.syncFn(); err != nil {
		return err
	}
	if err = os.Rename(tmpPath, j.path); err != nil {
		return err
	}
	if err = j.f.Close(); err != nil {
		return err
	}
	j.f, err = os.OpenFile(j.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	j.syncFn = j.f.Sync
	j.size = 0
	return nil
}

func (j *Journal) CompactOlderThan(age time.Duration) error {
	if age <= 0 {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	raw, err := io.ReadAll(j.f)
	if err != nil {
		return err
	}
	now := time.Now()
	kept := make([]byte, 0, len(raw))
	off := 0
	for off < len(raw) {
		if len(raw)-off < 8 {
			return io.ErrUnexpectedEOF
		}
		n := int(binary.BigEndian.Uint32(raw[off : off+4]))
		end := off + 8 + n
		if n < 0 || end > len(raw) {
			return io.ErrUnexpectedEOF
		}
		payload := raw[off+8 : end]
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(raw[off+4:off+8]) {
			return fmt.Errorf("journal checksum mismatch")
		}
		keep := true
		if env, e := DecodeEnvelope(payload); e == nil && env.CreatedUnixNano > 0 {
			keep = now.Sub(time.Unix(0, env.CreatedUnixNano)) <= age
		}
		if keep {
			kept = append(kept, raw[off:end]...)
		}
		off = end
	}
	if err := j.f.Truncate(0); err != nil {
		return err
	}
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := j.f.Write(kept); err != nil {
		return err
	}
	if err := j.syncFn(); err != nil {
		return err
	}
	j.size = int64(len(kept))
	return nil
}
