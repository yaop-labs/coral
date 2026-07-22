package journal

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
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
var ErrEnvelopeTooLarge = errors.New("journal envelope field too large")
var ErrRecordTooLarge = errors.New("journal record too large")
var ErrDuplicateRecordID = errors.New("duplicate journal record id")

const maxJournalRecordBytes = 64 << 20

type Journal struct {
	mu       sync.Mutex
	f        *os.File
	path     string
	syncFn   func() error
	maxBytes int64
	size     int64
}

type Envelope struct {
	Signal, Tenant, DeliveryID, RecordID string
	RequestDigest                        string
	FailureReason                        string
	Payload                              []byte
	CreatedUnixNano                      int64
	QuarantinedUnixNano                  int64
}

// NewRecordID returns a stable, opaque identity for one durable journal
// record. It is deliberately independent of the optional Wisp delivery ID:
// multiple attempts of one Wisp delivery and traffic from ordinary OTLP
// clients still need an internal lifecycle identity.
func NewRecordID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate journal record id: %w", err)
	}
	return hex.EncodeToString(id[:]), nil
}

func EncodeEnvelope(e Envelope) []byte {
	if len(e.Signal) > 255 || len(e.Tenant) > 255 || len(e.DeliveryID) > 255 ||
		len(e.RecordID) > 255 || len(e.RequestDigest) > 255 || len(e.FailureReason) > 4096 {
		return nil
	}
	version := byte(2)
	if e.DeliveryID != "" {
		version = 3
	}
	if e.RecordID != "" {
		version = 4
	}
	if e.RequestDigest != "" || e.FailureReason != "" || e.QuarantinedUnixNano != 0 {
		version = 5
	}
	b := make([]byte, 0, 30+len(e.Signal)+len(e.Tenant)+len(e.DeliveryID)+len(e.RecordID)+len(e.RequestDigest)+len(e.FailureReason)+len(e.Payload))
	b = append(b, version, byte(len(e.Signal)))
	b = append(b, e.Signal...)
	b = append(b, byte(len(e.Tenant)))
	b = append(b, e.Tenant...)
	if version >= 3 {
		b = append(b, byte(len(e.DeliveryID)))
		b = append(b, e.DeliveryID...)
	}
	if version >= 4 {
		b = append(b, byte(len(e.RecordID)))
		b = append(b, e.RecordID...)
	}
	if version == 5 {
		b = append(b, byte(len(e.RequestDigest)))
		b = append(b, e.RequestDigest...)
		var reasonLen [2]byte
		binary.BigEndian.PutUint16(reasonLen[:], uint16(len(e.FailureReason)))
		b = append(b, reasonLen[:]...)
		b = append(b, e.FailureReason...)
		var quarantined [8]byte
		binary.BigEndian.PutUint64(quarantined[:], uint64(e.QuarantinedUnixNano))
		b = append(b, quarantined[:]...)
	}
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(e.CreatedUnixNano))
	b = append(b, ts[:]...)
	b = append(b, e.Payload...)
	return b
}

func DecodeEnvelope(b []byte) (Envelope, error) {
	if len(b) < 3 || (b[0] != 1 && b[0] != 2 && b[0] != 3 && b[0] != 4 && b[0] != 5) {
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
	if b[0] >= 3 {
		if i+1 > len(b) {
			return Envelope{}, fmt.Errorf("truncated journal envelope")
		}
		nd := int(b[i])
		i++
		if i+nd > len(b) {
			return Envelope{}, fmt.Errorf("truncated journal envelope")
		}
		e.DeliveryID = string(b[i : i+nd])
		i += nd
	}
	if b[0] >= 4 {
		if i+1 > len(b) {
			return Envelope{}, fmt.Errorf("truncated journal envelope")
		}
		nr := int(b[i])
		i++
		if i+nr > len(b) {
			return Envelope{}, fmt.Errorf("truncated journal envelope")
		}
		e.RecordID = string(b[i : i+nr])
		i += nr
	}
	if b[0] == 5 {
		if i+1 > len(b) {
			return Envelope{}, fmt.Errorf("truncated journal envelope")
		}
		nd := int(b[i])
		i++
		if i+nd+2 > len(b) {
			return Envelope{}, fmt.Errorf("truncated journal envelope")
		}
		e.RequestDigest = string(b[i : i+nd])
		i += nd
		nr := int(binary.BigEndian.Uint16(b[i : i+2]))
		i += 2
		if nr > 4096 || i+nr+8 > len(b) {
			return Envelope{}, fmt.Errorf("truncated journal envelope")
		}
		e.FailureReason = string(b[i : i+nr])
		i += nr
		e.QuarantinedUnixNano = int64(binary.BigEndian.Uint64(b[i : i+8]))
		i += 8
	}
	if b[0] >= 2 {
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
	if len(payload) > maxJournalRecordBytes {
		return ErrRecordTooLarge
	}
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

// AppendEnvelope assigns an internal identity when needed and durably appends
// the encoded envelope. The returned value is the exact envelope stored on
// disk and can be carried through the delivery pipeline.
func (j *Journal) AppendEnvelope(e Envelope) (Envelope, error) {
	if e.RecordID == "" {
		id, err := NewRecordID()
		if err != nil {
			return Envelope{}, err
		}
		e.RecordID = id
	}
	if e.CreatedUnixNano == 0 {
		e.CreatedUnixNano = time.Now().UnixNano()
	}
	payload := EncodeEnvelope(e)
	if payload == nil {
		return Envelope{}, ErrEnvelopeTooLarge
	}
	if err := j.Append(payload); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

// Acknowledge atomically removes the record with recordID while retaining
// every other record. It is idempotent: acknowledging an already removed or
// unknown identity returns (false, nil).
//
// A crash before the rename leaves the old journal intact. A crash after the
// rename may replay an already delivered record if directory persistence was
// not yet confirmed, which is conservative at-least-once behaviour.
func (j *Journal) Acknowledge(recordID string) (bool, error) {
	if recordID == "" {
		return false, errors.New("journal record id is empty")
	}
	j.mu.Lock()
	defer j.mu.Unlock()

	raw, err := j.readAllLocked()
	if err != nil {
		return false, err
	}
	kept := make([]byte, 0, len(raw))
	found := false
	if err := walkRecords(raw, func(record, payload []byte) error {
		env, err := DecodeEnvelope(payload)
		if err != nil {
			return err
		}
		if env.RecordID == recordID {
			if found {
				return ErrDuplicateRecordID
			}
			found = true
			return nil
		}
		kept = append(kept, record...)
		return nil
	}); err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	if err := j.rewriteLocked(kept); err != nil {
		return false, err
	}
	return true, nil
}

// EnsureRecordIDs upgrades legacy envelopes in place before they are
// dispatched. This makes replayed work addressable by the same stable identity
// on every subsequent restart. Existing version-4 records are left byte-for-
// byte unchanged.
func (j *Journal) EnsureRecordIDs() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	raw, err := j.readAllLocked()
	if err != nil {
		return err
	}
	kept := make([]byte, 0, len(raw))
	seen := make(map[string]struct{})
	changed := false
	if err := walkRecords(raw, func(record, payload []byte) error {
		env, err := DecodeEnvelope(payload)
		if err != nil {
			return err
		}
		if env.RecordID == "" {
			env.RecordID, err = NewRecordID()
			if err != nil {
				return err
			}
			changed = true
			encoded := EncodeEnvelope(env)
			if encoded == nil {
				return ErrEnvelopeTooLarge
			}
			if len(encoded) > maxJournalRecordBytes {
				return ErrRecordTooLarge
			}
			kept = appendRecord(kept, encoded)
		} else {
			kept = append(kept, record...)
		}
		if _, exists := seen[env.RecordID]; exists {
			return ErrDuplicateRecordID
		}
		seen[env.RecordID] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if int64(len(kept)) > j.maxBytes {
		return ErrFull
	}
	return j.rewriteLocked(kept)
}

func appendRecord(dst, payload []byte) []byte {
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(payload))
	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

func (j *Journal) readAllLocked() ([]byte, error) {
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return io.ReadAll(j.f)
}

func walkRecords(raw []byte, fn func(record, payload []byte) error) error {
	for off := 0; off < len(raw); {
		if len(raw)-off < 8 {
			return io.ErrUnexpectedEOF
		}
		n := uint64(binary.BigEndian.Uint32(raw[off : off+4]))
		if n > maxJournalRecordBytes {
			return ErrRecordTooLarge
		}
		end64 := uint64(off) + 8 + n
		if end64 > uint64(len(raw)) {
			return io.ErrUnexpectedEOF
		}
		end := int(end64)
		payload := raw[off+8 : end]
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(raw[off+4:off+8]) {
			return fmt.Errorf("journal checksum mismatch")
		}
		if err := fn(raw[off:end], payload); err != nil {
			return err
		}
		off = end
	}
	return nil
}

func (j *Journal) rewriteLocked(kept []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(j.path), ".coral-journal-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err = tmp.Close(); err != nil {
		return err
	}
	tmp, err = os.OpenFile(tmpPath, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if _, err = tmp.Write(kept); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = j.syncFn(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = os.Rename(tmpPath, j.path); err != nil {
		_ = tmp.Close()
		return err
	}
	closeErr := j.f.Close()
	j.f = tmp
	j.syncFn = tmp.Sync
	j.size = int64(len(kept))
	dirErr := syncDirectory(filepath.Dir(j.path))
	if err = errors.Join(closeErr, dirErr); err != nil {
		return err
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
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
		if n > uint32(j.maxBytes) || n > maxJournalRecordBytes {
			return ErrRecordTooLarge
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
		if n > uint32(j.maxBytes) || n > maxJournalRecordBytes {
			return ErrRecordTooLarge
		}
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

// RecordStats returns bounded operational state without exposing payloads.
// It streams the file under the journal lock so memory use is independent of
// journal capacity.
func (j *Journal) RecordStats() (records uint64, oldest time.Time, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err = j.f.Seek(0, io.SeekStart); err != nil {
		return 0, time.Time{}, err
	}
	r := bufio.NewReader(j.f)
	for {
		var hdr [8]byte
		if _, err = io.ReadFull(r, hdr[:]); err == io.EOF {
			return records, oldest, nil
		} else if err != nil {
			return records, oldest, err
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		if n > uint32(j.maxBytes) || n > maxJournalRecordBytes {
			return records, oldest, ErrRecordTooLarge
		}
		payload := make([]byte, n)
		if _, err = io.ReadFull(r, payload); err != nil {
			return records, oldest, err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(hdr[4:]) {
			return records, oldest, fmt.Errorf("journal checksum mismatch")
		}
		records++
		if env, decodeErr := DecodeEnvelope(payload); decodeErr == nil && env.CreatedUnixNano > 0 {
			created := time.Unix(0, env.CreatedUnixNano)
			if oldest.IsZero() || created.Before(oldest) {
				oldest = created
			}
		}
	}
}

// LookupEnvelope finds one stable record identity. The returned envelope owns
// its payload bytes and remains valid after the journal lock is released.
func (j *Journal) LookupEnvelope(recordID string) (Envelope, bool, error) {
	if recordID == "" {
		return Envelope{}, false, nil
	}
	var found Envelope
	err := j.Replay(func(raw []byte) error {
		env, err := DecodeEnvelope(raw)
		if err != nil {
			return err
		}
		if env.RecordID == recordID {
			found = env
		}
		return nil
	})
	if err != nil {
		return Envelope{}, false, err
	}
	return found, found.RecordID != "", nil
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
	tmp, err := os.CreateTemp(filepath.Dir(j.path), ".coral-journal-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err = tmp.Write(kept); err != nil {
		_ = tmp.Close()
		return err
	}
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
	j.size = int64(len(kept))
	return nil
}
