package otlp

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

type dedupResult uint8

const (
	dedupNew dedupResult = iota
	dedupHit
	dedupConflict
)

type dedupEntry struct {
	hash    [32]byte
	expires time.Time
}

// dedupWindow is bounded by entries and TTL; keys are tenant/signal scoped.
type dedupWindow struct {
	mu    sync.Mutex
	max   int
	ttl   time.Duration
	now   func() time.Time
	items map[string]dedupEntry
}

func newDedupWindow(max int, ttl time.Duration) *dedupWindow {
	if max <= 0 {
		max = 100000
	}
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	return &dedupWindow{max: max, ttl: ttl, now: time.Now, items: make(map[string]dedupEntry)}
}

func (d *dedupWindow) check(tenant, signal, id string, payload []byte) dedupResult {
	return d.checkLocked(tenant, signal, id, payload, true)
}

func (d *dedupWindow) lookup(tenant, signal, id string, payload []byte) dedupResult {
	return d.checkLocked(tenant, signal, id, payload, false)
}

func (d *dedupWindow) checkLocked(tenant, signal, id string, payload []byte, remember bool) dedupResult {
	if id == "" {
		return dedupNew
	}
	key := tenant + "\x00" + signal + "\x00" + id
	hash := sha256.Sum256(payload)
	now := d.now()
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, v := range d.items {
		if !v.expires.After(now) {
			delete(d.items, k)
		}
	}
	if old, ok := d.items[key]; ok {
		if old.hash == hash {
			return dedupHit
		}
		return dedupConflict
	}
	if !remember {
		return dedupNew
	}
	if len(d.items) >= d.max {
		for k := range d.items {
			delete(d.items, k)
			break
		}
	}
	d.items[key] = dedupEntry{hash: hash, expires: now.Add(d.ttl)}
	return dedupNew
}

func (d *dedupWindow) remember(tenant, signal, id string, payload []byte) {
	_ = d.check(tenant, signal, id, payload)
}

func (r dedupResult) String() string {
	switch r {
	case dedupHit:
		return "hit"
	case dedupConflict:
		return "conflict"
	default:
		return "new"
	}
}
func digestHex(payload []byte) string { h := sha256.Sum256(payload); return hex.EncodeToString(h[:]) }
