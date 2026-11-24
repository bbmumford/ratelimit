package ratelimit

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// RateLimitMode names the algorithm. Only ModeTokenBucket is implemented by this in-memory
// limiter; the sliding-window/fixed modes are the (deferred) store-backed variant and are kept
// as declared constants for config compatibility.
type RateLimitMode string

const (
	ModeTokenBucket   RateLimitMode = "token_bucket"
	ModeSlidingWindow RateLimitMode = "sliding_window"
	ModeFixed         RateLimitMode = "fixed_window"
)

// RateLimitConfig configures a named rate limit. Dimensions are OPAQUE labels — the limiter
// does not interpret them; the consumer decides what "tenant"/"user"/"ip" mean by populating
// the matching keys in RateLimitContext.Dimensions.
type RateLimitConfig struct {
	Mode       RateLimitMode
	Capacity   int
	RefillRate int
	Interval   time.Duration
	Dimensions []string // opaque dimension labels; the bucket key is built from these, in order
	TokenCost  int
	Name       string
	Skip       func(ctx *RateLimitContext) bool
}

// RateLimitContext carries the request's opaque dimension values. A consumer supplies whatever
// dimensions it wants ({"ip": ..., "tenant": ...}); the limiter never names an axis.
type RateLimitContext struct {
	Dimensions map[string]string
}

// Key builds the bucket key from the named dimension labels, in order. Empty labels → a single
// global bucket. An absent dimension contributes an empty value, so a missing dimension yields
// a distinct, stable bucket (fail-safe: it does not silently merge with a populated one).
func (c *RateLimitContext) Key(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	var b strings.Builder
	for i, label := range labels {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(label)
		b.WriteByte(':')
		if c != nil && c.Dimensions != nil {
			b.WriteString(c.Dimensions[label])
		}
	}
	return b.String()
}

// bucketEntry wraps a TokenBucket with an access timestamp for stale-eviction.
//
// lastAccess holds UnixNano and is atomic because getOrCreateBucket refreshes it on the
// read-locked hit path, where every concurrent request on the key writes it, and
// evictStale reads it; a read lock does not order those writes against each other.
type bucketEntry struct {
	bucket     *TokenBucket
	lastAccess atomic.Int64
}

// RateLimiter manages named configs and their per-key token buckets. Thread-safe.
type RateLimiter struct {
	mu      sync.RWMutex
	buckets map[string]*bucketEntry
	configs map[string]*RateLimitConfig
	maxAge  time.Duration
	// maxKeys caps len(buckets); 0 leaves the map bounded only by the maxAge sweep.
	maxKeys int
	// fullUntil is when a full map's soonest bucket is back at capacity: until then a new key is
	// refused without scanning the map again, so a stream of new keys costs one scan per refill.
	fullUntil time.Time
	stopCh    chan struct{}
}

// ErrNoRoom is Protect's answer for a new key that a bounded limiter's full key map cannot hold:
// every bucket in it is still below capacity, and dropping one would hand that key a full bucket
// on its next request, resetting its limit. The request is not judged; a caller limiting abuse
// refuses it.
var ErrNoRoom = errors.New("ratelimit: the key map is full of buckets still below capacity")

// NewRateLimiter creates a limiter with a 10-minute unused-bucket expiry.
func NewRateLimiter() *RateLimiter { return NewRateLimiterWithMaxAge(10 * time.Minute) }

// NewRateLimiterWithMaxAge creates a limiter with a custom unused-bucket expiry and starts its
// cleanup goroutine (stop it with Stop to avoid a leak).
func NewRateLimiterWithMaxAge(maxAge time.Duration) *RateLimiter {
	return NewRateLimiterWithBounds(maxAge, 0)
}

// NewRateLimiterWithBounds is NewRateLimiterWithMaxAge with the key map capped at maxKeys
// buckets (maxKeys <= 0: uncapped). It exists for a limiter keyed by a value its caller does
// not choose, such as a public edge's source address: a caller rotating that value between
// sweeps would otherwise grow the map without bound. A new key that meets a full map drops
// every bucket already back at capacity (a new bucket starts full, so nothing is lost) and
// never one still below it, whose spent tokens are the limit's state: if none is back at
// capacity, the new key is refused with ErrNoRoom until the soonest bucket refills.
func NewRateLimiterWithBounds(maxAge time.Duration, maxKeys int) *RateLimiter {
	if maxKeys < 0 {
		maxKeys = 0
	}
	rl := &RateLimiter{
		buckets: make(map[string]*bucketEntry),
		configs: make(map[string]*RateLimitConfig),
		maxAge:  maxAge,
		maxKeys: maxKeys,
		stopCh:  make(chan struct{}),
	}
	go rl.cleanup()
	return rl
}

// Stop halts the cleanup goroutine.
func (rl *RateLimiter) Stop() {
	if rl != nil {
		close(rl.stopCh)
	}
}

// Register adds a named config.
func (rl *RateLimiter) Register(name string, config *RateLimitConfig) {
	if rl == nil {
		return
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	config.Name = name
	rl.configs[name] = config
}

// Registered reports whether a config is registered under name.
//
// It exists because Protect fails OPEN on an unregistered name and says so only by
// allowing the request: it returns a nil error either way, so a caller cannot tell a
// request that passed a limit from one that met no limit at all. A name that is
// mounted but never registered — a typo, a registration dropped while moving a
// consumer off a shared package-level limiter — therefore serves unlimited traffic
// with a green build and no log line.
//
// Call it where that mistake is still cheap: at composition, over the names actually
// mounted, before serving. Checking per request would only rediscover the same wiring
// fact on every call.
func (rl *RateLimiter) Registered(name string) bool {
	if rl == nil {
		return false
	}
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	_, ok := rl.configs[name]
	return ok
}

// Dimensions returns the opaque dimension labels a registered config keys its buckets
// on, and whether that name is registered at all. The slice is a copy.
//
// It exists so a composition root can check that whatever supplies the per-request
// dimension VALUES covers every label the config keys on. A label with no supplier
// contributes an empty value to every request, and Key builds one bucket for all of
// them — the limit silently becomes global instead of per-dimension, which looks
// identical to a working limiter until one caller exhausts everyone's tokens.
func (rl *RateLimiter) Dimensions(name string) ([]string, bool) {
	if rl == nil {
		return nil, false
	}
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	config, ok := rl.configs[name]
	if !ok {
		return nil, false
	}
	out := make([]string, len(config.Dimensions))
	copy(out, config.Dimensions)
	return out, true
}

// RegisteredNames returns the registered config names in no particular order, so a
// composition root can diff what it registered against what it mounted rather than
// probing one name at a time. The slice is a copy; mutating it changes no state.
func (rl *RateLimiter) RegisteredNames() []string {
	if rl == nil {
		return nil
	}
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	names := make([]string, 0, len(rl.configs))
	for n := range rl.configs {
		names = append(names, n)
	}
	return names
}

// Protect reports whether a request under the named config is allowed, plus the remaining
// tokens and the reset duration. An unregistered name fails OPEN (allow) — registration is the
// consumer's explicit act, so an unconfigured limit does not block traffic.
func (rl *RateLimiter) Protect(name string, ctx *RateLimitContext) (allowed bool, remaining int, reset time.Duration, err error) {
	rl.mu.RLock()
	config, ok := rl.configs[name]
	rl.mu.RUnlock()
	if !ok {
		return true, -1, 0, nil
	}
	if config.Skip != nil && config.Skip(ctx) {
		return true, -1, 0, nil
	}
	key := name + ":" + ctx.Key(config.Dimensions)
	bucket, err := rl.getOrCreateBucket(key, config)
	if err != nil {
		return false, 0, 0, err
	}
	cost := config.TokenCost
	if cost <= 0 {
		cost = 1
	}
	allowed = bucket.Allow(float64(cost))
	remainingInt, resetSeconds, _ := bucket.State()
	return allowed, remainingInt, time.Duration(resetSeconds) * time.Second, nil
}

// getOrCreateBucket returns the bucket for key, creating it under the write lock if absent; a
// new key a full bounded map cannot hold is refused with ErrNoRoom.
func (rl *RateLimiter) getOrCreateBucket(key string, config *RateLimitConfig) (*TokenBucket, error) {
	now := time.Now()
	rl.mu.RLock()
	entry, ok := rl.buckets[key]
	rl.mu.RUnlock()
	if ok {
		entry.lastAccess.Store(now.UnixNano())
		return entry.bucket, nil
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if entry, ok = rl.buckets[key]; ok {
		entry.lastAccess.Store(now.UnixNano())
		return entry.bucket, nil
	}
	if rl.maxKeys > 0 && len(rl.buckets) >= rl.maxKeys && !rl.makeRoomLocked(now) {
		return nil, ErrNoRoom
	}
	bucket := NewTokenBucket(float64(config.Capacity), float64(config.RefillRate), config.Interval)
	entry = &bucketEntry{bucket: bucket}
	entry.lastAccess.Store(now.UnixNano())
	rl.buckets[key] = entry
	return bucket, nil
}

// makeRoomLocked drops every bucket of a full key map that is back at capacity, as
// NewRateLimiterWithBounds describes, and reports whether the map now has room. When it has
// none it records when the soonest bucket will be back at capacity, and before then answers
// false without scanning. Caller holds rl.mu for writing.
func (rl *RateLimiter) makeRoomLocked(now time.Time) bool {
	if now.Before(rl.fullUntil) {
		return false
	}
	soonest := time.Duration(-1)
	for key, entry := range rl.buckets {
		reset := entry.bucket.Reset()
		if reset == 0 {
			delete(rl.buckets, key)
			continue
		}
		if soonest < 0 || reset < soonest {
			soonest = reset
		}
	}
	if len(rl.buckets) < rl.maxKeys {
		return true
	}
	rl.fullUntil = now.Add(soonest)
	return false
}

// cleanup evicts stale buckets on a ticker until Stop.
func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-rl.stopCh:
			return
		case <-ticker.C:
			rl.evictStale()
		}
	}
}

// evictStale removes buckets untouched within maxAge that are back at capacity. A bucket still
// below capacity is kept however long it has been idle: dropping it would hand its key a full
// bucket on the next request, resetting the limit before it has refilled.
func (rl *RateLimiter) evictStale() {
	cutoff := time.Now().Add(-rl.maxAge).UnixNano()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for key, entry := range rl.buckets {
		if entry.lastAccess.Load() < cutoff && entry.bucket.Reset() == 0 {
			delete(rl.buckets, key)
		}
	}
}

// BucketCount returns the number of live buckets (monitoring).
func (rl *RateLimiter) BucketCount() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return len(rl.buckets)
}
