package ratelimit

import (
	"strings"
	"sync"
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
type bucketEntry struct {
	bucket     *TokenBucket
	lastAccess time.Time
}

// RateLimiter manages named configs and their per-key token buckets. Thread-safe.
type RateLimiter struct {
	mu      sync.RWMutex
	buckets map[string]*bucketEntry
	configs map[string]*RateLimitConfig
	maxAge  time.Duration
	stopCh  chan struct{}
}

// NewRateLimiter creates a limiter with a 10-minute unused-bucket expiry.
func NewRateLimiter() *RateLimiter { return NewRateLimiterWithMaxAge(10 * time.Minute) }

// NewRateLimiterWithMaxAge creates a limiter with a custom unused-bucket expiry and starts its
// cleanup goroutine (stop it with Stop to avoid a leak).
func NewRateLimiterWithMaxAge(maxAge time.Duration) *RateLimiter {
	rl := &RateLimiter{
		buckets: make(map[string]*bucketEntry),
		configs: make(map[string]*RateLimitConfig),
		maxAge:  maxAge,
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
	bucket := rl.getOrCreateBucket(key, config)
	cost := config.TokenCost
	if cost <= 0 {
		cost = 1
	}
	allowed = bucket.Allow(float64(cost))
	remainingInt, resetSeconds, _ := bucket.State()
	return allowed, remainingInt, time.Duration(resetSeconds) * time.Second, nil
}

// getOrCreateBucket returns the bucket for key, creating it under the write lock if absent.
func (rl *RateLimiter) getOrCreateBucket(key string, config *RateLimitConfig) *TokenBucket {
	now := time.Now()
	rl.mu.RLock()
	entry, ok := rl.buckets[key]
	rl.mu.RUnlock()
	if ok {
		entry.lastAccess = now
		return entry.bucket
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if entry, ok = rl.buckets[key]; ok {
		entry.lastAccess = now
		return entry.bucket
	}
	bucket := NewTokenBucket(float64(config.Capacity), float64(config.RefillRate), config.Interval)
	rl.buckets[key] = &bucketEntry{bucket: bucket, lastAccess: now}
	return bucket
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

// evictStale removes buckets untouched within maxAge.
func (rl *RateLimiter) evictStale() {
	cutoff := time.Now().Add(-rl.maxAge)
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for key, entry := range rl.buckets {
		if entry.lastAccess.Before(cutoff) {
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
