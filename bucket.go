// Package ratelimit is a scope-agnostic in-memory rate limiter. Like loom it interprets no
// axis: a bucket is keyed by opaque caller-supplied DIMENSIONS ((label,value) pairs), never
// by a baked "tenant"/"user"/"ip" concept. A consumer that wants per-tenant limiting adds a
// "tenant" dimension; the limiter never names it.
//
// This module ships the pure in-memory TokenBucket + RateLimiter. The database-backed
// sliding-window variant (HSTLES `platform/ratelimit` `Allow`) is NOT here — it couples a SQL
// store (session + stmtcache), which are separate neutral leaves; it lands when those extract,
// via an injected store interface, not a baked DB.
package ratelimit

import (
	"sync"
	"time"
)

// TokenBucket implements the token bucket rate limiting algorithm. Thread-safe.
//
// The algorithm allows bursting up to capacity while maintaining a sustained rate defined by
// refillRate/interval. Example: capacity=10, refillRate=5, interval=10s → burst up to 10, then
// a sustained 0.5 req/s (5 tokens refilled every 10s).
type TokenBucket struct {
	mu         sync.Mutex
	capacity   float64
	tokens     float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

// NewTokenBucket creates a token bucket. capacity is the burst size; refillRate is tokens
// added per interval (converted internally to per-second). The bucket starts full.
func NewTokenBucket(capacity, refillRate float64, interval time.Duration) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     capacity,
		refillRate: refillRate / interval.Seconds(),
		lastRefill: time.Now(),
	}
}

// NewTokenBucketAt is NewTokenBucket with its refill clock started at start instead of the
// wall clock, for a caller that judges every call by AllowAt on its own clock (an injected or
// fixed time): the bucket starts full at start and AllowAt earns tokens only for time after it.
func NewTokenBucketAt(capacity, refillRate float64, interval time.Duration, start time.Time) *TokenBucket {
	tb := NewTokenBucket(capacity, refillRate, interval)
	tb.lastRefill = start
	return tb
}

// refill adds tokens based on elapsed time. Must be called with the mutex held.
func (tb *TokenBucket) refill() {
	tb.refillAt(time.Now())
}

// refillAt adds the tokens earned between the last refill and now. A now that
// is not after the last refill adds nothing and leaves the last refill where it
// was, so a clock stepping backwards neither earns nor costs tokens.
func (tb *TokenBucket) refillAt(now time.Time) {
	if !now.After(tb.lastRefill) {
		return
	}
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	tb.lastRefill = now
}

// AllowAt is Allow judged at now, the caller's time, instead of the wall clock:
// the bucket refills for the time since its last refill and takes cost when it
// holds that much. Its consumers keep their own clock: a limiter sharing one
// instant across its checks, a budget whose caller passes the time.
func (tb *TokenBucket) AllowAt(now time.Time, cost float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refillAt(now)
	if tb.tokens >= cost {
		tb.tokens -= cost
		return true
	}
	return false
}

// Allow decrements cost tokens and returns true if the request fits, false otherwise.
func (tb *TokenBucket) Allow(cost float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	if tb.tokens >= cost {
		tb.tokens -= cost
		return true
	}
	return false
}

// AllowN is Allow(float64(n)).
func (tb *TokenBucket) AllowN(n int) bool { return tb.Allow(float64(n)) }

// AllowOne is Allow(1) — the common per-request case.
func (tb *TokenBucket) AllowOne() bool { return tb.Allow(1.0) }

// Remaining returns the current whole-token count.
func (tb *TokenBucket) Remaining() int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	return int(tb.tokens)
}

// resetDuration is the time until the bucket is full. Must be called with the mutex held.
func (tb *TokenBucket) resetDuration() time.Duration {
	if tb.tokens >= tb.capacity {
		return 0
	}
	return time.Duration((tb.capacity - tb.tokens) / tb.refillRate * float64(time.Second))
}

// Reset returns the duration until the bucket is full again.
func (tb *TokenBucket) Reset() time.Duration {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	return tb.resetDuration()
}

// Capacity returns the bucket's maximum capacity.
func (tb *TokenBucket) Capacity() int { return int(tb.capacity) }

// State returns (remaining tokens, seconds until reset, capacity) for rate-limit headers.
func (tb *TokenBucket) State() (remaining, resetSeconds, capacity int) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	resetSeconds = int(tb.resetDuration().Seconds())
	if resetSeconds < 0 {
		resetSeconds = 0
	}
	return int(tb.tokens), resetSeconds, int(tb.capacity)
}
