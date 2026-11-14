package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"
)

// KeyLimiter is one per-key rate check whose capacity the caller supplies with
// each check, for a limit that is a property of the key itself (an API key's
// requests per minute, read from its row) rather than of a registered config.
// It is layered:
//
//   - a token bucket per key in this process, sized to the limit for the
//     window, refuses early and cheaply, with no shared read: a key past its
//     limit here is refused without touching the shared counter;
//   - a window over a shared WindowStore is the authority: a request the local
//     bucket admits is counted in the key's window there, so a key's limit
//     holds across every process and every enforcement point that counts in
//     the same store and scope.
//
// A limit that is not positive is unlimited. A limit that changes between
// checks resizes the key's bucket. A store error fails closed. Buckets idle for
// longer than an hour are dropped as keys are checked.
type KeyLimiter struct {
	store  WindowStore
	scope  string
	window time.Duration
	now    func() time.Time

	mu      sync.Mutex
	buckets map[string]*keyBucket
	swept   time.Time
}

// keyBucket is one key's local bucket and the limit it was sized for.
type keyBucket struct {
	limit  int
	bucket *TokenBucket
	used   time.Time
}

// keyBucketIdle is how long a key's bucket may go unchecked before a sweep
// drops it.
const keyBucketIdle = time.Hour

// ErrKeyLimiterConfig refuses a KeyLimiter built without a store, a scope or a
// positive window.
var ErrKeyLimiterConfig = errors.New("ratelimit: a key limiter needs a window store, a scope and a positive window")

// NewKeyLimiter builds a KeyLimiter counting in store under scope, over windows
// of window. Every enforcement point that must share a key's limit builds its
// limiter over the same store and scope.
func NewKeyLimiter(store WindowStore, scope string, window time.Duration) (*KeyLimiter, error) {
	if store == nil || scope == "" || window <= 0 {
		return nil, ErrKeyLimiterConfig
	}
	return &KeyLimiter{store: store, scope: scope, window: window, now: time.Now, buckets: map[string]*keyBucket{}}, nil
}

// Allow checks one request of key against limit requests per window: the local
// bucket first, then the shared window. remaining is what the shared window
// leaves (0 when the local bucket refused), and resetAt is when the window
// closes. err is the store's error, with allowed false.
func (l *KeyLimiter) Allow(ctx context.Context, key string, limit int) (allowed bool, remaining int, resetAt time.Time, err error) {
	now := l.now()
	if limit <= 0 {
		return true, limit, now.UTC().Add(l.window), nil
	}
	if !l.local(key, limit, now) {
		return false, 0, now.UTC().Truncate(l.window).Add(l.window), nil
	}
	return AllowWindowAt(ctx, l.store, l.scope, key, limit, l.window, now)
}

// local takes one token from key's bucket, building or resizing it for limit.
func (l *KeyLimiter) local(key string, limit int, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.buckets[key]
	if b == nil || b.limit != limit {
		b = &keyBucket{limit: limit, bucket: NewTokenBucket(float64(limit), float64(limit), l.window)}
		b.bucket.lastRefill = now
		l.buckets[key] = b
	}
	b.used = now
	if now.Sub(l.swept) >= keyBucketIdle {
		for k, other := range l.buckets {
			if now.Sub(other.used) >= keyBucketIdle {
				delete(l.buckets, k)
			}
		}
		l.swept = now
	}
	return b.bucket.AllowAt(now, 1)
}
