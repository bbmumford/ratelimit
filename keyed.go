package ratelimit

import (
	"math"
	"sync"
	"time"
)

// keyedDimension is the one opaque dimension every Keyed limit is keyed by; the
// limit's name already keeps two limits' buckets apart.
const keyedDimension = "key"

// Keyed holds named per-key token-bucket limits on one bounded RateLimiter, for a
// handler that limits by a value it reads while serving a request (a source
// address, a verified token's id, a partition) rather than through middleware.
//
// The limiter is built by the first Set, so a Keyed with no limit set holds no
// cleanup goroutine. A refusal carries the wait until the key's bucket refills.
// Allow refuses whenever the limiter cannot say yes, so a limit guarding a public
// route against abuse fails closed.
type Keyed struct {
	maxAge  time.Duration
	maxKeys int

	mu    sync.Mutex
	rl    *RateLimiter
	names map[string]bool // the names Set registered on rl and Unlimited has not lifted
}

// NewKeyed returns a Keyed whose buckets idle for maxAge are swept and whose key
// map is capped at maxKeys buckets (NewRateLimiterWithBounds), so a caller
// rotating the keyed value cannot grow memory without bound.
func NewKeyed(maxAge time.Duration, maxKeys int) *Keyed {
	return &Keyed{maxAge: maxAge, maxKeys: maxKeys}
}

// Set registers the limit name at rate requests per interval with a burst of
// burst (at least one). A rate that is not positive, or an interval that is not,
// sets nothing, so Allow admits every request under name. It runs at
// composition, before the handler serves.
func (k *Keyed) Set(name string, rate, burst int, interval time.Duration) {
	if k == nil || rate <= 0 || interval <= 0 {
		return
	}
	if burst < 1 {
		burst = 1
	}
	k.mu.Lock()
	if k.rl == nil {
		k.rl = NewRateLimiterWithBounds(k.maxAge, k.maxKeys)
	}
	if k.names == nil {
		k.names = map[string]bool{}
	}
	k.names[name] = true
	rl := k.rl
	k.mu.Unlock()
	// Register refuses only a nil limiter and a config whose interval is not
	// positive; rl is never nil here and Set has already returned for such an
	// interval, so its result is dropped.
	_ = rl.Register(name, &RateLimitConfig{
		Mode:       ModeTokenBucket,
		Capacity:   burst,
		RefillRate: rate,
		Interval:   interval,
		Dimensions: []string{keyedDimension},
	})
}

// Unlimited declares that the composition deliberately leaves name without a
// limit: Allow admits every request under it until a Set of name limits it.
// It runs at composition, as Set does, and is how a caller says "no limit"
// rather than leaving the name undeclared.
func (k *Keyed) Unlimited(name string) {
	if k == nil {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.names, name)
}

// Allow spends one request of the limit name for key. A refusal returns the
// whole seconds until the bucket is full again, at least one. A name never set
// is unlimited.
func (k *Keyed) Allow(name, key string) (ok bool, retryAfter time.Duration) {
	if k == nil {
		return true, 0
	}
	k.mu.Lock()
	rl, set := k.rl, k.names[name]
	k.mu.Unlock()
	if rl == nil || !set {
		return true, 0
	}
	allowed, _, reset, err := rl.Protect(name, &RateLimitContext{Dimensions: map[string]string{keyedDimension: key}})
	if err == nil && allowed {
		return true, 0
	}
	seconds := math.Ceil(reset.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	return false, time.Duration(seconds) * time.Second
}

// Stop halts the limiter's cleanup goroutine, if one was started.
func (k *Keyed) Stop() {
	if k == nil {
		return
	}
	k.mu.Lock()
	rl := k.rl
	k.mu.Unlock()
	rl.Stop()
}
