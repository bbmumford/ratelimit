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
// Every name a handler consults is declared at composition: Set gives it a
// limit, Unlimited deliberately leaves it without one. Allow fails closed on
// everything else: a nil Keyed, a name never declared (a typo, or a limit the
// composition forgot) and a name Set with a rate or interval that is not
// positive all refuse, so a limit
// guarding a public route against abuse never opens by mistake. The limiter is
// built by the first valid Set, so a Keyed with no limit set holds no cleanup
// goroutine. A refusal carries the wait until the key's bucket refills.
type Keyed struct {
	maxAge  time.Duration
	maxKeys int

	mu    sync.Mutex
	rl    *RateLimiter
	names map[string]keyedState // every name the composition declared
}

// keyedState is what the composition declared for one name.
type keyedState uint8

const (
	keyedLimited   keyedState = iota + 1 // Set registered a limit on rl
	keyedUnlimited                       // Unlimited: every request admitted
	keyedInvalid                         // Set could not be read: every request refused
)

// NewKeyed returns a Keyed whose buckets idle for maxAge are swept and whose key
// map is capped at maxKeys buckets (NewRateLimiterWithBounds), so a caller
// rotating the keyed value cannot grow memory without bound.
func NewKeyed(maxAge time.Duration, maxKeys int) *Keyed {
	return &Keyed{maxAge: maxAge, maxKeys: maxKeys}
}

// Set declares the limit name at rate requests per interval with a burst of
// burst (at least one). A rate that is not positive, or an interval that is not,
// declares a limit that cannot be read, and name then refuses every request: a
// limit set wrong never admits. On a nil Keyed it does nothing (Allow refuses
// there anyway). It runs at composition, before the handler serves.
func (k *Keyed) Set(name string, rate, burst int, interval time.Duration) {
	if k == nil {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.names == nil {
		k.names = map[string]keyedState{}
	}
	if rate <= 0 || interval <= 0 {
		k.names[name] = keyedInvalid
		return
	}
	if burst < 1 {
		burst = 1
	}
	if k.rl == nil {
		k.rl = NewRateLimiterWithBounds(k.maxAge, k.maxKeys)
	}
	// Register refuses only a nil limiter and a config whose interval is not
	// positive; rl is not nil here and the interval was checked above. Were it
	// ever to refuse, the name is declared invalid and refuses every request.
	if err := k.rl.Register(name, &RateLimitConfig{
		Mode:       ModeTokenBucket,
		Capacity:   burst,
		RefillRate: rate,
		Interval:   interval,
		Dimensions: []string{keyedDimension},
	}); err != nil {
		k.names[name] = keyedInvalid
		return
	}
	k.names[name] = keyedLimited
}

// Unlimited declares that the composition deliberately leaves name without a
// limit: Allow admits every request under it until a Set of name limits it.
// It runs at composition, as Set does, and is how a caller says "no limit";
// a name left undeclared is refused.
func (k *Keyed) Unlimited(name string) {
	if k == nil {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.names == nil {
		k.names = map[string]keyedState{}
	}
	k.names[name] = keyedUnlimited
}

// refusedUndeclared is the wait Allow answers for a refusal no bucket can lift
// (a nil Keyed, an undeclared or invalid name): the smallest Retry-After.
const refusedUndeclared = time.Second

// Allow spends one request of the limit name for key. A refusal returns the
// whole seconds until the bucket is full again, at least one. A nil Keyed, a
// name never declared and a name Set with a rate or interval that is not
// positive refuse every request.
func (k *Keyed) Allow(name, key string) (ok bool, retryAfter time.Duration) {
	if k == nil {
		return false, refusedUndeclared
	}
	k.mu.Lock()
	rl, state := k.rl, k.names[name]
	k.mu.Unlock()
	switch state {
	case keyedUnlimited:
		return true, 0
	case keyedLimited:
	default:
		return false, refusedUndeclared
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
