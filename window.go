package ratelimit

import (
	"context"
	"errors"
	"time"
)

// ErrNoStore is returned (fail-closed) when AllowWindow is called without a WindowStore.
var ErrNoStore = errors.New("ratelimit: no window store")

// WindowStore is a consumer-supplied backend for distributed, persistent window-based limiting
// — the counterpart to the in-memory TokenBucket. The neutral module owns the windowing logic;
// the consumer implements Incr over any store (SQL, tursokv, Redis, …), so ratelimit stays
// store-agnostic. This keeps distributed database-backed sliding/fixed-window limiting available
// without baking a *sql.DB, a stmt cache, or a session package into the module.
type WindowStore interface {
	// Incr atomically increments and returns the request count for (scope, key) in the window
	// beginning at windowStart. Implementations MUST be atomic (exactly one increment per call)
	// so the returned count is authoritative across distributed callers — that atomicity is the
	// whole point of the distributed variant over the local TokenBucket.
	Incr(ctx context.Context, scope, key string, windowStart time.Time) (count int, err error)
}

// AllowWindow performs distributed window-based rate limiting via a consumer WindowStore. The
// window is time.Now().UTC() truncated to `window`; the counter increments once per call and the
// request is allowed while count <= limit. A non-positive limit disables the check (unlimited).
// A nil store or a store error fails CLOSED (denied) — a missing/failing seam never allows.
// This is the store-agnostic, distributed counterpart to the local TokenBucket.
func AllowWindow(ctx context.Context, store WindowStore, scope, key string, limit int, window time.Duration) (allowed bool, remaining int, resetAt time.Time, err error) {
	if limit <= 0 {
		// disabled or effectively unlimited
		return true, limit, time.Now().UTC().Add(window), nil
	}
	now := time.Now().UTC()
	start := now.Truncate(window)
	resetAt = start.Add(window)
	if store == nil {
		return false, 0, resetAt, ErrNoStore
	}
	count, err := store.Incr(ctx, scope, key, start)
	if err != nil {
		return false, 0, resetAt, err
	}
	if count <= limit {
		return true, limit - count, resetAt, nil
	}
	return false, 0, resetAt, nil
}
