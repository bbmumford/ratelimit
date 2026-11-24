package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrNoStore is returned (fail-closed) when AllowWindow is called without a WindowStore.
var ErrNoStore = errors.New("ratelimit: no window store")

// ErrInvalidWindow is returned (fail-closed) for a window that is not positive: truncating to it
// would make every instant a window of its own, so no count would ever pass the limit.
var ErrInvalidWindow = errors.New("ratelimit: the window is not positive")

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
// request is allowed while count <= limit. A non-positive limit disables the check (unlimited);
// a non-positive window fails CLOSED with ErrInvalidWindow.
// A nil store or a store error fails CLOSED (denied) — a missing/failing seam never allows.
// This is the store-agnostic, distributed counterpart to the local TokenBucket.
func AllowWindow(ctx context.Context, store WindowStore, scope, key string, limit int, window time.Duration) (allowed bool, remaining int, resetAt time.Time, err error) {
	return AllowWindowAt(ctx, store, scope, key, limit, window, time.Now())
}

// AllowWindowAt is AllowWindow judged at now instead of the wall clock, for a
// caller that holds its own clock (a test's fixed time, a limiter sharing one
// instant across checks). The rule is AllowWindow's.
func AllowWindowAt(ctx context.Context, store WindowStore, scope, key string, limit int, window time.Duration, now time.Time) (allowed bool, remaining int, resetAt time.Time, err error) {
	now = now.UTC()
	if limit <= 0 {
		// disabled or effectively unlimited
		return true, limit, now.Add(window), nil
	}
	if window <= 0 {
		return false, 0, now, ErrInvalidWindow
	}
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

// WindowPeeker is a WindowStore that can also read a window's count without
// incrementing it, for a caller that reports how much of a limit is left
// without spending any. A store may implement it beside Incr.
type WindowPeeker interface {
	Count(ctx context.Context, scope, key string, windowStart time.Time) (count int, err error)
}

// PeekWindow reports what remains of limit in the window holding now, without
// counting a request: limit minus the window's count, never below zero. A
// non-positive limit is unlimited. A store that is nil or that reads no count
// without incrementing (not a WindowPeeker) fails closed with ErrNoStore.
func PeekWindow(ctx context.Context, store WindowStore, scope, key string, limit int, window time.Duration, now time.Time) (remaining int, resetAt time.Time, err error) {
	now = now.UTC()
	if limit > 0 && window <= 0 {
		return 0, now, ErrInvalidWindow
	}
	start := now.Truncate(window)
	resetAt = start.Add(window)
	if limit <= 0 {
		return limit, resetAt, nil
	}
	peeker, ok := store.(WindowPeeker)
	if store == nil || !ok {
		return 0, resetAt, ErrNoStore
	}
	count, err := peeker.Count(ctx, scope, key, start)
	if err != nil {
		return 0, resetAt, err
	}
	if count >= limit {
		return 0, resetAt, nil
	}
	return limit - count, resetAt, nil
}

// MemWindowStore is the in-memory WindowStore, for one process: a counter per
// (scope, key) for the current window only. A window older than the one an
// increment names is dropped when that key is next counted, so the store holds
// at most one window per key; and once it holds more than memWindowSweepAt
// keys, an increment also drops every key of its scope whose window has
// closed, since one scope counts over one window length. It also implements
// WindowPeeker. Safe for concurrent use.
type MemWindowStore struct {
	mu      sync.Mutex
	windows map[memWindowKey]memWindow
}

type memWindowKey struct{ scope, key string }

type memWindow struct {
	start time.Time
	count int
}

var (
	_ WindowStore  = (*MemWindowStore)(nil)
	_ WindowPeeker = (*MemWindowStore)(nil)
)

// NewMemWindowStore builds an empty MemWindowStore.
func NewMemWindowStore() *MemWindowStore {
	return &MemWindowStore{windows: map[memWindowKey]memWindow{}}
}

// Incr counts one request for (scope, key) in the window beginning at
// windowStart. A request naming a later window than the one held starts it at
// one; a request naming an earlier window than the one held counts in the held
// one, so a caller with a slower clock cannot reopen a closed window.
func (s *MemWindowStore) Incr(_ context.Context, scope, key string, windowStart time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := memWindowKey{scope, key}
	w := s.windows[k]
	if windowStart.After(w.start) {
		w = memWindow{start: windowStart}
	}
	w.count++
	s.windows[k] = w
	if len(s.windows) > memWindowSweepAt {
		for other, held := range s.windows {
			if other.scope == scope && held.start.Before(windowStart) {
				delete(s.windows, other)
			}
		}
	}
	return w.count, nil
}

// memWindowSweepAt is the key count past which an increment sweeps its scope's
// closed windows.
const memWindowSweepAt = 4096

// Count reads (scope, key)'s count in the window beginning at windowStart: the
// held count when that window is the one held, and zero otherwise.
func (s *MemWindowStore) Count(_ context.Context, scope, key string, windowStart time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.windows[memWindowKey{scope, key}]
	if !w.start.Equal(windowStart) {
		return 0, nil
	}
	return w.count, nil
}
