package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"
)

// failingStore answers every increment with an error.
type failingStore struct{}

func (failingStore) Incr(context.Context, string, string, time.Time) (int, error) {
	return 0, errors.New("shared counter unreachable")
}

// A key's limit holds across two limiters sharing one store and scope, as two
// enforcement points or two instances do: neither admits more than the limit in
// a window between them, and the next window admits again. A limit that is not
// positive is unlimited, and a changed limit resizes the key's bucket.
func TestAKeysLimitHoldsAcrossLimitersSharingAStore(t *testing.T) {
	store := NewMemWindowStore()
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	bridge, err := NewKeyLimiter(store, "api_key_rpm", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	governor, _ := NewKeyLimiter(store, "api_key_rpm", time.Minute)
	bridge.now, governor.now = clock, clock
	admitted := 0
	for i := 0; i < 4; i++ {
		for _, l := range []*KeyLimiter{bridge, governor} {
			if ok, _, _, err := l.Allow(context.Background(), "key_1", 5); err != nil {
				t.Fatal(err)
			} else if ok {
				admitted++
			}
		}
	}
	if admitted != 5 {
		t.Fatalf("admitted %d of 8 across the two limiters, want the limit 5", admitted)
	}
	now = now.Add(time.Minute)
	if ok, remaining, _, _ := bridge.Allow(context.Background(), "key_1", 5); !ok || remaining != 4 {
		t.Fatalf("the next window: allowed %v remaining %d, want a fresh window", ok, remaining)
	}
	if ok, _, _, _ := bridge.Allow(context.Background(), "key_2", 0); !ok {
		t.Fatal("a limit of 0 was enforced; it is unlimited")
	}
	for i := 0; i < 3; i++ {
		bridge.Allow(context.Background(), "key_3", 2)
	}
	if ok, _, _, _ := bridge.Allow(context.Background(), "key_3", 10); !ok {
		t.Fatal("a raised limit kept the old bucket's refusal")
	}
}

// The local bucket refuses a key past its limit without reaching the shared
// store, and a store that fails refuses (fails closed).
func TestTheLocalBucketRefusesBeforeTheStoreAndAStoreErrorRefuses(t *testing.T) {
	store := NewMemWindowStore()
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	l, _ := NewKeyLimiter(store, "s", time.Minute)
	l.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		l.Allow(context.Background(), "k", 2)
	}
	start := now.Truncate(time.Minute)
	if n, _ := store.Count(context.Background(), "s", "k", start); n != 2 {
		t.Fatalf("the shared window counted %d, want 2: the third request was refused locally", n)
	}
	failing, _ := NewKeyLimiter(failingStore{}, "s", time.Minute)
	if ok, _, _, err := failing.Allow(context.Background(), "k", 2); ok || err == nil {
		t.Fatalf("a failing store: allowed %v err %v, want refused with the error", ok, err)
	}
	if _, err := NewKeyLimiter(nil, "s", time.Minute); !errors.Is(err, ErrKeyLimiterConfig) {
		t.Fatalf("no store: %v", err)
	}
}

// MemWindowStore counts one window per key, starts a later window afresh,
// counts an earlier window's request in the held one, and reads a count
// without spending one (PeekWindow).
func TestTheMemWindowStoreCountsTheCurrentWindow(t *testing.T) {
	s := NewMemWindowStore()
	ctx := context.Background()
	w1 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	w2 := w1.Add(time.Minute)
	for i := 1; i <= 3; i++ {
		if n, _ := s.Incr(ctx, "s", "k", w1); n != i {
			t.Fatalf("increment %d counted %d", i, n)
		}
	}
	if n, _ := s.Incr(ctx, "s", "k", w2); n != 1 {
		t.Fatalf("a later window counted %d, want 1", n)
	}
	if n, _ := s.Incr(ctx, "s", "k", w1); n != 2 {
		t.Fatalf("an earlier window's request counted %d in the held window, want 2", n)
	}
	remaining, _, err := PeekWindow(ctx, s, "s", "k", 5, time.Minute, w2.Add(10*time.Second))
	if err != nil || remaining != 3 {
		t.Fatalf("PeekWindow = %d, %v; want 3 left of 5", remaining, err)
	}
	if n, _ := s.Count(ctx, "s", "k", w2); n != 2 {
		t.Fatalf("the peek spent a request: count %d", n)
	}
	if _, _, err := PeekWindow(ctx, failingStore{}, "s", "k", 5, time.Minute, w2); !errors.Is(err, ErrNoStore) {
		t.Fatalf("a store that cannot peek: %v, want ErrNoStore", err)
	}
}

// AllowWindowAt judges the window at the caller's time; AllowAt takes the
// caller's time and a clock stepping backwards earns no tokens.
func TestTheCallerTimeForms(t *testing.T) {
	s := NewMemWindowStore()
	at := time.Date(2030, 1, 1, 0, 0, 30, 0, time.UTC)
	if ok, remaining, reset, err := AllowWindowAt(context.Background(), s, "s", "k", 2, time.Minute, at); !ok || remaining != 1 || !reset.Equal(at.Truncate(time.Minute).Add(time.Minute)) || err != nil {
		t.Fatalf("AllowWindowAt = %v %d %v %v", ok, remaining, reset, err)
	}
	b := NewTokenBucket(1, 1, time.Second)
	start := b.lastRefill
	if !b.AllowAt(start, 1) {
		t.Fatal("a full bucket refused")
	}
	if b.AllowAt(start.Add(-time.Hour), 1) {
		t.Fatal("a clock stepping back an hour earned a token")
	}
	if !b.AllowAt(start.Add(time.Second), 1) {
		t.Fatal("a second later the bucket had not refilled")
	}
}
