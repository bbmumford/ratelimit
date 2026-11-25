package ratelimit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeStore is an in-memory atomic WindowStore for testing the distributed-limiting contract.
type fakeStore struct {
	mu     sync.Mutex
	counts map[string]int
	failed bool
}

func (f *fakeStore) Incr(_ context.Context, scope, key string, ws time.Time) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failed {
		return 0, errors.New("store down")
	}
	k := scope + "|" + key + "|" + ws.Format(time.RFC3339)
	f.counts[k]++
	return f.counts[k], nil
}

func newStore() *fakeStore { return &fakeStore{counts: map[string]int{}} }

func TestAllowWindow_LimitAndRemaining(t *testing.T) {
	store := newStore()
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		ok, remaining, _, err := AllowWindow(ctx, store, "op", "acme", 3, time.Minute)
		if err != nil || !ok {
			t.Fatalf("request %d within limit must pass; ok=%v err=%v", i, ok, err)
		}
		if want := 3 - i; remaining != want {
			t.Fatalf("request %d remaining=%d, want %d", i, remaining, want)
		}
	}
	if ok, _, _, _ := AllowWindow(ctx, store, "op", "acme", 3, time.Minute); ok {
		t.Fatal("request beyond the window limit must be denied")
	}
}

func TestAllowWindow_IndependentKeys(t *testing.T) {
	store := newStore()
	ctx := context.Background()
	// exhaust acme
	for i := 0; i < 2; i++ {
		if _, _, _, err := AllowWindow(ctx, store, "op", "acme", 2, time.Minute); err != nil {
			t.Fatalf("AllowWindow: %v", err)
		}
	}
	if ok, _, _, _ := AllowWindow(ctx, store, "op", "acme", 2, time.Minute); ok {
		t.Fatal("acme should be exhausted")
	}
	// a different key is independent
	if ok, _, _, _ := AllowWindow(ctx, store, "op", "globex", 2, time.Minute); !ok {
		t.Fatal("a different key must have its own distributed window count")
	}
}

func TestAllowWindow_UnlimitedBypasses(t *testing.T) {
	if ok, _, _, err := AllowWindow(context.Background(), newStore(), "op", "k", 0, time.Minute); !ok || err != nil {
		t.Fatalf("a non-positive limit must be unlimited; ok=%v err=%v", ok, err)
	}
}

func TestAllowWindow_NilStoreAndErrorFailClosed(t *testing.T) {
	if ok, _, _, err := AllowWindow(context.Background(), nil, "op", "k", 5, time.Minute); ok || !errors.Is(err, ErrNoStore) {
		t.Fatalf("nil store must fail CLOSED with ErrNoStore; ok=%v err=%v", ok, err)
	}
	store := newStore()
	store.failed = true
	if ok, _, _, err := AllowWindow(context.Background(), store, "op", "k", 5, time.Minute); ok || err == nil {
		t.Fatalf("a store error must fail CLOSED; ok=%v err=%v", ok, err)
	}
}
