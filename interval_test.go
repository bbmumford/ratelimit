package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A zero interval or window never removes a limit: a bucket with capacity one
// admits at most one request, and a window limit of one at most one.
func TestAZeroIntervalOrWindowKeepsTheLimit(t *testing.T) {
	rl := NewRateLimiterWithMaxAge(time.Hour)
	defer rl.Stop()
	rl.Register("op", &RateLimitConfig{Mode: ModeTokenBucket, Capacity: 1, RefillRate: 1, Interval: 0, Dimensions: []string{"k"}})
	allowed := 0
	for i := 0; i < 100; i++ {
		if ok, _, _, _ := rl.Protect("op", &RateLimitContext{Dimensions: map[string]string{"k": "a"}}); ok {
			allowed++
		}
	}
	if allowed > 1 {
		t.Fatalf("a config with a zero interval admitted %d of 100 requests at capacity one", allowed)
	}

	tb := NewTokenBucket(1, 1, 0)
	allowed = 0
	for i := 0; i < 100; i++ {
		if tb.AllowOne() {
			allowed++
		}
	}
	if allowed > 1 {
		t.Fatalf("a bucket with a zero interval admitted %d of 100 requests at capacity one", allowed)
	}

	store := NewMemWindowStore()
	allowed = 0
	for i := 0; i < 100; i++ {
		if ok, _, _, _ := AllowWindow(context.Background(), store, "scope", "k", 1, 0); ok {
			allowed++
		}
	}
	if allowed > 1 {
		t.Fatalf("a window limit of one over a zero window admitted %d of 100 requests", allowed)
	}
}

// Register refuses a config it cannot enforce and registers nothing, and a
// zero window is refused with its reason.
func TestRegisterAndAllowWindowNameTheRefusal(t *testing.T) {
	rl := NewRateLimiterWithMaxAge(time.Hour)
	defer rl.Stop()
	if err := rl.Register("zero", &RateLimitConfig{Capacity: 1, RefillRate: 1, Interval: 0}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("a zero interval: got %v, want ErrInvalidConfig", err)
	}
	if err := rl.Register("none", nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("a nil config: got %v, want ErrInvalidConfig", err)
	}
	if rl.Registered("zero") || rl.Registered("none") {
		t.Fatal("a refused config was registered")
	}
	if err := rl.Register("ok", &RateLimitConfig{Capacity: 1, RefillRate: 1, Interval: time.Second}); err != nil {
		t.Fatalf("a valid config: %v", err)
	}
	if _, _, _, err := AllowWindow(context.Background(), NewMemWindowStore(), "s", "k", 1, 0); !errors.Is(err, ErrInvalidWindow) {
		t.Fatalf("a zero window: got %v, want ErrInvalidWindow", err)
	}
	if _, _, err := PeekWindow(context.Background(), NewMemWindowStore(), "s", "k", 1, 0, time.Now()); !errors.Is(err, ErrInvalidWindow) {
		t.Fatalf("a zero window peek: got %v, want ErrInvalidWindow", err)
	}
}
