package ratelimit

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// New keys arriving at a full bounded limiter never reset another key's limit:
// a partition limit spent before a flood of new source keys is still spent
// after it.
func TestBoundedLimiter_NewKeysNeverResetASpentLimit(t *testing.T) {
	rl := NewRateLimiterWithBounds(time.Hour, 8)
	defer rl.Stop()
	if err := rl.Register("partition", &RateLimitConfig{Capacity: 1, RefillRate: 1, Interval: time.Hour, Dimensions: []string{"key"}}); err != nil {
		t.Fatalf("rl.Register: %v", err)
	}
	if err := rl.Register("source", &RateLimitConfig{Capacity: 1, RefillRate: 1, Interval: time.Hour, Dimensions: []string{"key"}}); err != nil {
		t.Fatalf("rl.Register: %v", err)
	}
	p := &RateLimitContext{Dimensions: map[string]string{"key": "p-1"}}
	if ok, _, _, _ := rl.Protect("partition", p); !ok {
		t.Fatal("the partition's first request was refused")
	}
	if ok, _, _, _ := rl.Protect("partition", p); ok {
		t.Fatal("the partition's second request was allowed past its limit of one an hour")
	}
	for i := 0; i < 32; i++ {
		// A new key the full map cannot hold is refused with ErrNoRoom; any other
		// error is a failure.
		if _, _, _, err := rl.Protect("source", &RateLimitContext{Dimensions: map[string]string{"key": fmt.Sprintf("198.51.100.%d", i)}}); err != nil && !errors.Is(err, ErrNoRoom) {
			t.Fatalf("rl.Protect: %v", err)
		}
	}
	if ok, _, _, _ := rl.Protect("partition", p); ok {
		t.Fatal("after new source keys the partition's spent limit was reset")
	}
}

// The stale sweep keeps a bucket still below capacity however long it has
// been idle, since dropping it would reset its key's limit.
func TestEvictStaleKeepsASpentBucket(t *testing.T) {
	const maxAge = 50 * time.Millisecond
	rl := NewRateLimiterWithMaxAge(maxAge)
	defer rl.Stop()
	if err := rl.Register("op", &RateLimitConfig{Capacity: 1, RefillRate: 1, Interval: time.Hour, Dimensions: []string{"k"}}); err != nil {
		t.Fatalf("rl.Register: %v", err)
	}
	k := &RateLimitContext{Dimensions: map[string]string{"k": "a"}}
	if _, _, _, err := rl.Protect("op", k); err != nil {
		t.Fatalf("rl.Protect: %v", err)
	}
	time.Sleep(maxAge + 30*time.Millisecond)
	rl.evictStale()
	if ok, _, _, _ := rl.Protect("op", k); ok {
		t.Fatal("an idle spent bucket was swept and its limit reset")
	}
}
