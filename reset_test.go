package ratelimit

import (
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
	rl.Register("partition", &RateLimitConfig{Capacity: 1, RefillRate: 1, Interval: time.Hour, Dimensions: []string{"key"}})
	rl.Register("source", &RateLimitConfig{Capacity: 1, RefillRate: 1, Interval: time.Hour, Dimensions: []string{"key"}})
	p := &RateLimitContext{Dimensions: map[string]string{"key": "p-1"}}
	if ok, _, _, _ := rl.Protect("partition", p); !ok {
		t.Fatal("the partition's first request was refused")
	}
	if ok, _, _, _ := rl.Protect("partition", p); ok {
		t.Fatal("the partition's second request was allowed past its limit of one an hour")
	}
	for i := 0; i < 32; i++ {
		rl.Protect("source", &RateLimitContext{Dimensions: map[string]string{"key": fmt.Sprintf("198.51.100.%d", i)}})
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
	rl.Register("op", &RateLimitConfig{Capacity: 1, RefillRate: 1, Interval: time.Hour, Dimensions: []string{"k"}})
	k := &RateLimitContext{Dimensions: map[string]string{"k": "a"}}
	rl.Protect("op", k)
	time.Sleep(maxAge + 30*time.Millisecond)
	rl.evictStale()
	if ok, _, _, _ := rl.Protect("op", k); ok {
		t.Fatal("an idle spent bucket was swept and its limit reset")
	}
}
