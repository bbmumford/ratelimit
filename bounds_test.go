package ratelimit

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// A caller rotating its key on every request never grows a bounded limiter past its cap.
func TestBoundedLimiter_KeyRotationStaysUnderTheCap(t *testing.T) {
	rl := NewRateLimiterWithBounds(time.Hour, 100)
	defer rl.Stop()
	rl.Register("ip", &RateLimitConfig{Capacity: 2, RefillRate: 1, Interval: time.Hour, Dimensions: []string{"ip"}})
	for i := 0; i < 10_000; i++ {
		ctx := &RateLimitContext{Dimensions: map[string]string{"ip": fmt.Sprintf("198.51.100.%d", i)}}
		allowed, _, _, err := rl.Protect("ip", ctx)
		switch {
		case i < 100 && !allowed:
			t.Fatalf("request %d is the first on its key, the map has room, and it was refused", i)
		case i >= 100 && (allowed || !errors.Is(err, ErrNoRoom)):
			t.Fatalf("request %d on a new key found the map full of spent buckets: allowed %v, err %v; want ErrNoRoom", i, allowed, err)
		}
		if n := rl.BucketCount(); n > 100 {
			t.Fatalf("after %d keys the map holds %d buckets, cap 100", i+1, n)
		}
	}
}

// A full map gives up the buckets back at capacity before any bucket still holding a
// spent token, so a limited key keeps its count while idle keys come and go.
func TestBoundedLimiter_RefilledBucketsGoFirst(t *testing.T) {
	rl := NewRateLimiterWithBounds(time.Hour, 10)
	defer rl.Stop()
	// idle refills within the test's own runtime; busy refills one token an hour.
	rl.Register("idle", &RateLimitConfig{Capacity: 1, RefillRate: 1_000_000, Interval: time.Millisecond, Dimensions: []string{"k"}})
	rl.Register("busy", &RateLimitConfig{Capacity: 3, RefillRate: 1, Interval: time.Hour, Dimensions: []string{"k"}})
	hot := &RateLimitContext{Dimensions: map[string]string{"k": "hot"}}
	if allowed, _, _, _ := rl.Protect("busy", hot); !allowed {
		t.Fatal("the busy key's first request must be allowed")
	}
	for i := 0; i < 50; i++ {
		rl.Protect("idle", &RateLimitContext{Dimensions: map[string]string{"k": fmt.Sprintf("idle-%d", i)}})
		time.Sleep(time.Millisecond)
	}
	if _, remaining, _, _ := rl.Protect("busy", hot); remaining != 1 {
		t.Fatalf("the busy key lost its count to eviction: remaining %d, want 1", remaining)
	}
	if n := rl.BucketCount(); n > 10 {
		t.Fatalf("the map holds %d buckets, cap 10", n)
	}
}

// maxKeys <= 0 leaves the map uncapped, as NewRateLimiterWithMaxAge always has.
func TestBoundedLimiter_NonPositiveCapIsUncapped(t *testing.T) {
	for _, maxKeys := range []int{0, -1} {
		rl := NewRateLimiterWithBounds(time.Hour, maxKeys)
		rl.Register("ip", &RateLimitConfig{Capacity: 1, RefillRate: 1, Interval: time.Hour, Dimensions: []string{"ip"}})
		for i := 0; i < 500; i++ {
			rl.Protect("ip", &RateLimitContext{Dimensions: map[string]string{"ip": fmt.Sprint(i)}})
		}
		if n := rl.BucketCount(); n != 500 {
			t.Fatalf("maxKeys %d: the map holds %d buckets, want all 500", maxKeys, n)
		}
		rl.Stop()
	}
}
