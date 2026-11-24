package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// TestRateLimiter_ConcurrentRequestsOnOneBucketDoNotRace drives one existing bucket from
// many goroutines at once while the stale sweep runs, which is what an endpoint's root
// limit sees when two requests from one client arrive together. Run under -race: the
// access timestamp each request refreshes is shared by every request on the key and by
// evictStale, so a plain write to it is a data race.
func TestRateLimiter_ConcurrentRequestsOnOneBucketDoNotRace(t *testing.T) {
	rl := NewRateLimiterWithMaxAge(time.Hour)
	defer rl.Stop()
	rl.Register("root", &RateLimitConfig{Mode: ModeTokenBucket, Capacity: 1000, RefillRate: 1000, Interval: time.Second, Dimensions: []string{"ip"}})
	ctx := ctxWith(map[string]string{"ip": "203.0.113.7"})
	if ok, _, _, _ := rl.Protect("root", ctx); !ok {
		t.Fatal("the first request must create the bucket and pass")
	}

	const workers, perWorker = 8, 50
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < perWorker; i++ {
				rl.Protect("root", ctx)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < perWorker; i++ {
			rl.evictStale()
		}
	}()
	close(start)
	wg.Wait()

	if got := rl.BucketCount(); got != 1 {
		t.Fatalf("one key under concurrent requests holds %d buckets, want 1", got)
	}
}

// TestRateLimiter_EvictStaleKeepsATouchedBucketAndDropsAnUntouchedOne holds the sweep's
// reading of the access timestamp: a bucket a request refreshed inside maxAge survives,
// and one nothing touched for longer than maxAge is removed.
func TestRateLimiter_EvictStaleKeepsATouchedBucketAndDropsAnUntouchedOne(t *testing.T) {
	const maxAge = 200 * time.Millisecond
	rl := NewRateLimiterWithMaxAge(maxAge)
	defer rl.Stop()
	// Refills within the sleep below, so the idle bucket is back at capacity when swept.
	rl.Register("op", &RateLimitConfig{Mode: ModeTokenBucket, Capacity: 2, RefillRate: 1000, Interval: time.Second, Dimensions: []string{"k"}})
	touched := ctxWith(map[string]string{"k": "touched"})
	idle := ctxWith(map[string]string{"k": "idle"})
	rl.Protect("op", touched)
	rl.Protect("op", idle)

	time.Sleep(maxAge + 50*time.Millisecond)
	rl.Protect("op", touched) // refreshes the access time of an existing bucket
	rl.evictStale()

	if got := rl.BucketCount(); got != 1 {
		t.Fatalf("after the sweep %d buckets remain, want 1 (the touched one)", got)
	}
	rl.mu.RLock()
	_, kept := rl.buckets["op:"+touched.Key([]string{"k"})]
	rl.mu.RUnlock()
	if !kept {
		t.Fatal("the bucket refreshed inside maxAge was evicted")
	}
}
