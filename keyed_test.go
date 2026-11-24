package ratelimit

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// A set limit refuses past its burst with a wait of at least a second, keeps
// two keys and two limits apart, and an unset or non-positive limit admits
// everything.
func TestKeyedLimitsEachKeyAndAnswersTheWait(t *testing.T) {
	k := NewKeyed(time.Minute, 100)
	defer k.Stop()
	if ok, _ := k.Allow("source", "a"); !ok {
		t.Fatal("a Keyed with no limit set refused")
	}
	k.Set("source", 1, 2, time.Second)
	k.Set("token", 0, 5, time.Second)
	k.Set("partition", 1, 1, 0)
	for i := 0; i < 2; i++ {
		if ok, _ := k.Allow("source", "a"); !ok {
			t.Fatalf("request %d within the burst refused", i+1)
		}
	}
	ok, wait := k.Allow("source", "a")
	if ok || wait < time.Second {
		t.Fatalf("past the burst = (%v, %v), want a refusal waiting at least a second", ok, wait)
	}
	if ok, _ := k.Allow("source", "b"); !ok {
		t.Error("another key shared the first key's bucket")
	}
	for i := 0; i < 10; i++ {
		if ok, _ := k.Allow("token", "a"); !ok {
			t.Fatal("a limit set at rate 0 refused")
		}
		if ok, _ := k.Allow("partition", "a"); !ok {
			t.Fatal("a limit set over a zero interval refused")
		}
	}
}

// The key map stays bounded when every request names a new key.
func TestKeyedHoldsItsKeyCap(t *testing.T) {
	k := NewKeyed(time.Minute, 16)
	defer k.Stop()
	k.Set("source", 1, 1, time.Second)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k.Allow("source", strconv.Itoa(g*1000+i))
			}
		}(g)
	}
	wg.Wait()
	k.rl.mu.RLock()
	n := len(k.rl.buckets)
	k.rl.mu.RUnlock()
	if n > 16 {
		t.Fatalf("%d buckets held, want at most the cap of 16", n)
	}
}

// A name declared Unlimited admits every request until a Set limits it, and a
// later Unlimited lifts the limit again.
func TestKeyedUnlimitedDeclaresANameWithoutALimit(t *testing.T) {
	k := NewKeyed(time.Minute, 100)
	defer k.Stop()
	k.Unlimited("open")
	for i := 0; i < 20; i++ {
		if ok, _ := k.Allow("open", "a"); !ok {
			t.Fatal("an Unlimited name refused")
		}
	}
	k.Set("open", 1, 1, time.Hour)
	k.Allow("open", "a")
	if ok, _ := k.Allow("open", "a"); ok {
		t.Fatal("a Set after Unlimited did not limit the name")
	}
	k.Unlimited("open")
	if ok, _ := k.Allow("open", "a"); !ok {
		t.Fatal("an Unlimited after Set did not lift the limit")
	}
}
