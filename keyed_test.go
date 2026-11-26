package ratelimit

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// A set limit refuses past its burst with a wait of at least a second, and
// keeps two keys and two limits apart.
func TestKeyedLimitsEachKeyAndAnswersTheWait(t *testing.T) {
	k := NewKeyed(time.Minute, 100)
	defer k.Stop()
	k.Set("source", 1, 2, time.Second)
	k.Set("token", 1, 5, time.Second)
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
	if ok, _ := k.Allow("token", "a"); !ok {
		t.Error("another limit shared the source limit's bucket")
	}
}

// Allow fails closed on everything the composition did not declare: a nil
// Keyed, a name no Set or Unlimited declared, and a name Set with a rate or an
// interval that is not positive all refuse, with a wait of at least a second.
func TestKeyedRefusesWhatTheCompositionDidNotDeclare(t *testing.T) {
	var nilKeyed *Keyed
	if ok, wait := nilKeyed.Allow("source", "a"); ok || wait < time.Second {
		t.Errorf("a nil Keyed = (%v, %v), want a refusal", ok, wait)
	}
	k := NewKeyed(time.Minute, 100)
	defer k.Stop()
	if ok, _ := k.Allow("source", "a"); ok {
		t.Error("a Keyed with nothing declared admitted a request")
	}
	k.Set("source", 5, 5, time.Second)
	if ok, _ := k.Allow("sorce", "a"); ok {
		t.Error("a name never declared (a typo of a set one) admitted a request")
	}
	k.Set("token", 0, 5, time.Second)
	k.Set("partition", 1, 1, 0)
	k.Set("negative", -3, 1, time.Second)
	for _, name := range []string{"token", "partition", "negative"} {
		if ok, wait := k.Allow(name, "a"); ok || wait < time.Second {
			t.Errorf("a limit %q set with a non-positive rate or interval = (%v, %v), want a refusal", name, ok, wait)
		}
	}
	k.Unlimited("token")
	if ok, _ := k.Allow("token", "a"); !ok {
		t.Error("Unlimited after an invalid Set still refused")
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
