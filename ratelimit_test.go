package ratelimit

import (
	"errors"
	"testing"
	"time"
)

func TestTokenBucket_BurstThenDeny(t *testing.T) {
	tb := NewTokenBucket(3, 3, time.Minute) // 3 burst, slow refill
	for i := 0; i < 3; i++ {
		if !tb.AllowOne() {
			t.Fatalf("request %d within capacity must be allowed", i+1)
		}
	}
	if tb.AllowOne() {
		t.Fatal("request beyond capacity must be denied")
	}
	if got := tb.Capacity(); got != 3 {
		t.Fatalf("Capacity()=%d, want 3", got)
	}
	if got := tb.Remaining(); got != 0 {
		t.Fatalf("Remaining()=%d, want 0", got)
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	tb := NewTokenBucket(1, 100, time.Second) // 100 tokens/sec → ~1 token per 10ms
	if !tb.AllowOne() {
		t.Fatal("first request should pass")
	}
	if tb.AllowOne() {
		t.Fatal("bucket should be empty immediately after")
	}
	time.Sleep(40 * time.Millisecond) // enough to refill at least one token
	if !tb.AllowOne() {
		t.Fatal("bucket should have refilled at least one token")
	}
}

func TestTokenBucket_AllowN(t *testing.T) {
	tb := NewTokenBucket(5, 5, time.Minute)
	if !tb.AllowN(5) {
		t.Fatal("cost 5 within capacity 5 must pass")
	}
	if tb.AllowN(1) {
		t.Fatal("bucket empty after draining all 5")
	}
}

func cfg(dims ...string) *RateLimitConfig {
	return &RateLimitConfig{Mode: ModeTokenBucket, Capacity: 2, RefillRate: 2, Interval: time.Minute, Dimensions: dims}
}

func ctxWith(m map[string]string) *RateLimitContext { return &RateLimitContext{Dimensions: m} }

func TestRateLimiter_ProtectSharedBucket(t *testing.T) {
	rl := NewRateLimiterWithMaxAge(time.Hour)
	defer rl.Stop()
	if err := rl.Register("op", cfg("tenant")); err != nil {
		t.Fatalf("rl.Register: %v", err)
	}
	c := ctxWith(map[string]string{"tenant": "acme"})
	for i := 0; i < 2; i++ {
		if ok, _, _, _ := rl.Protect("op", c); !ok {
			t.Fatalf("request %d for the same key within capacity must pass", i+1)
		}
	}
	if ok, _, _, _ := rl.Protect("op", c); ok {
		t.Fatal("3rd request for the same key must be denied")
	}
}

// TestRateLimiter_OpaqueDimensionsAreIndependent is the axis-neutrality proof: the limiter
// treats "tenant" as an OPAQUE label. Two different dimension values get independent buckets;
// the limiter never interprets what "tenant" means — a consumer could use any label.
func TestRateLimiter_OpaqueDimensionsAreIndependent(t *testing.T) {
	rl := NewRateLimiterWithMaxAge(time.Hour)
	defer rl.Stop()
	if err := rl.Register("op", cfg("tenant")); err != nil {
		t.Fatalf("rl.Register: %v", err)
	}
	acme := ctxWith(map[string]string{"tenant": "acme"})
	globex := ctxWith(map[string]string{"tenant": "globex"})
	// drain acme's bucket
	if _, _, _, err := rl.Protect("op", acme); err != nil {
		t.Fatalf("rl.Protect: %v", err)
	}
	if _, _, _, err := rl.Protect("op", acme); err != nil {
		t.Fatalf("rl.Protect: %v", err)
	}
	if ok, _, _, _ := rl.Protect("op", acme); ok {
		t.Fatal("acme should be exhausted")
	}
	// globex is a DIFFERENT value → independent bucket, still full
	if ok, _, _, _ := rl.Protect("op", globex); !ok {
		t.Fatal("a different dimension value must have its own bucket (independent limit)")
	}
	// the label itself is opaque: a config keyed on "planet" behaves identically
	if err := rl.Register("op2", cfg("planet")); err != nil {
		t.Fatalf("rl.Register: %v", err)
	}
	if ok, _, _, _ := rl.Protect("op2", ctxWith(map[string]string{"planet": "mars"})); !ok {
		t.Fatal("an arbitrary opaque label must work exactly like any other")
	}
}

func TestRateLimiter_MultiDimensionCombo(t *testing.T) {
	rl := NewRateLimiterWithMaxAge(time.Hour)
	defer rl.Stop()
	if err := rl.Register("op", cfg("tenant", "user")); err != nil {
		t.Fatalf("rl.Register: %v", err)
	}
	a1 := ctxWith(map[string]string{"tenant": "acme", "user": "u1"})
	a2 := ctxWith(map[string]string{"tenant": "acme", "user": "u2"}) // same tenant, different user → distinct bucket
	if _, _, _, err := rl.Protect("op", a1); err != nil {
		t.Fatalf("rl.Protect: %v", err)
	}
	if _, _, _, err := rl.Protect("op", a1); err != nil {
		t.Fatalf("rl.Protect: %v", err)
	}
	if ok, _, _, _ := rl.Protect("op", a1); ok {
		t.Fatal("acme/u1 exhausted")
	}
	if ok, _, _, _ := rl.Protect("op", a2); !ok {
		t.Fatal("acme/u2 is a distinct (tenant,user) combo → its own bucket")
	}
}

func TestRateLimiter_UnregisteredFailsClosed(t *testing.T) {
	rl := NewRateLimiterWithMaxAge(time.Hour)
	defer rl.Stop()
	if ok, _, _, err := rl.Protect("never-registered", ctxWith(nil)); ok || !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("an unregistered config = (%v, %v), want a refusal with ErrNotRegistered", ok, err)
	}
	var none *RateLimiter
	if ok, _, _, err := none.Protect("any", ctxWith(nil)); ok || !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("a nil limiter = (%v, %v), want a refusal with ErrNotRegistered", ok, err)
	}
}

func TestRateLimiter_SkipBypasses(t *testing.T) {
	rl := NewRateLimiterWithMaxAge(time.Hour)
	defer rl.Stop()
	c := cfg("ip")
	c.Skip = func(*RateLimitContext) bool { return true }
	if err := rl.Register("op", c); err != nil {
		t.Fatalf("rl.Register: %v", err)
	}
	ctx := ctxWith(map[string]string{"ip": "1.2.3.4"})
	for i := 0; i < 10; i++ { // far beyond capacity 2
		if ok, _, _, _ := rl.Protect("op", ctx); !ok {
			t.Fatal("Skip must bypass the limit")
		}
	}
}

func TestKey_EmptyAndAbsent(t *testing.T) {
	c := ctxWith(map[string]string{"tenant": "acme"})
	if got := c.Key(nil); got != "" {
		t.Fatalf("no labels → global key \"\"; got %q", got)
	}
	if got := c.Key([]string{"tenant"}); got != "tenant:acme" {
		t.Fatalf("Key=%q, want tenant:acme", got)
	}
	// absent dimension → empty value, distinct from a populated one
	if got := c.Key([]string{"missing"}); got != "missing:" {
		t.Fatalf("absent dim → %q, want missing:", got)
	}
	if c.Key([]string{"tenant"}) == c.Key([]string{"missing"}) {
		t.Fatal("populated and absent dimensions must not collide")
	}
}
