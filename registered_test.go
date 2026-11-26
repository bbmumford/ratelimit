/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package ratelimit

import (
	"errors"
	"sort"
	"testing"
	"time"
)

func newTestLimiter(t *testing.T, names ...string) *RateLimiter {
	t.Helper()
	rl := NewRateLimiter()
	t.Cleanup(rl.Stop)
	for _, n := range names {
		if err := rl.Register(n, &RateLimitConfig{
			Mode: ModeTokenBucket, Capacity: 1, RefillRate: 1, Interval: time.Minute,
			Dimensions: []string{"ip"},
		}); err != nil {
			t.Fatalf("rl.Register: %v", err)
		}
	}
	return rl
}

func TestRegisteredDistinguishesMountedFromRegistered(t *testing.T) {
	rl := newTestLimiter(t, "api")
	if !rl.Registered("api") {
		t.Fatal("Registered said no for a name that was registered")
	}
	if rl.Registered("api_typo") {
		t.Fatal("Registered said yes for a name that was never registered")
	}
}

// The reason Registered exists: Protect cannot tell these two apart.
func TestProtectRefusesAMountedNameThatIsNotRegistered(t *testing.T) {
	rl := newTestLimiter(t, "api")
	ctx := &RateLimitContext{}

	allowedReal, _, _, errReal := rl.Protect("api", ctx)
	allowedMissing, _, _, errMissing := rl.Protect("api_typo", ctx)

	if !allowedReal || errReal != nil {
		t.Fatalf("the registered name = (%v, %v), want allowed", allowedReal, errReal)
	}
	if allowedMissing || !errors.Is(errMissing, ErrNotRegistered) {
		t.Fatalf("the typo = (%v, %v), want refused with ErrNotRegistered", allowedMissing, errMissing)
	}
	// Registered finds the same mistake at boot, before a request is refused.
	if rl.Registered("api_typo") {
		t.Fatal("Registered reported the typo as registered")
	}
}

func TestRegisteredNamesListsEveryRegistration(t *testing.T) {
	rl := newTestLimiter(t, "api", "login", "oauth")
	got := rl.RegisteredNames()
	sort.Strings(got)
	want := []string{"api", "login", "oauth"}
	if len(got) != len(want) {
		t.Fatalf("RegisteredNames = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("RegisteredNames = %v, want %v", got, want)
		}
	}
}

func TestRegisteredNamesReturnsAnIndependentSliceEachCall(t *testing.T) {
	rl := newTestLimiter(t, "api")
	first := rl.RegisteredNames()
	first[0] = "mutated"
	second := rl.RegisteredNames()
	// Compare against a SECOND call, not against Registered: Registered reads the
	// config map, so it is unaffected by this slice either way and would pass even
	// if every caller were handed one shared, cached slice.
	if second[0] != "api" {
		t.Fatalf("second call returned %q — callers share one slice, so one caller's write is visible to the next", second[0])
	}
}

func TestNilLimiterIsSafeAndReportsNotRegistered(t *testing.T) {
	var rl *RateLimiter
	if rl.Registered("api") {
		t.Fatal("a nil limiter reported a name as registered")
	}
	if n := rl.RegisteredNames(); n != nil {
		t.Fatalf("nil limiter RegisteredNames = %v, want nil", n)
	}
}
