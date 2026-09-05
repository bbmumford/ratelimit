/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ratelimit

import (
	"sort"
	"testing"
	"time"
)

func newTestLimiter(t *testing.T, names ...string) *RateLimiter {
	t.Helper()
	rl := NewRateLimiter()
	t.Cleanup(rl.Stop)
	for _, n := range names {
		rl.Register(n, &RateLimitConfig{
			Mode: ModeTokenBucket, Capacity: 1, RefillRate: 1, Interval: time.Minute,
			Dimensions: []string{"ip"},
		})
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
func TestProtectCannotDistinguishUnlimitedFromWithinLimit(t *testing.T) {
	rl := newTestLimiter(t, "api")
	ctx := &RateLimitContext{}

	allowedReal, _, _, errReal := rl.Protect("api", ctx)
	allowedMissing, _, _, errMissing := rl.Protect("api_typo", ctx)

	if !allowedReal || !allowedMissing {
		t.Fatalf("expected both allowed, got real=%v missing=%v", allowedReal, allowedMissing)
	}
	if errReal != nil || errMissing != nil {
		t.Fatalf("expected nil errors, got real=%v missing=%v", errReal, errMissing)
	}
	// Identical answers for "passed the limit" and "there is no limit" — which is
	// exactly why a caller needs Registered to catch the wiring mistake at boot.
	if rl.Registered("api_typo") {
		t.Fatal("Registered must be the thing that separates these two cases")
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
