package ratelimit

import (
	"testing"
	"time"
)

// A bucket started at a caller's time far from the wall clock is full there,
// earns tokens only for the caller's time after it, and earns nothing for a step
// backwards.
func TestATokenBucketStartedAtTheCallersTimeRefillsOnThatClock(t *testing.T) {
	start := time.Unix(1_000, 0)
	tb := NewTokenBucketAt(2, 1, time.Second, start)
	if !tb.AllowAt(start, 1) || !tb.AllowAt(start, 1) || tb.AllowAt(start, 1) {
		t.Fatal("a full bucket of two did not allow exactly two at its start")
	}
	if tb.AllowAt(start.Add(-time.Hour), 1) {
		t.Fatal("a step backwards earned a token")
	}
	if !tb.AllowAt(start.Add(time.Second), 1) || tb.AllowAt(start.Add(time.Second), 1) {
		t.Fatal("one second did not earn exactly one token")
	}
	if !tb.AllowAt(start.Add(time.Hour), 2) || tb.AllowAt(start.Add(time.Hour), 1) {
		t.Fatal("an hour did not refill to the capacity and no further")
	}
}
