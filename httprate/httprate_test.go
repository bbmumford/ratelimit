package httprate

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bbmumford/ratelimit"
)

func newLimiter(t *testing.T, name string, capacity int, dims []string) *ratelimit.RateLimiter {
	t.Helper()
	rl := ratelimit.NewRateLimiter()
	t.Cleanup(rl.Stop)
	rl.Register(name, &ratelimit.RateLimitConfig{
		Mode:       ratelimit.ModeTokenBucket,
		Capacity:   capacity,
		RefillRate: 0,
		Interval:   time.Minute,
		Dimensions: dims,
	})
	return rl
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("served"))
	})
}

func byHeader(name string) func(*http.Request) string {
	return func(r *http.Request) string { return r.Header.Get(name) }
}

// An unregistered name makes Protect allow everything, so mounting one would
// look limited and limit nothing. Limit must refuse it instead.
func TestLimit_UnregisteredNameIsRejected(t *testing.T) {
	rl := newLimiter(t, "api", 1, nil)
	_, err := Limit(Options{Limiter: rl, Name: "api_typo"})
	if !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("err = %v, want ErrNotRegistered", err)
	}
}

// A dimension with no extractor contributes "" on every request, collapsing all
// callers into one bucket. Limit must refuse it.
func TestLimit_UncoveredDimensionIsRejected(t *testing.T) {
	rl := newLimiter(t, "api", 1, []string{"caller", "route"})
	_, err := Limit(Options{
		Limiter:    rl,
		Name:       "api",
		Extractors: map[string]func(*http.Request) string{"caller": byHeader("X-Caller")},
	})
	if !errors.Is(err, ErrNoExtractor) {
		t.Fatalf("err = %v, want ErrNoExtractor", err)
	}
}

func TestLimit_NilLimiterAndEmptyNameRejected(t *testing.T) {
	if _, err := Limit(Options{Name: "api"}); !errors.Is(err, ErrNoLimiter) {
		t.Fatalf("nil limiter err = %v, want ErrNoLimiter", err)
	}
	rl := newLimiter(t, "api", 1, nil)
	if _, err := Limit(Options{Limiter: rl}); !errors.Is(err, ErrNoName) {
		t.Fatalf("empty name err = %v, want ErrNoName", err)
	}
}

func TestLimit_AllowsUntilExhaustedThenDenies(t *testing.T) {
	rl := newLimiter(t, "api", 2, []string{"caller"})
	mw, err := Limit(Options{
		Limiter:    rl,
		Name:       "api",
		Extractors: map[string]func(*http.Request) string{"caller": byHeader("X-Caller")},
		AddHeaders: true,
	})
	if err != nil {
		t.Fatalf("Limit: %v", err)
	}
	h := mw(okHandler())

	for i := 1; i <= 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.Header.Set("X-Caller", "a")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: code = %d, want 200", i, rec.Code)
		}
		if rec.Header().Get("X-RateLimit-Remaining") == "" {
			t.Fatalf("request %d: no X-RateLimit-Remaining", i)
		}
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("X-Caller", "a")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("third request code = %d, want 429", rec.Code)
	}
	if rec.Body.String() == "served" {
		t.Fatal("denied request still reached the handler")
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 carried no Retry-After")
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Fatalf("429 X-RateLimit-Remaining = %q, want 0", got)
	}
}

// Distinct dimension values must not share a budget, or one caller exhausts
// everyone's.
func TestLimit_DistinctDimensionValuesGetIndependentBudgets(t *testing.T) {
	rl := newLimiter(t, "api", 1, []string{"caller"})
	mw, err := Limit(Options{
		Limiter:    rl,
		Name:       "api",
		Extractors: map[string]func(*http.Request) string{"caller": byHeader("X-Caller")},
	})
	if err != nil {
		t.Fatalf("Limit: %v", err)
	}
	h := mw(okHandler())

	serve := func(caller string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.Header.Set("X-Caller", caller)
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := serve("a"); code != http.StatusOK {
		t.Fatalf("a first = %d, want 200", code)
	}
	if code := serve("a"); code != http.StatusTooManyRequests {
		t.Fatalf("a second = %d, want 429", code)
	}
	if code := serve("b"); code != http.StatusOK {
		t.Fatalf("b first = %d, want 200 — b spent a's budget", code)
	}
}

// The middleware must hold its own copy of the extractors: a caller mutating the
// map it passed must not change how a mounted handler builds its key.
func TestLimit_ExtractorMapMutationAfterMountIsIgnored(t *testing.T) {
	rl := newLimiter(t, "api", 1, []string{"caller"})
	extractors := map[string]func(*http.Request) string{"caller": byHeader("X-Caller")}
	mw, err := Limit(Options{Limiter: rl, Name: "api", Extractors: extractors})
	if err != nil {
		t.Fatalf("Limit: %v", err)
	}
	extractors["caller"] = func(*http.Request) string { return "constant" }
	h := mw(okHandler())

	serve := func(caller string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.Header.Set("X-Caller", caller)
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	if code := serve("a"); code != http.StatusOK {
		t.Fatalf("a = %d, want 200", code)
	}
	if code := serve("b"); code != http.StatusOK {
		t.Fatalf("b = %d, want 200 — the swapped extractor collapsed the buckets", code)
	}
}

// A config the limiter skips reports remaining < 0: there is no budget, so no
// budget headers may be written.
func TestLimit_NoHeadersWhenNoLimitApplied(t *testing.T) {
	rl := ratelimit.NewRateLimiter()
	t.Cleanup(rl.Stop)
	rl.Register("api", &ratelimit.RateLimitConfig{
		Mode: ratelimit.ModeTokenBucket, Capacity: 1, Interval: time.Minute,
		Skip: func(*ratelimit.RateLimitContext) bool { return true },
	})
	mw, err := Limit(Options{Limiter: rl, Name: "api", AddHeaders: true})
	if err != nil {
		t.Fatalf("Limit: %v", err)
	}
	rec := httptest.NewRecorder()
	mw(okHandler()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "" {
		t.Fatalf("X-RateLimit-Remaining = %q on an unlimited request, want none", got)
	}
}

func TestLimit_HeadersOmittedUnlessRequested(t *testing.T) {
	rl := newLimiter(t, "api", 5, nil)
	mw, err := Limit(Options{Limiter: rl, Name: "api"})
	if err != nil {
		t.Fatalf("Limit: %v", err)
	}
	rec := httptest.NewRecorder()
	mw(okHandler()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "" {
		t.Fatalf("header written without AddHeaders: %q", got)
	}
}

// A sub-second reset must not be advertised as 0, which invites an immediate
// retry that is denied again.
func TestResetSeconds_SubSecondRoundsUpToOne(t *testing.T) {
	for _, tc := range []struct {
		in   time.Duration
		want int
	}{
		{0, 0}, {-time.Second, 0}, {time.Millisecond, 1}, {999 * time.Millisecond, 1},
		{time.Second, 1}, {90 * time.Second, 90},
	} {
		if got := resetSeconds(tc.in); got != tc.want {
			t.Fatalf("resetSeconds(%v) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestMustLimit_PanicsOnWiringMistake(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustLimit did not panic on an unregistered name")
		}
	}()
	rl := newLimiter(t, "api", 1, nil)
	MustLimit(Options{Limiter: rl, Name: "nope"})
}

// A request the limiter cannot judge is refused through OnExceeded, and served
// only when the options say FailOpen. A bounded limiter whose key map is full
// of spent buckets cannot judge a new key (ratelimit.ErrNoRoom).
func TestLimit_AnUnjudgedRequestIsRefusedUnlessFailOpen(t *testing.T) {
	rl := ratelimit.NewRateLimiterWithBounds(time.Hour, 1)
	defer rl.Stop()
	rl.Register("login", &ratelimit.RateLimitConfig{Mode: ratelimit.ModeTokenBucket, Capacity: 1, RefillRate: 1,
		Interval: time.Hour, Dimensions: []string{"ip"}})
	spent := &ratelimit.RateLimitContext{Dimensions: map[string]string{"ip": "198.51.100.1"}}
	if ok, _, _, err := rl.Protect("login", spent); !ok || err != nil {
		t.Fatalf("the first key's request = (%v, %v)", ok, err)
	}
	if _, _, _, err := rl.Protect("login", &ratelimit.RateLimitContext{Dimensions: map[string]string{"ip": "198.51.100.7"}}); !errors.Is(err, ratelimit.ErrNoRoom) {
		t.Fatalf("a new key at the full map: err %v, want ErrNoRoom", err)
	}

	for _, tc := range []struct {
		failOpen bool
		want     int
	}{{false, http.StatusTooManyRequests}, {true, http.StatusOK}} {
		mw, err := Limit(Options{Limiter: rl, Name: "login", FailOpen: tc.failOpen,
			Extractors: map[string]func(*http.Request) string{"ip": func(*http.Request) string { return "198.51.100.7" }}})
		if err != nil {
			t.Fatalf("Limit: %v", err)
		}
		w := httptest.NewRecorder()
		mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })).
			ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/login", nil))
		if w.Code != tc.want {
			t.Errorf("FailOpen %v: an unjudged request answered %d, want %d", tc.failOpen, w.Code, tc.want)
		}
	}
}
