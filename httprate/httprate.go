// Package httprate mounts a ratelimit.RateLimiter as HTTP middleware.
//
// It lives beside the limiter rather than in an HTTP toolkit so the limiter
// itself stays free of net/http, and it names no dimension: the caller supplies
// one extractor per opaque label its config keys on, so this package never
// learns what an "ip" or a "session" is.
package httprate

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/bbmumford/ratelimit"
)

// Errors Limit returns for a wiring mistake, all detectable before serving.
var (
	ErrNoLimiter     = errors.New("httprate: nil limiter")
	ErrNoName        = errors.New("httprate: no config name")
	ErrNotRegistered = errors.New("httprate: config name is not registered on the limiter")
	ErrNoExtractor   = errors.New("httprate: registered dimension has no extractor")
)

// Options configures the middleware.
type Options struct {
	// Limiter holds the registered configs. Required: there is no package-level
	// default, so a limiter is always the one the caller registered against.
	Limiter *ratelimit.RateLimiter

	// Name selects the registered config. Required, and Limit rejects a name the
	// limiter does not know: Protect treats an unregistered name as allow, so a
	// typo here would mount a middleware that limits nothing and looks mounted.
	Name string

	// Extractors supplies the per-request value for each opaque dimension label
	// the config keys on, keyed by that label. Limit rejects a config whose
	// labels are not all covered.
	Extractors map[string]func(r *http.Request) string

	// OnExceeded writes the over-limit response. Defaults to 429 with Retry-After.
	OnExceeded func(w http.ResponseWriter, r *http.Request, remaining int, reset time.Duration)

	// AddHeaders writes X-RateLimit-Remaining and X-RateLimit-Reset on allowed
	// responses.
	AddHeaders bool
}

// Limit returns the middleware, or an error describing the wiring mistake.
//
// Every check here is a boot-time one because each failure is invisible at
// runtime: an unregistered name and an uncovered dimension both serve traffic
// that looks limited and is not.
func Limit(opts Options) (func(http.Handler) http.Handler, error) {
	if opts.Limiter == nil {
		return nil, ErrNoLimiter
	}
	if opts.Name == "" {
		return nil, ErrNoName
	}
	labels, ok := opts.Limiter.Dimensions(opts.Name)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNotRegistered, opts.Name)
	}
	for _, label := range labels {
		if opts.Extractors[label] == nil {
			return nil, fmt.Errorf("%w: %q on config %q", ErrNoExtractor, label, opts.Name)
		}
	}

	// Copy the extractors for the labels actually keyed on. A caller that mutates
	// its map after mounting cannot change how a live handler builds its key, and
	// an extractor for a label the config does not use never runs.
	extractors := make(map[string]func(*http.Request) string, len(labels))
	for _, label := range labels {
		extractors[label] = opts.Extractors[label]
	}

	onExceeded := opts.OnExceeded
	if onExceeded == nil {
		onExceeded = DefaultOnExceeded
	}
	limiter, name, addHeaders := opts.Limiter, opts.Name, opts.AddHeaders

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			dims := make(map[string]string, len(extractors))
			for label, extract := range extractors {
				dims[label] = extract(r)
			}

			allowed, remaining, reset, err := limiter.Protect(name, &ratelimit.RateLimitContext{Dimensions: dims})
			if err != nil {
				// Fail OPEN. The limiter protects availability, so a limiter fault
				// that denied traffic would turn its own outage into the service's.
				next.ServeHTTP(w, r)
				return
			}

			// remaining < 0 means no limit applied to this request, so there is no
			// count to report — writing it would advertise a budget that does not
			// exist.
			if addHeaders && remaining >= 0 {
				setLimitHeaders(w, remaining, reset)
			}
			if !allowed {
				onExceeded(w, r, remaining, reset)
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}

// MustLimit is Limit for a composition root that cannot proceed without the
// middleware: it panics rather than returning an error, so a wiring mistake
// stops the process at boot instead of serving unlimited traffic.
func MustLimit(opts Options) func(http.Handler) http.Handler {
	mw, err := Limit(opts)
	if err != nil {
		panic(err)
	}
	return mw
}

// DefaultOnExceeded writes 429 with Retry-After and a zero remaining budget.
func DefaultOnExceeded(w http.ResponseWriter, r *http.Request, remaining int, reset time.Duration) {
	seconds := strconv.Itoa(resetSeconds(reset))
	w.Header().Set("Retry-After", seconds)
	w.Header().Set("X-RateLimit-Remaining", "0")
	w.Header().Set("X-RateLimit-Reset", seconds)
	http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
}

func setLimitHeaders(w http.ResponseWriter, remaining int, reset time.Duration) {
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
	w.Header().Set("X-RateLimit-Reset", strconv.Itoa(resetSeconds(reset)))
}

// resetSeconds rounds a sub-second reset UP to 1. A client reading "0" retries
// immediately and is denied again, so the floor keeps a retry loop off the door.
func resetSeconds(reset time.Duration) int {
	if reset <= 0 {
		return 0
	}
	if reset < time.Second {
		return 1
	}
	return int(reset / time.Second)
}
