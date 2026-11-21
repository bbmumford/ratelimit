# ratelimit

Request-rate limiting with no axis of its own. Bucket keys are built from opaque dimension labels you fill (an IP, a session, a tenant, anything).

- **`RateLimiter`**: in-memory named budgets (token bucket, sliding window or fixed window), each keyed by the dimensions its config lists, with idle buckets evicted.
- **`httprate`**: mounts a registered budget as `net/http` middleware. It refuses at construction a budget name the limiter does not know or a dimension no extractor fills, so a typo cannot mount a limiter that limits nothing; a request the limiter cannot judge is refused unless `FailOpen` is set.
- **`AllowWindow` and `KeyLimiter`**: a fixed-window check over a counter store you supply (`WindowStore`; `MemWindowStore` in memory), for limits that must hold across processes.
- **`Keyed` and `TokenBucket`**: the smaller building blocks.

## Use

```sh
go get github.com/bbmumford/ratelimit
```

```go
rl := ratelimit.NewRateLimiter()
defer rl.Stop()
rl.Register("login", &ratelimit.RateLimitConfig{
	Mode:       ratelimit.ModeTokenBucket,
	Capacity:   5,
	RefillRate: 1,
	Interval:   time.Minute,
	Dimensions: []string{"ip"},
})

limit, err := httprate.Limit(httprate.Options{
	Limiter:    rl,
	Name:       "login",
	Extractors: map[string]func(*http.Request) string{"ip": clientIP},
	AddHeaders: true,
})
mux.Handle("/login", limit(loginHandler)) // over the limit: 429 with Retry-After
```

## Consumers

ORBTR library, ai cortex, and io constellation, tools and the relay.orbtr.io and app.orbtr.io endpoints.
