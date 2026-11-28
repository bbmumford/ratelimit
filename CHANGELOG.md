# Changelog

## [0.2.0](https://github.com/bbmumford/ratelimit/compare/v0.1.0...v0.2.0) (2026-11-28)


### ⚠ BREAKING CHANGES

* a nil Keyed, a name no Set or Unlimited declared, and a name Set with a non-positive rate or interval now refuse every request. Declare an unbounded name with Unlimited.

### Bug Fixes

* Keyed refuses what the composition did not declare: a nil Keyed, an undeclared name and a limit set with a rate or interval that is not positive ([0206844](https://github.com/bbmumford/ratelimit/commit/020684419560cb6a2fa3897efc6bd523409d1a34))

## 0.1.0 (2026-11-25)


### ⚠ BREAKING CHANGES

* Protect answers allowed=false with ErrNotRegistered for an unregistered name, where it answered allowed=true.

### Features

* AllowWindow limits by window over an injected WindowStore, failing closed ([896abe8](https://github.com/bbmumford/ratelimit/commit/896abe87038ad747b3df95d9732e4814b1d31578))
* an in-memory rate limiter keyed by caller-supplied dimensions ([8baaf01](https://github.com/bbmumford/ratelimit/commit/8baaf016408bafc9d3fdc8ecce842ae86d2091dc))
* Keyed holds named per-key limits on one bounded limiter, and httprate refuses a request it cannot judge unless FailOpen ([de6ba6c](https://github.com/bbmumford/ratelimit/commit/de6ba6c7e59a6e3c090ce86d127eb502a2edf0ac))
* Keyed.Unlimited declares a name the composition leaves without a limit ([2a91e91](https://github.com/bbmumford/ratelimit/commit/2a91e91addac5640cdf912df97cf5666a682f04d))
* KeyLimiter checks one key's limit, supplied per call, through a local bucket and a shared window; MemWindowStore, PeekWindow and caller-time forms ([d777916](https://github.com/bbmumford/ratelimit/commit/d77791630020e77494d40f8d1dd952e6c5c3bbe2))
* NewTokenBucketAt starts a bucket's refill clock at the caller's time ([4dd2b18](https://github.com/bbmumford/ratelimit/commit/4dd2b18a09d57aa44529dd1b55e6eb5de208af60))


### Bug Fixes

* a bounded limiter never drops a bucket still below capacity ([f9ff99a](https://github.com/bbmumford/ratelimit/commit/f9ff99a7885f8686aa286705abae0525ef76da66))
* a bucket's access time is atomic, so concurrent requests on one key and the stale sweep do not race on it ([dfe6eb3](https://github.com/bbmumford/ratelimit/commit/dfe6eb3e9e9fd6a46b6236e6603d975d22da56d9))
* a non-positive interval or window never removes a limit ([da8249a](https://github.com/bbmumford/ratelimit/commit/da8249a7805c44b3c63f0832d2f635b0116c40a3))
* Protect refuses a request it cannot judge ([2bd7312](https://github.com/bbmumford/ratelimit/commit/2bd73129ea8be4c3bb1dfc51748ff00ddc348982))
