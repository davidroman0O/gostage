package gostage

import (
	"math"
	"math/rand"
	"time"
)

// RetryStrategy determines the delay between retry attempts.
// The attempt parameter is zero-based: 0 for the first retry, 1 for the second, etc.
type RetryStrategy interface {
	Delay(attempt int) time.Duration
}

// fixedDelayStrategy returns the same delay for every attempt.
type fixedDelayStrategy struct {
	delay time.Duration
}

// FixedDelay returns a strategy that waits the same duration between every retry.
//
//	gostage.Task("flaky", handler, gostage.WithRetry(3), gostage.WithRetryStrategy(gostage.FixedDelay(time.Second)))
func FixedDelay(d time.Duration) RetryStrategy {
	return &fixedDelayStrategy{delay: d}
}

// Delay returns the fixed delay regardless of the attempt number.
func (s *fixedDelayStrategy) Delay(_ int) time.Duration {
	return s.delay
}

// exponentialBackoffStrategy doubles the delay on each attempt up to a maximum.
type exponentialBackoffStrategy struct {
	base time.Duration
	max  time.Duration
}

// ExponentialBackoff returns a strategy that doubles the delay on each attempt.
// The delay for attempt N is base * 2^N, capped at max. If max is 0, no cap is applied.
//
//	gostage.Task("api-call", handler, gostage.WithRetry(5), gostage.WithRetryStrategy(
//	    gostage.ExponentialBackoff(100*time.Millisecond, 30*time.Second),
//	))
func ExponentialBackoff(base, max time.Duration) RetryStrategy {
	return &exponentialBackoffStrategy{base: base, max: max}
}

// Delay returns base * 2^attempt, capped at the configured maximum.
func (s *exponentialBackoffStrategy) Delay(attempt int) time.Duration {
	d := time.Duration(float64(s.base) * math.Pow(2, float64(attempt)))
	if s.max > 0 && d > s.max {
		d = s.max
	}
	return d
}

// exponentialJitterStrategy applies exponential backoff with random jitter.
type exponentialJitterStrategy struct {
	base time.Duration
	max  time.Duration
}

// ExponentialBackoffWithJitter returns a strategy that applies exponential backoff
// with random jitter to prevent thundering herds. The delay is a random value
// between 0 and min(base * 2^attempt, max).
//
//	gostage.Task("api-call", handler, gostage.WithRetry(5), gostage.WithRetryStrategy(
//	    gostage.ExponentialBackoffWithJitter(100*time.Millisecond, 30*time.Second),
//	))
func ExponentialBackoffWithJitter(base, max time.Duration) RetryStrategy {
	return &exponentialJitterStrategy{base: base, max: max}
}

// Delay returns a random duration between 0 and min(base * 2^attempt, max).
func (s *exponentialJitterStrategy) Delay(attempt int) time.Duration {
	ceiling := time.Duration(float64(s.base) * math.Pow(2, float64(attempt)))
	if s.max > 0 && ceiling > s.max {
		ceiling = s.max
	}
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(ceiling)))
}
