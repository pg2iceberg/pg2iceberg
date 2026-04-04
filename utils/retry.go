package utils

import (
	"context"
	"log"
	"math/rand/v2"
	"time"
)

// Do calls fn up to maxAttempts times with exponential backoff and full jitter.
// Returns nil on the first successful call, or the last error if all attempts fail.
// Respects context cancellation — returns ctx.Err() immediately if the context
// is done before or during backoff.
func Do(ctx context.Context, maxAttempts int, initialDelay, maxDelay time.Duration, fn func() error) error {
	var lastErr error
	delay := initialDelay

	for attempt := range maxAttempts {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		// Don't sleep after the last attempt.
		if attempt == maxAttempts-1 {
			break
		}

		// Full jitter: sleep for a random duration in [0, delay).
		jittered := time.Duration(rand.Int64N(int64(delay)))
		log.Printf("[retry] attempt %d/%d failed: %v (backoff %s)", attempt+1, maxAttempts, lastErr, jittered)

		timer := time.NewTimer(jittered)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		// Exponential increase, capped at maxDelay.
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	return lastErr
}
