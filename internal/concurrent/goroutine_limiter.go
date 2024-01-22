package concurrent

import (
	"context"

	"golang.org/x/sync/semaphore"
)

type LimitedGoroutineRunner struct {
	sem *semaphore.Weighted
}

// NewGoroutineLimiter creates a new GoroutineRunner that limits the number of goroutines. No more than the limit number
// of goroutines can be running at the same time.
func NewGoroutineLimiter(limit int64) *LimitedGoroutineRunner {
	return &LimitedGoroutineRunner{
		sem: semaphore.NewWeighted(limit),
	}
}

func (l *LimitedGoroutineRunner) Go(ctx context.Context, fn func()) error {
	if err := l.sem.Acquire(ctx, 1); err != nil {
		return err
	}

	go func() {
		defer l.sem.Release(1)
		fn()
	}()

	return nil
}
