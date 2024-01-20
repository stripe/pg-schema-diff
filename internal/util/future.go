package util

import "context"

// Future are a way to easily start a go routine and get the result of the go routine later. Avoid building any
// mapping functionality into futures, as that can make them difficult to reason about
type (
	result[T any] struct {
		err error
		res T
	}

	GoRoutineRunner interface {
		// Go starts a go routine and returns an error if the go routine could not be started.
		Go(context.Context, func()) error
	}

	Future[T any] struct {
		resultChan chan result[T]
	}
)

type SynchronousGoRoutineRunner struct{}

// NewSynchronousGoRoutineRunner creates a new goroutine runner that runs the goroutines synchronously
func NewSynchronousGoRoutineRunner() GoRoutineRunner {
	return &SynchronousGoRoutineRunner{}
}

func (r *SynchronousGoRoutineRunner) Go(_ context.Context, fn func()) error {
	fn()
	return nil
}

func NewFuture[T any](ctx context.Context, runner GoRoutineRunner, fn func() (T, error)) (Future[T], error) {
	future := Future[T]{
		resultChan: make(chan result[T], 1),
	}

	if err := runner.Go(ctx, func() {
		res, err := fn()
		future.resultChan <- result[T]{
			err: err,
			res: res,
		}
	}); err != nil {
		return Future[T]{}, err
	}

	return future, nil
}

func (f Future[T]) Get(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		var zeroVal T
		return zeroVal, ctx.Err()
	case res := <-f.resultChan:
		// Re-send the result to the channel so that the next call to Get() will get the same computed value. This
		// should be safe because the channel is buffered with a size of 1.
		f.resultChan <- res
		return res.res, res.err
	}
}

func ResolveAll[T any](ctx context.Context, futures ...Future[T]) ([]T, error) {
	vals := make([]T, len(futures))
	for i, future := range futures {
		val, err := future.Get(ctx)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}
