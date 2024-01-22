package concurrent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type asyncGoroutineRunner struct{}

func (a *asyncGoroutineRunner) Go(_ context.Context, fn func()) error {
	go fn()
	return nil
}

type errGoroutineRunner struct {
	err error
}

func (e *errGoroutineRunner) Go(_ context.Context, _ func()) error {
	if e.err == nil {
		panic("err must be provided")
	}
	return e.err
}

func TestFuture_GoroutineRunnerError(t *testing.T) {
	expectedErr := errors.New("some error")
	_, err := SubmitFuture(context.Background(), &errGoroutineRunner{err: expectedErr}, func() (int, error) {
		return 5, nil
	})
	require.ErrorIs(t, err, expectedErr)
}

func TestFuture_Get_Error(t *testing.T) {
	expectedErr := errors.New("some error")
	future, err := SubmitFuture(context.Background(), &asyncGoroutineRunner{}, func() (int, error) {
		return 5, expectedErr
	})
	require.NoError(t, err)

	_, err = future.Get(context.Background())
	require.ErrorIs(t, err, expectedErr)
}

func TestFuture_Get_FinishesBeforeRead(t *testing.T) {
	future, err := SubmitFuture(context.Background(), NewSynchronousGoroutineRunner(), func() (int, error) {
		return 5, nil
	})
	require.NoError(t, err)

	res, err := future.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 5, res)

	// Re-reading should produce the same result
	res, err = future.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 5, res)
}

func TestFuture_Get_ReadStartsBeforeFinish(t *testing.T) {
	readStartedChan := make(chan struct{})
	future, err := SubmitFuture(context.Background(), &asyncGoroutineRunner{}, func() (int, error) {
		<-readStartedChan
		return 5, nil
	})
	require.NoError(t, err)

	resChan := make(chan int)
	go func() {
		res, err := future.Get(context.Background())
		require.NoError(t, err)
		resChan <- res
	}()
	readStartedChan <- struct{}{}
	assert.Equal(t, 5, <-resChan)

	// Re-reading should produce the same result
	res, err := future.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 5, res)
}

func TestFuture_Get_ConcurrentReads(t *testing.T) {
	future, err := SubmitFuture(context.Background(), &asyncGoroutineRunner{}, func() (int, error) {
		return 5, nil
	})
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := future.Get(context.Background())
			require.NoError(t, err)
			assert.Equal(t, 5, res)
		}()
	}
	wg.Wait()
}

func TestFuture_Get_ConcurrentReadsCancel(t *testing.T) {
	someLock := sync.Mutex{}
	someLock.Lock()
	future, err := SubmitFuture(context.Background(), &asyncGoroutineRunner{}, func() (int, error) {
		someLock.Lock() // This lock will be stuck
		return 5, nil
	})
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, err := future.Get(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}()
	}
	wg.Wait()

	// Unstick the lock and get the result
	someLock.Unlock()
	res, err := future.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 5, res)
}

func TestResolveAll_Success(t *testing.T) {
	future1, err := SubmitFuture(context.Background(), &asyncGoroutineRunner{}, func() (int, error) {
		return 1, nil
	})
	require.NoError(t, err)
	future2, err := SubmitFuture(context.Background(), &asyncGoroutineRunner{}, func() (int, error) {
		return 2, nil
	})
	require.NoError(t, err)

	res, err := GetAll(context.Background(), future1, future2)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, res)
}

func TestResolveAll_Error(t *testing.T) {
	expectedErr := errors.New("some error")
	future1, err := SubmitFuture(context.Background(), &asyncGoroutineRunner{}, func() (int, error) {
		return 1, nil
	})
	require.NoError(t, err)
	future2, err := SubmitFuture(context.Background(), &asyncGoroutineRunner{}, func() (int, error) {
		return 2, expectedErr
	})
	require.NoError(t, err)
	future3, err := SubmitFuture(context.Background(), &asyncGoroutineRunner{}, func() (int, error) {
		return 3, nil
	})
	require.NoError(t, err)

	_, err = GetAll(context.Background(), future1, future2, future3)
	require.ErrorIs(t, expectedErr, err)
}
