package concurrent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGoroutineLimiter_Go_Concurrent(t *testing.T) {
	limiter := NewGoroutineLimiter(2)

	wg := sync.WaitGroup{}
	routine2CompleteChan := make(chan any)
	completionOrderChan := make(chan int, 3)

	wg.Add(3)
	require.NoError(t, limiter.Go(context.Background(), func() {
		defer wg.Done()
		defer func() {
			completionOrderChan <- 1
		}()

		// Block until routine 2 completes to validate that the function is being running concurrently
		<-routine2CompleteChan
	}))
	require.NoError(t, limiter.Go(context.Background(), func() {
		defer wg.Done()
		defer func() {
			completionOrderChan <- 2
		}()

		time.Sleep(500 * time.Millisecond) // Make the routine take longer than the third routine in most cases

		routine2CompleteChan <- struct{}{}
	}))
	// Spin up a third go routine to validate the limit is being respected
	require.NoError(t, limiter.Go(context.Background(), func() {
		defer wg.Done()
		defer func() {
			completionOrderChan <- 3
		}()
	}))

	wg.Wait()

	var completionOrder []int
	for val := range completionOrderChan {
		completionOrder = append(completionOrder, val)
		if len(completionOrder) == 3 {
			close(completionOrderChan)
		}
	}

	require.NotEqual(t, int64(3), completionOrder[0], "the third routine cannot complete first, otherwise the go routine limit is not being respected.")
}

func TestGoroutineLimiter_Go_Serial(t *testing.T) {
	limiter := NewGoroutineLimiter(1)

	wg := sync.WaitGroup{}
	orderingChan := make(chan int, 3)

	wg.Add(3)
	require.NoError(t, limiter.Go(context.Background(), func() {
		defer wg.Done()
		orderingChan <- 1
	}))
	require.NoError(t, limiter.Go(context.Background(), func() {
		defer wg.Done()
		orderingChan <- 2
	}))
	require.NoError(t, limiter.Go(context.Background(), func() {
		defer wg.Done()
		orderingChan <- 3
	}))
	wg.Wait()
	close(orderingChan)

	var ordering []int
	for routineIdx := range orderingChan {
		ordering = append(ordering, routineIdx)
	}

	assert.Equal(t, []int{1, 2, 3}, ordering)
}
