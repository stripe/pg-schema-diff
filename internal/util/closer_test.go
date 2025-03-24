package util_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/util"
)

func TestNoOpCloser(t *testing.T) {
	require.NoError(t, util.NoOpCloser().Close())
}

func TestDoOnErrOrPanicIsCalledOnError(t *testing.T) {
	var err error
	wasCalled := false
	defer func() {
		require.True(t, wasCalled)
	}()
	defer util.DoOnErrOrPanic(&err, func() {
		wasCalled = true
	})

	err = fmt.Errorf("some error")
	return
}

func TestDoOnErrOrPanicIsNotCalledOnNoError(t *testing.T) {
	var err error
	wasCalled := false
	defer func() {
		require.False(t, wasCalled)
	}()
	defer util.DoOnErrOrPanic(&err, func() {
		wasCalled = true
	})

	return
}

func TestDoOnErrOrPanicIsCalledOnPanic(t *testing.T) {
	var err error
	wasCalled := false
	defer func() {
		recover()
		require.True(t, wasCalled)
	}()
	defer util.DoOnErrOrPanic(&err, func() {
		wasCalled = true
	})

	panic("some panic")
}
