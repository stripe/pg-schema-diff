package util_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/util"
)

func Test_DoOnErrOrPanicIsCalledOnError(t *testing.T) {
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

func Test_DoOnErrOrPanicIsNotCalledOnNoError(t *testing.T) {
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

func Test_DoOnErrOrPanicIsCalledOnPanic(t *testing.T) {
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
