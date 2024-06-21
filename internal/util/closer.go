package util

// DoOnErrOrPanic calls f if the value of err is not nil or if the goroutine is
// panicking. If there is a panic, it is rethrown.
//
// DoOnErrOrPanic should be called as a defer function to properly handle panics:
//
//	defer DoOnErrOrPanic(&returnErr, func() {
//		db.Close()
//	})
func DoOnErrOrPanic(err *error, f func()) {
	// err needs to be a pointer, otherwise it will be evaluated at defer
	// creation time as a copy of the passed in variable. This doesn't play well
	// with ie named return value errors, where you want the final value of the
	// return error to be checked.
	p := recover()
	if *err != nil || p != nil {
		f()
	}
	if p != nil {
		panic(p)
	}
}
