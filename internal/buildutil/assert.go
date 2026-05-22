package buildutil

import (
	"errors"
	"fmt"
	"runtime"
)

// Assert reports an invariant violation when cond is false. Returns nil when
// cond is true.
//
// Under tests ([TestMode] true) a failure panics so the bug is loud. In
// production a failure returns an error; the caller decides how to react.
//
// Assert takes no message: the hot path is one bool check plus a function call
// with no argument boxing, safe to use in tight loops. On failure, the file
// and line of the call site are captured via [runtime.Caller] so the panic /
// error still points to the assertion that fired. Callers that need dynamic
// context wrap the returned error with [fmt.Errorf]:
//
//	if err := buildutil.Assert(sqe != nil); err != nil {
//	    err = fmt.Errorf("GetSQE nil; slot=%d: %w", slot, err)
//	    sched.setFatalErr(err)
//	    sched.stop()
//	    return err
//	}
func Assert(cond bool) error {
	if cond {
		return nil
	}
	_, file, line, _ := runtime.Caller(1)
	msg := fmt.Sprintf("assertion failed at %s:%d", file, line)
	if TestMode() {
		panic(msg)
	}
	return errors.New(msg)
}
