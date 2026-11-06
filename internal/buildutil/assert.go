package buildutil

// Assert panics if cond is false. Guard calls with [TestMode] to skip evaluating
// the condition when assertions are disabled:
//
//	if buildutil.TestMode {
//		buildutil.Assert(cond)
//	}
func Assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}
