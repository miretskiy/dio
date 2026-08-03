// Package ringo provides a low-level, lifetime-safe Go interface to Linux
// io_uring.
//
// Ringo exposes typed operations, ring-scoped handles, and owned registered
// resources. It does not expose raw submission or completion entries,
// user_data, integer-encoded Go pointers, or manual completion-queue
// advancement. It adds no goroutines, futures, waiters, scheduling policy, or
// synchronization.
//
// # Platform
//
// Ringo is Linux-only. Programs using its API must be built with GOOS=linux.
// New requires IORING_SETUP_SUBMIT_ALL, IORING_FEAT_LINKED_FILE, and
// IORING_FEAT_NODROP. In an unmodified mainline kernel that makes Linux 5.18
// the minimum; distribution kernels may backport individual capabilities.
//
// # Overview
//
// The usual lifecycle is:
//
//  1. Create and configure an Op.
//  2. Transfer it to a Ring with Push or PushLinked.
//  3. Enter the kernel with Submit or SubmitAndWait.
//  4. Consume completions with Reap.
//
// For example:
//
//	ring, err := New(WithDepth(64))
//	if err != nil {
//		return err
//	}
//	defer ring.Close()
//
//	op := Read(FileFD(file), buffer, 0)
//	handle, err := ring.Push(op)
//	if err != nil {
//		return err
//	}
//	if _, err := ring.SubmitAndWait(1); err != nil {
//		return err
//	}
//	for completion := range ring.Reap() {
//		if completion.Handle == handle {
//			// Inspect completion.Result and completion.Err.
//		}
//	}
//
// A successful Push transfers the Op to the Ring permanently. The caller's
// right to use the Op does not return after completion; only the Handle remains
// caller-visible. The Ring retains the operation's state until Reap yields its
// final completion and that iterator step ends. PushLinked performs the same
// transfer atomically for a complete linked sequence: validation or capacity
// failure queues and transfers nothing.
//
// The request and ownership flow is:
//
//	create and configure Op                    caller owns Op
//	          |
//	          | successful Push / PushLinked
//	          v
//	pending slot + encoded SQE                 Ring owns Op
//	          |
//	          | Submit / SubmitAndWait
//	          v
//	       kernel
//	          |
//	          | CQE
//	          v
//	Reap resolves user_data and yields Completion
//	          |
//	          | iterator step ends
//	          v
//	advance CQ
//	          |
//	          +-- IORING_CQE_F_MORE --> retain Op and resources
//	          |
//	          `-- final CQE ---------> release or recycle internal state
//
// Submit and SubmitAndWait never reap. The caller must continue calling Reap
// until every pushed operation reaches a final completion. If the caller does
// not reap, CQ and pending-slot capacity remain occupied and Push
// eventually returns ErrFull. Ringo has no background reaper.
//
// A submit call reports either progress or an error, never both:
// io_uring_enter returns the number of entries it consumed, and only reports a
// wait error when it consumed none. An error does not return ownership of
// pushed operations to the caller either way.
//
// Ring.Close always closes the io_uring descriptor and never waits. Reaping
// every pushed operation first is the orderly close, and the only one that
// releases everything. Closing with operations still pending is allowed but
// permanently retains the Ring and their operands, and reports ErrPending:
// kernel ring teardown is asynchronous and unobservable, so it is not an
// operation-lifetime barrier, and no later moment is provably safe either. What
// one such Close retains is bounded by that Ring's capacity, but nothing is ever
// released, so a program that repeatedly closes Rings with work in flight
// accumulates one retention per Ring for the life of the process.
//
// # Operations and memory safety
//
// io_uring SQEs contain addresses that the kernel may use after io_uring_enter
// returns. A Go goroutine stack can move as it grows. Typed pointers are
// updated when that happens, but an address already converted to uintptr or
// uint64 in an SQE is not:
//
//	unsafe raw preparation:
//	stack object -> integer address in SQE -> stack moves -> stale address
//
//	Ringo:
//	typed Op fields -> ring-owned pending Op -> encode heap address in SQE
//	                                               |
//	                                               v
//	                                    final CQE -> release Op
//
// Constructors keep kernel operands as typed Go fields. Input-only values are
// copied when practical; examples include paths, timespecs, and open_how
// values. Buffers, output structures, and other caller-visible memory are
// retained because copying would change the operation's result.
//
// Push stores the concrete Op in the Ring's heap-owned pending table before
// the Op encodes a complete SQE. This forces the operation and its reachable
// backing objects to escape before their addresses become integers. The Ring
// retains those typed references through the final CQE. IORING_CQE_F_MORE
// therefore extends the retention interval for supported multishot operations.
//
// Ringo relies explicitly on the standard Go runtime's non-moving heap.
// Reachability prevents reclamation, and the supported heap collector does not
// relocate reachable objects. This is a runtime implementation dependency, not
// a Go language guarantee; a future moving collector requires a compatibility
// review. Buffers backed by mmap are outside the Go heap and independently
// stable.
//
// Ringo does not use runtime.Pinner. Under the stated non-moving-heap contract,
// pinning every operation would add cost without changing the required
// lifetime rule. Registration lengthens that lifetime but does not otherwise
// change it.
//
// runtime.KeepAlive is not a substitute for this ownership model. It can keep
// an object reachable until a point in the current function, but it does not
// pin memory, repair a stale integer address, or retain an operand through a
// later CQE. Ringo uses KeepAlive only where a synchronous syscall requires it.
//
// A successful Push or PushLinked takes ownership of each Op permanently. The
// caller must discard every interface copy of it and instead use its opaque
// Handle. A call that returns an error takes no Op.
//
// Go cannot express that transfer the way a move-only type would, so the rule
// is documented rather than enforced: Ringo does not track or diagnose reuse.
// An operation constructed WithAlloc returns to that OpAlloc once the Ring has
// released its final completion, so a retained alias can come to refer to an
// unrelated operation. Using a pushed Op again is undefined.
//
// Ringo keeps no pool of its own. An OpAlloc is caller-owned and opted into per
// operation, so recycling is a decision the caller makes rather than one the
// library makes on its behalf. It is safe to share across goroutines, because
// Reap returns operations to it and reaping need not happen on the goroutine
// that built them.
//
// Retention keeps referenced memory alive; it does not freeze that memory or
// revoke the caller's other aliases. These rules apply to every alias of an
// operand. While an operation is active, the caller must not:
//
//   - modify memory the kernel may read;
//   - read or modify memory the kernel may write;
//   - explicitly close a retained file;
//   - submit conflicting operations over the same output without ordering.
//
// For example, copying a slice value does not create independent storage.
// After pushing a Read using buf, neither buf nor any copy of its slice header
// may be accessed until the final completion's iterator step ends. After
// pushing a Write, no alias of its input buffer may be modified during that
// interval. Ordinary I/O buffers are deliberately not copied.
//
// # Operation configuration and ownership
//
// PushLinked accepts a first Op followed by one or more Links. Each Link
// contains the next Op and controls the edge from the preceding operation to
// that Op. This represents mixed soft and hard links without mutating an Op or
// permitting a dangling link with no successor. PushLinked requires
// IORING_SETUP_SUBMIT_ALL because kernel links cannot cross a short submission
// boundary. New requires that setup mode for every Ring. Drain mutates an
// unsubmitted Op and creates a broader barrier: it waits for all earlier
// requests, and later requests wait for it.
//
// A FixedFile names a table slot, not a particular file generation. Linking a
// direct open to an operation that uses the same slot requires a kernel with
// IORING_FEAT_LINKED_FILE. Without that feature, the kernel may resolve the
// later operation's file before the open installs it. New therefore requires
// this feature for every Ring.
//
// PushLinked takes every referenced Op only after it has validated and reserved
// the complete sequence, so a failed call leaves the Ring and every Op
// untouched. Each element must reference a distinct Op. The variadic []Link
// backing array is not retained. Ringo has no caller-visible Reset or pooling
// API.
//
// # Completion and concurrency rules
//
// Handle is an opaque, comparable, generation-safe identity local to one Ring;
// it is not a future or waiter. Completion contains the nonnegative CQE result,
// or the syscall error represented by a negative result, plus the kernel CQE
// flags.
//
// Reap is nonblocking and visits a bounded snapshot of ready CQEs. It decodes
// each kernel-owned CQE into an independent Completion value before yielding
// it. When the iterator resumes, breaks, or panics, Ringo advances that CQE
// and releases final operation state.
//
// Ring methods do not synchronize with one another. Calls operating on the same
// Ring must not overlap; callers may provide external serialization. This is a
// Ringo API constraint, not a limitation of the kernel interface. The Reap
// iterator exclusively borrows the Ring; calling another Ring method from its
// loop body is invalid.
//
// CancelAll is the single exception, and it may overlap another call on the
// same Ring except Close. That is what lets one goroutine break another out of
// a blocking SubmitAndWait during shutdown, without which a Ring holding
// uncancelled work could not be drained at all. Close must not begin until the
// cancellation call and every other Ring call have returned.
//
// Lookups on FixedFiles and FixedBuffers are not Ring methods and carry no such
// constraint. Neither lookup mutates anything, and the state they read is fixed
// for the table's whole lifetime, including across Ring.Close, so a submitting
// goroutine may validate a buffer while another reaps. FixedFiles.Update is the
// exception: it issues a registration syscall on the Ring and rewrites the
// table's retained files, so it must be serialized with Ring calls exactly as
// though it were one.
//
// # File descriptors and registered resources
//
// FD makes descriptor lifetime explicit without transferring ownership. The
// Ring never closes a descriptor it did not open:
//
//   - FileFD retains an *os.File through final completion so it cannot be
//     finalized mid-operation. The caller still owns and closes the file.
//   - FixedFD names a typed slot in this Ring's fixed-file table. Linux does
//     not accept a fixed-file descriptor as the directory argument of OpenAt,
//     OpenAt2, or StatxAt; those must name their directory with one of the
//     other two.
//   - BorrowedFD leaves descriptor lifetime entirely to the caller.
//
// Reachability prevents an os.File finalizer from closing a descriptor, but it
// cannot prevent an explicit Close. CloseFD therefore accepts a borrowed
// descriptor rather than an *os.File; closing a fixed-file slot uses
// CloseDirect.
//
// FixedFiles and FixedBuffers are typed, Ring-owned resources. They are scoped
// to one Ring and remain registered until Ring.Close.
//
// A fixed-buffer table is immutable. Register it once, then use FixedBuffer
// values returned by Buffer. Ringo retains the backing slices for the Ring's
// lifetime.
//
// A fixed-file table may be initialized with files or created sparse.
// OpenAtDirect and OpenAt2Direct can populate an empty slot, and CloseDirect
// can release one. FixedFile values continue to name the slot if it is later
// reused; callers own allocation and reuse policy and must wait for the old
// file's final operation and close completions before reuse. Files passed to
// RegisterFiles are retained until Update replaces or clears their slot, or for
// as long as the table remains reachable.
//
// FixedFiles.Update synchronously replaces or clears existing slots. Linux
// keeps displaced resources alive for requests already using them, so Ringo
// needs no per-operation active counter. Update is not an ordering barrier:
// callers own the slot lifecycle and must decide when a replacement may become
// visible. Ringo deliberately provides no explicit fixed-resource unregister
// API; Ring.Close owns whole-table teardown.
//
// # Kernel ABI
//
// Ringo's private SQE, CQE, setup, registration layouts, and UAPI constants are
// generated from the bundled Linux header with cgo -godefs and committed as
// ordinary Go source. Production builds do not import C or require CGo. The
// independent liburing conformance tests compare complete generated SQEs with
// liburing.
//
// # Scope
//
// Ringo covers common file I/O, timeouts, cancellation, polling, fixed-file
// resources, and immutable fixed-buffer resources.
//
// It deliberately does not expose networking operation families, provided
// buffers, generic URING_CMD bytes, personalities, restrictions, message
// rings, resource tags, zero-copy queues, or 128-byte and mixed SQE/CQE modes.
// Networking can be reconsidered when a useful Go-native workload demonstrates
// an API and performance benefit that justify its ownership complexity. The
// other features likewise need additional pointer, completion, cross-ring, or
// buffer-consumption models rather than more raw constants.
package ringo
