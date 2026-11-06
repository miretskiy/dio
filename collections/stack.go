// # Lock-Free Stacks
package collections

import (
	"sync/atomic"
	"unsafe"
)

// Link must be a field of T to use it with a TreiberStack.
type Link[T any] struct {
	next *T
}

// TreiberStack accumulates intrusive nodes for batch consumption. A node must
// belong to at most one stack and may not be pushed again until its consumer
// yields ownership. A stack must not be copied after first use.
type TreiberStack[T any] struct {
	head   atomic.Pointer[T]
	offset uintptr // offset to the Link[T] embedded in T
}

// NewTreiberStack records the offset of the Link field selected by linker.
func NewTreiberStack[T any](linker func(*T) *Link[T]) *TreiberStack[T] {
	var zero T
	link := linker(&zero)
	baseAddr := uintptr(unsafe.Pointer(&zero))
	linkAddr := uintptr(unsafe.Pointer(link))

	structSize := unsafe.Sizeof(zero)
	if linkAddr < baseAddr || structSize < unsafe.Sizeof(*link) || linkAddr-baseAddr > structSize-unsafe.Sizeof(*link) {
		panic("intrusive: returned pointer is outside struct bounds")
	}

	return &TreiberStack[T]{offset: linkAddr - baseAddr}
}

// pNext returns pointer to next linked item
func (s *TreiberStack[T]) pNext(item *T) **T {
	return (**T)(unsafe.Add(unsafe.Pointer(item), s.offset))
}

// Push atomically adds an item to the top of the stack.
// It is lock-free and safe for concurrent producers owning distinct nodes.
func (s *TreiberStack[T]) Push(item *T) {
	next := s.pNext(item)
	for {
		oldHead := s.head.Load()

		// The producer owns item until a successful CAS publishes it.
		*next = oldHead

		if s.head.CompareAndSwap(oldHead, item) {
			return
		}
	}
}

// ConsumeAll atomically detaches the entire pending batch and yields items in
// native LIFO (Last-In, First-Out) order. Safe for multiple concurrent consumers.
// yield owns each detached item and may push it again. It must not panic if the
// remaining detached items need to be processed.
func (s *TreiberStack[T]) ConsumeAll(yield func(item *T)) {
	if s.head.Load() == nil {
		return
	}
	current := s.head.Swap(nil)

	// This consumer owns the detached list. Capture the next node before yield
	// so reusing the current node cannot change the traversal.
	for current != nil {
		next := s.pNext(current)
		nextItem := *next

		*next = nil // Prevent memory retention chains
		yield(current)

		current = nextItem
	}
}
