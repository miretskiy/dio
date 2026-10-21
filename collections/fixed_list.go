package collections

import "iter"

// FixedList is a fixed-capacity List whose values remain in stable storage.
// Removed values are retained for reuse; callers must clear references they
// no longer need. Construct one with MakeFixedList. A FixedList must not be
// copied after first use. It is not safe for concurrent access.
type FixedList[T any] struct {
	list List[T]
}

// MakeFixedList creates a FixedList with capacity slots.
func MakeFixedList[T any](capacity int) FixedList[T] {
	if capacity < 0 || uint64(capacity) > uint64(freeNode-1) {
		panic("collections: invalid fixed-list capacity")
	}
	var fixed FixedList[T]
	fixed.list.nodes = make([]node[T], capacity)
	if capacity != 0 {
		fixed.list.free = 1
	}
	for i := range fixed.list.nodes {
		fixed.list.nodes[i].prev = freeNode
		fixed.list.nodes[i].generation = 1
		if i+1 < len(fixed.list.nodes) {
			fixed.list.nodes[i].next = uint32(i + 2)
		}
	}
	return fixed
}

// PushBack acquires a slot. It panics when the list is full.
func (l *FixedList[T]) PushBack() Handle {
	handle, ok := l.TryPushBack()
	if !ok {
		panic("collections: fixed list is full")
	}
	return handle
}

// TryPushBack acquires a slot if one is available.
func (l *FixedList[T]) TryPushBack() (Handle, bool) {
	if l.Len() == len(l.list.nodes) {
		return 0, false
	}
	index, _ := l.list.pushBack()
	return l.list.handle(index), true
}

// Remove returns handle to the list without clearing its value.
func (l *FixedList[T]) Remove(handle Handle) {
	l.list.remove(handle, false)
}

// Value returns the value identified by handle. Its address remains stable.
func (l *FixedList[T]) Value(handle Handle) *T {
	return l.list.Value(handle)
}

// TryValue returns the value identified by handle, or false if handle is
// invalid, stale, or no longer occupied.
func (l *FixedList[T]) TryValue(handle Handle) (*T, bool) {
	return l.list.TryValue(handle)
}

// Front returns the first handle in l.
func (l *FixedList[T]) Front() (Handle, bool) {
	return l.list.Front()
}

// Back returns the last handle in l.
func (l *FixedList[T]) Back() (Handle, bool) {
	return l.list.Back()
}

// Next returns the handle after handle.
func (l *FixedList[T]) Next(handle Handle) (Handle, bool) {
	return l.list.Next(handle)
}

// Prev returns the handle before handle.
func (l *FixedList[T]) Prev(handle Handle) (Handle, bool) {
	return l.list.Prev(handle)
}

// All yields occupied slots in list order. The current slot may be removed
// during iteration; other mutations are unsupported.
func (l *FixedList[T]) All() iter.Seq2[Handle, *T] {
	return l.list.All()
}

// AllSlots yields every slot in storage order, including free slots and their
// retained values. Indices are storage positions, not handles. Values may be
// modified through the returned pointers; slot occupancy is unchanged.
func (l *FixedList[T]) AllSlots() iter.Seq2[int, *T] {
	return func(yield func(int, *T) bool) {
		for i := range l.list.nodes {
			if !yield(i, &l.list.nodes[i].value) {
				return
			}
		}
	}
}

// Len returns the number of occupied slots in l.
func (l *FixedList[T]) Len() int {
	return l.list.Len()
}

// Cap returns the number of slots in l.
func (l *FixedList[T]) Cap() int {
	return len(l.list.nodes)
}
