package intrusive

import "iter"

// FixedList is a fixed-capacity List whose values remain in stable storage.
// Removed values are retained for reuse; callers must clear live references
// before Remove.
type FixedList[T any] struct {
	list List[T]
}

// MakeFixedList creates a FixedList with capacity slots.
func MakeFixedList[T any](capacity int) FixedList[T] {
	if capacity < 0 || uint64(capacity) > uint64(freeNode-1) {
		panic("intrusive: invalid fixed-list capacity")
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
	if l.Len() == len(l.list.nodes) {
		panic("intrusive: fixed list is full")
	}
	index, _ := l.list.pushBack()
	return l.list.handle(index)
}

// Remove returns handle to the list without clearing its value.
func (l *FixedList[T]) Remove(handle Handle) {
	l.list.remove(handle, false)
}

// Value returns the value identified by handle. Its address remains stable.
func (l *FixedList[T]) Value(handle Handle) *T {
	return l.list.Value(handle)
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

// Len returns the number of occupied slots in l.
func (l *FixedList[T]) Len() int {
	return l.list.Len()
}

// Cap returns the number of slots in l.
func (l *FixedList[T]) Cap() int {
	return len(l.list.nodes)
}
