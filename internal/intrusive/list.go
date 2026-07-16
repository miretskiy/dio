package intrusive

import "iter"

// Handle identifies a value until it is removed. Its zero value is invalid.
type Handle uint64

const (
	invalid  Handle = 0
	freeNode        = ^uint32(0)
)

type node[T any] struct {
	value      T
	prev       uint32
	next       uint32
	generation uint32
}

// List is an indexed doubly linked list. Values stay in one canonical node
// while other structures refer to them by Handle. Removed nodes are reused.
type List[T any] struct {
	nodes []node[T]
	head  uint32
	tail  uint32
	free  uint32
	len   int
}

// MakeList returns an empty list with space for capacity values.
func MakeList[T any](capacity int) List[T] {
	return List[T]{nodes: make([]node[T], 0, capacity)}
}

// PushBack adds value to the end of l and returns its handle.
func (l *List[T]) PushBack(value T) Handle {
	index, n := l.pushBack()
	n.value = value
	return l.handle(index)
}

func (l *List[T]) pushBack() (uint32, *node[T]) {
	var index uint32
	if l.free != 0 {
		index = l.free
		n := &l.nodes[index-1]
		l.free = n.next
		n.prev = l.tail
		n.next = 0
	} else {
		if uint64(len(l.nodes)) == uint64(freeNode-1) {
			panic("intrusive: handle space exhausted")
		}
		l.nodes = append(l.nodes, node[T]{
			prev:       l.tail,
			generation: 1,
		})
		index = uint32(len(l.nodes))
	}

	if l.tail == 0 {
		l.head = index
	} else {
		l.nodes[l.tail-1].next = index
	}
	l.tail = index
	l.len++
	return index, &l.nodes[index-1]
}

// Remove removes handle from l. References held by its value are cleared.
func (l *List[T]) Remove(handle Handle) {
	l.remove(handle, true)
}

func (l *List[T]) remove(handle Handle, clearValue bool) {
	index, n := l.node(handle)
	if n.prev == 0 {
		l.head = n.next
	} else {
		l.nodes[n.prev-1].next = n.next
	}
	if n.next == 0 {
		l.tail = n.prev
	} else {
		l.nodes[n.next-1].prev = n.prev
	}

	if clearValue {
		var zero T
		n.value = zero
	}
	n.prev = freeNode
	n.next = l.free
	n.generation++
	l.free = index
	l.len--
}

// Value returns the value identified by handle. The returned pointer must not
// be retained across PushBack, which may grow the list's backing storage.
func (l *List[T]) Value(handle Handle) *T {
	_, n := l.node(handle)
	return &n.value
}

// Front returns the first handle in l.
func (l *List[T]) Front() (Handle, bool) {
	return l.handle(l.head), l.head != 0
}

// Back returns the last handle in l.
func (l *List[T]) Back() (Handle, bool) {
	return l.handle(l.tail), l.tail != 0
}

// Next returns the handle after handle.
func (l *List[T]) Next(handle Handle) (Handle, bool) {
	_, n := l.node(handle)
	return l.handle(n.next), n.next != 0
}

// Prev returns the handle before handle.
func (l *List[T]) Prev(handle Handle) (Handle, bool) {
	_, n := l.node(handle)
	return l.handle(n.prev), n.prev != 0
}

// All yields entries in list order. The current entry may be removed during
// iteration; other mutations are unsupported.
func (l *List[T]) All() iter.Seq2[Handle, *T] {
	return func(yield func(Handle, *T) bool) {
		for handle, ok := l.Front(); ok; {
			next, hasNext := l.Next(handle)
			if !yield(handle, l.Value(handle)) {
				return
			}
			handle, ok = next, hasNext
		}
	}
}

// Len returns the number of values in l.
func (l *List[T]) Len() int {
	return l.len
}

func (l *List[T]) node(handle Handle) (uint32, *node[T]) {
	index := handle.index()
	if uint64(index) > uint64(len(l.nodes)) {
		panic("intrusive: invalid handle")
	}
	n := &l.nodes[index-1]
	if n.generation != handle.generation() || n.prev == freeNode {
		panic("intrusive: invalid handle")
	}
	return index, n
}

func (l *List[T]) handle(index uint32) Handle {
	if index == 0 {
		return invalid
	}
	return Handle(uint64(l.nodes[index-1].generation)<<32 | uint64(index))
}

func (h Handle) index() uint32 {
	index := uint32(h)
	if index == 0 {
		panic("intrusive: invalid handle")
	}
	return index
}

func (h Handle) generation() uint32 {
	return uint32(uint64(h) >> 32)
}
