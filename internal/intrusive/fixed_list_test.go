package intrusive

import "testing"

func TestFixedListCapacityAndReuse(t *testing.T) {
	list := MakeFixedList[int](2)
	first := list.PushBack()
	second := list.PushBack()
	if second == first {
		t.Fatal("PushBack reused an occupied slot")
	}
	assertPanics(t, "PushBack past capacity", func() { list.PushBack() })
	if list.Len() != 2 || list.Cap() != 2 {
		t.Fatalf("size: len=%d cap=%d", list.Len(), list.Cap())
	}

	*list.Value(first) = 42
	list.Remove(first)
	reused := list.PushBack()
	if reused.index() != first.index() || reused == first {
		t.Fatalf("reused handle: old=%d new=%d", first, reused)
	}
	if *list.Value(reused) != 42 {
		t.Fatalf("retained value: got %d want 42", *list.Value(reused))
	}
	assertPanics(t, "stale handle", func() { list.Value(first) })
}

func TestFixedListTraversal(t *testing.T) {
	list := MakeFixedList[int](4)
	for _, value := range []int{1, 2, 3} {
		handle := list.PushBack()
		*list.Value(handle) = value
	}

	var sum int
	for _, value := range list.All() {
		sum += *value
	}
	if sum != 6 {
		t.Fatalf("range sum: got %d want 6", sum)
	}
}

func TestFixedListTraversalMayRemoveCurrentSlot(t *testing.T) {
	list := MakeFixedList[int](4)
	for range 3 {
		list.PushBack()
	}
	for handle := range list.All() {
		list.Remove(handle)
	}
	if list.Len() != 0 {
		t.Fatalf("occupied slots after traversal: %d", list.Len())
	}
}

func TestFixedListRejectsReturnedHandle(t *testing.T) {
	list := MakeFixedList[int](1)
	handle := list.PushBack()
	list.Remove(handle)
	assertPanics(t, "returned handle", func() { list.Remove(handle) })
	assertPanics(t, "zero handle", func() { list.Value(0) })
}
