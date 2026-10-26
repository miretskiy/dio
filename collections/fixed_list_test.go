package collections

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
	if *list.Value(reused) != 0 {
		t.Fatalf("removed value: got %d want 0", *list.Value(reused))
	}
	assertPanics(t, "stale handle", func() { list.Value(first) })
}

func TestFixedListRecycle(t *testing.T) {
	list := MakeFixedList[*int](1)
	value := 42
	handle := list.PushBack()
	slot := list.Value(handle)
	*slot = &value
	list.Recycle(handle)
	if list.Len() != 0 || *slot != &value {
		t.Fatal("Recycle did not free the slot and retain its value")
	}
	if _, ok := list.TryValue(handle); ok {
		t.Fatal("Recycle did not invalidate the handle")
	}
	assertPanics(t, "recycled handle", func() { list.Recycle(handle) })

	reused := list.PushBack()
	if reused.index() != handle.index() || reused == handle {
		t.Fatalf("reused handle: old=%d new=%d", handle, reused)
	}
	if list.Value(reused) != slot || *list.Value(reused) != &value {
		t.Fatal("PushBack did not preserve the recycled slot and value")
	}
	assertPanics(t, "stale handle", func() { list.Value(handle) })

	list.Remove(reused)
	if *slot != nil {
		t.Fatal("Remove did not clear the retained reference")
	}
}

func TestFixedListRecycleAllocs(t *testing.T) {
	list := MakeFixedList[int](1)
	if allocations := testing.AllocsPerRun(1000, func() {
		list.Recycle(list.PushBack())
	}); allocations != 0 {
		t.Fatalf("PushBack/Recycle allocated %g times, want 0", allocations)
	}
}

func TestFixedListTryOperations(t *testing.T) {
	list := MakeFixedList[int](1)
	handle, ok := list.TryPushBack()
	if !ok {
		t.Fatal("TryPushBack rejected an available slot")
	}
	if _, ok := list.TryPushBack(); ok {
		t.Fatal("TryPushBack accepted a full list")
	}
	if value, ok := list.TryValue(handle); !ok || value != list.Value(handle) {
		t.Fatal("TryValue did not return the occupied slot")
	}
	list.Remove(handle)
	for _, invalid := range []Handle{0, handle, Handle(1<<32 | 2)} {
		if value, ok := list.TryValue(invalid); ok || value != nil {
			t.Fatalf("TryValue accepted invalid handle %d", invalid)
		}
	}
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

func TestFixedListAllSlots(t *testing.T) {
	list := MakeFixedList[int](3)
	first := list.PushBack()
	*list.Value(first) = 10
	second := list.PushBack()
	*list.Value(second) = 20
	list.Recycle(first)

	// Include a returned slot, an occupied slot, and a never-used slot.
	want := []int{10, 20, 0}
	count := 0
	for i, value := range list.AllSlots() {
		if *value != want[i] {
			t.Fatalf("slot %d: got %d, want %d", i, *value, want[i])
		}
		*value += 1
		count++
	}
	if count != list.Cap() || list.Len() != 1 {
		t.Fatalf("visited %d slots; occupied %d", count, list.Len())
	}
	if *list.Value(second) != 21 || *list.Value(list.PushBack()) != 11 {
		t.Fatal("slot updates were not retained")
	}

	count = 0
	for range list.AllSlots() {
		count++
		break
	}
	if count != 1 {
		t.Fatalf("early stop visited %d slots", count)
	}
	empty := MakeFixedList[int](0)
	for range empty.AllSlots() {
		t.Fatal("empty list yielded a slot")
	}
}
