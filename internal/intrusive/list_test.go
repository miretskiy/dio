package intrusive

import "testing"

func TestMakeList(t *testing.T) {
	list := MakeList[int](4)
	if list.Len() != 0 || cap(list.nodes) != 4 {
		t.Fatalf("made list: len=%d capacity=%d", list.Len(), cap(list.nodes))
	}
	for i := range 4 {
		list.PushBack(i)
	}
	if len(list.nodes) != 4 || cap(list.nodes) != 4 {
		t.Fatalf("filled list: nodes=%d capacity=%d", len(list.nodes), cap(list.nodes))
	}
}

func TestListTraversal(t *testing.T) {
	var list List[string]
	first := list.PushBack("first")
	second := list.PushBack("second")
	third := list.PushBack("third")

	if list.Len() != 3 {
		t.Fatalf("length: got %d want 3", list.Len())
	}
	if got, ok := list.Front(); !ok || got != first {
		t.Fatalf("front: got (%d, %v) want (%d, true)", got, ok, first)
	}
	if got, ok := list.Back(); !ok || got != third {
		t.Fatalf("back: got (%d, %v) want (%d, true)", got, ok, third)
	}
	if got, ok := list.Next(first); !ok || got != second {
		t.Fatalf("first next: got (%d, %v) want (%d, true)", got, ok, second)
	}
	if got, ok := list.Prev(third); !ok || got != second {
		t.Fatalf("third prev: got (%d, %v) want (%d, true)", got, ok, second)
	}
	if got, ok := list.Next(third); ok || got != invalid {
		t.Fatalf("third next: got (%d, %v) want (0, false)", got, ok)
	}

	var values []string
	for handle, ok := list.Front(); ok; handle, ok = list.Next(handle) {
		values = append(values, *list.Value(handle))
	}
	want := []string{"first", "second", "third"}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("values[%d]: got %q want %q", i, values[i], want[i])
		}
	}
}

func TestListRemoveAnyPosition(t *testing.T) {
	var list List[int]
	first := list.PushBack(1)
	middle := list.PushBack(2)
	last := list.PushBack(3)

	list.Remove(middle)
	if next, ok := list.Next(first); !ok || next != last {
		t.Fatalf("next after middle removal: got (%d, %v) want (%d, true)", next, ok, last)
	}
	if prev, ok := list.Prev(last); !ok || prev != first {
		t.Fatalf("prev after middle removal: got (%d, %v) want (%d, true)", prev, ok, first)
	}

	list.Remove(first)
	if front, ok := list.Front(); !ok || front != last {
		t.Fatalf("front after first removal: got (%d, %v) want (%d, true)", front, ok, last)
	}

	list.Remove(last)
	if list.Len() != 0 {
		t.Fatalf("length after removing all values: got %d want 0", list.Len())
	}
	if front, ok := list.Front(); ok || front != invalid {
		t.Fatalf("empty front: got (%d, %v) want (0, false)", front, ok)
	}
	if back, ok := list.Back(); ok || back != invalid {
		t.Fatalf("empty back: got (%d, %v) want (0, false)", back, ok)
	}
}

func TestListReusesAndScrubsRemovedNode(t *testing.T) {
	type item struct {
		name string
		ptr  *int
	}

	var list List[item]
	value := 1
	handle := list.PushBack(item{name: "old", ptr: &value})
	list.Remove(handle)

	n := &list.nodes[handle.index()-1]
	if n.value.name != "" || n.value.ptr != nil || n.prev != freeNode {
		t.Fatalf("removed node was not scrubbed: %+v", n)
	}

	reused := list.PushBack(item{name: "new"})
	if reused.index() != handle.index() {
		t.Fatalf("reused slot: got %d want %d", reused.index(), handle.index())
	}
	if reused == handle {
		t.Fatalf("reused handle did not advance generation: %d", reused)
	}
	if got := list.Value(reused).name; got != "new" {
		t.Fatalf("reused value: got %q want new", got)
	}
	assertPanics(t, "stale handle", func() { list.Value(handle) })
}

func TestListHandlesSurviveGrowth(t *testing.T) {
	var list List[int]
	const count = 1024
	handles := make([]Handle, count)
	for i := range handles {
		handles[i] = list.PushBack(i)
	}

	for i, handle := range handles {
		*list.Value(handle) += count
		if got := *list.Value(handle); got != i+count {
			t.Fatalf("value %d after growth: got %d want %d", i, got, i+count)
		}
	}
}

func TestListRejectsInvalidOrRemovedHandle(t *testing.T) {
	var list List[int]
	handle := list.PushBack(1)
	list.Remove(handle)

	assertPanics(t, "zero handle", func() { list.Value(0) })
	assertPanics(t, "removed handle", func() { list.Value(handle) })
}

func assertPanics(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s did not panic", name)
		}
	}()
	fn()
}
