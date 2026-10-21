package collections

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardedMap(t *testing.T) {
	m := NewShardedMap[string, int, struct{}](3, 10, nil)
	require.Zero(t, m.Len())
	value, ok := m.Get("missing")
	require.False(t, ok)
	require.Zero(t, value)

	m.Put("one", 1)
	m.Put("two", 2)
	m.Put("one", 11)
	value, ok = m.Get("one")
	require.True(t, ok)
	require.Equal(t, 11, value)
	require.Equal(t, 2, m.Len())
	require.Equal(t, map[string]int{"one": 11, "two": 2}, maps.Collect(m.All()))
	require.ElementsMatch(t, []string{"one", "two"}, slices.Collect(m.Keys()))
	require.ElementsMatch(t, []int{11, 2}, slices.Collect(m.Values()))

	collected := m.Collect([]int{-1})
	require.Equal(t, -1, collected[0])
	require.ElementsMatch(t, []int{11, 2}, collected[1:])
	require.True(t, m.Delete("one"))
	require.False(t, m.Delete("one"))
	_, ok = m.Get("one")
	require.False(t, ok)
	m.Clear()
	require.Zero(t, m.Len())
	require.Empty(t, maps.Collect(m.All()))
	require.Equal(t, []int{-1}, m.Collect([]int{-1}))
	m.Put("after clear", 3)
	value, ok = m.Get("after clear")
	require.True(t, ok)
	require.Equal(t, 3, value)
}

func TestShardedMapShardCounts(t *testing.T) {
	for _, count := range []int{1, 3, 4, 6, 16} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			m := NewShardedMap[uint64, uint64, struct{}](count, 0, func(key uint64) uint64 { return key })
			require.Equal(t, count, m.ShardCount())
			for key := range uint64(count * 3) {
				require.Same(t, m.ShardAt(int(key%uint64(count))), m.Shard(key))
				m.Put(key, key+1)
				value, ok := m.Get(key)
				require.True(t, ok)
				require.Equal(t, key+1, value)
			}
			require.Same(t, m.ShardAt(int(^uint64(0)%uint64(count))), m.Shard(^uint64(0)))
		})
	}
	require.Panics(t, func() { NewShardedMap[int, int, struct{}](0, 0, nil) })
	require.Panics(t, func() { NewShardedMap[int, int, struct{}](-1, 0, nil) })
	require.Panics(t, func() { NewShardedMap[int, int, struct{}](4, -1, nil) })
}

func TestShardedMapPrehashedKeys(t *testing.T) {
	type key struct{ lo, hi uint64 }
	m := NewShardedMap[key, string, struct{}](4, 0, func(k key) uint64 { return k.lo })
	first, second := key{lo: 7, hi: 1}, key{lo: 7, hi: 2}
	require.Same(t, m.Shard(first), m.Shard(second))
	m.Put(first, "first")
	m.Put(second, "second")
	value, ok := m.Get(first)
	require.True(t, ok)
	require.Equal(t, "first", value)
	value, ok = m.Get(second)
	require.True(t, ok)
	require.Equal(t, "second", value)
}

func TestShardedMapExtra(t *testing.T) {
	m := NewShardedMap[int, int, int](1, 0, nil)
	s := m.Shard(1)
	s.Lock()
	s.Items[1], s.Items[2] = 10, 20
	s.Extra = 30
	s.Unlock()

	sum := 0
	m.ForEach(func(key, value int, extra *int) bool {
		require.Equal(t, 30, *extra)
		sum += value
		return true
	})
	require.Equal(t, 30, sum)
	m.Clear()
	s.RLock()
	require.Empty(t, s.Items)
	require.Equal(t, 30, s.Extra, "Extra remains owned by the caller")
	s.RUnlock()
}

func TestShardedMapIterationUnlocks(t *testing.T) {
	for _, mode := range []string{"ForEach", "All", "Keys", "Values"} {
		for _, panics := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/panic=%t", mode, panics), func(t *testing.T) {
				m := NewShardedMap[int, int, struct{}](4, 0, nil)
				for key := range 20 {
					m.Put(key, key)
				}
				visited := 0
				visit := func() bool {
					visited++
					if panics {
						panic("iteration stopped")
					}
					return false
				}
				run := func() {
					switch mode {
					case "ForEach":
						m.ForEach(func(int, int, *struct{}) bool { return visit() })
					case "All":
						for range m.All() {
							if !visit() {
								break
							}
						}
					case "Keys":
						for range m.Keys() {
							if !visit() {
								break
							}
						}
					case "Values":
						for range m.Values() {
							if !visit() {
								break
							}
						}
					}
				}
				if panics {
					require.PanicsWithValue(t, "iteration stopped", run)
				} else {
					run()
				}
				require.Equal(t, 1, visited)
				for i := range m.ShardCount() {
					s := m.ShardAt(i)
					require.True(t, s.TryLock(), "shard %d stayed locked", i)
					s.Unlock()
				}
			})
		}
	}
}

func TestShardedMapInvalidKeyUnlocks(t *testing.T) {
	m := NewShardedMap[any, int, struct{}](1, 0, func(any) uint64 { return 0 })
	invalid := []int{1}
	for _, operation := range []func(){
		func() { m.Put(invalid, 1) },
		func() { m.Get(invalid) },
		func() { m.Delete(invalid) },
	} {
		require.Panics(t, operation)
		s := m.ShardAt(0)
		require.True(t, s.TryLock(), "invalid key left the shard locked")
		s.Unlock()
	}
}

func TestShardedMapConcurrent(t *testing.T) {
	const workers, keys = 8, 256
	m := NewShardedMap[int, int, struct{}](6, 0, nil)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range keys {
				key := worker*keys + i
				m.Put(key, key)
				value, ok := m.Get(key)
				if !ok || value != key {
					t.Errorf("Get(%d) = (%d, %t)", key, value, ok)
				}
				if i%2 == 0 && !m.Delete(key) {
					t.Errorf("Delete(%d) returned false", key)
				}
				if i%32 == 0 {
					for key, value := range m.All() {
						if key != value {
							t.Errorf("iteration returned %d = %d", key, value)
						}
					}
				}
			}
		}()
	}
	wg.Wait()
	require.Equal(t, workers*keys/2, m.Len())
	require.Len(t, m.Collect(nil), workers*keys/2)
}

func ExampleShardedMap_All() {
	m := NewShardedMap[string, int, struct{}](4, 0, nil)
	m.Put("apple", 2)
	m.Put("pear", 3)
	copy := maps.Collect(m.All())
	fmt.Println(copy["apple"], copy["pear"])
	// Output: 2 3
}
