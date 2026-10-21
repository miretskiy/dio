package collections

import (
	"hash/maphash"
	"iter"
	"slices"
	"sync"
)

// ShardedMap is a concurrent map with caller-owned state in each shard.
// Use NewShardedMap or NewShardedMapWithExtra to initialize it.
// A ShardedMap must not be copied.
//
// Get, Put, and Delete lock one shard. Callers may instead lock a Shard to
// update several entries and its Extra state together. Map methods do not
// modify Extra or manage resources owned by keys, values, or Extra.
//
// Len, iteration, Collect, and Clear visit one shard at a time; they are not
// atomic operations on the whole map. Iteration order is unspecified.
type ShardedMap[K comparable, V, E any] struct {
	shards []MapShard[K, V, E]
	hash   func(K) uint64
}

// MapShard holds entries and caller-owned state under the same lock.
// Access Items and Extra while holding the appropriate read or write lock.
// A MapShard must not be copied after first use.
//
// E owns the shard layout, including any padding. The VerifyAlignment helper
// in alignment_test.go can be copied into caller tests to check the shard size
// and compiler-inserted trailing padding in E. Constructors accept any layout;
// insufficient padding may cause false sharing between shards.
type MapShard[K comparable, V, E any] struct {
	sync.RWMutex
	Items map[K]V
	Extra E
}

// Pad32 supplies padding when no per-shard state is needed. On 64-bit targets,
// the lock and map occupy 32 bytes, so MapShard[K, V, Pad32] occupies 64 bytes.
type Pad32 [32]byte

// ShardedMapOption configures a map independently of its value and state types.
type ShardedMapOption[K comparable] func(*shardedMapOptions[K])

type shardedMapOptions[K comparable] struct {
	shardCapacity int
	hash          func(K) uint64
}

// WithShardCapacity sets the initial capacity hint for each shard's map.
// The default is zero. A negative capacity causes construction to panic.
func WithShardCapacity[K comparable](capacity int) ShardedMapOption[K] {
	return func(o *shardedMapOptions[K]) { o.shardCapacity = capacity }
}

// WithHasher selects the hash used to choose a shard. A nil hash uses
// maphash.Comparable with a seed unique to the map, which is also the default.
// A supplied hash must be safe for concurrent calls, return the same hash for
// equal keys throughout the map's lifetime, and distribute keys across its
// output bits. Already-hashed keys can supply their existing hash directly.
// The underlying maps still compare complete keys.
func WithHasher[K comparable](hash func(K) uint64) ShardedMapOption[K] {
	return func(o *shardedMapOptions[K]) { o.hash = hash }
}

// NewShardedMap creates a map with exactly shards shards. It uses Pad32 for a
// 64-byte shard on 64-bit targets. Use NewShardedMapWithExtra for caller-owned
// per-shard state. The shard count must be positive, but need not be a power
// of two. Options set the per-shard capacity hint and hash function.
func NewShardedMap[K comparable, V any](shards int, opts ...ShardedMapOption[K]) *ShardedMap[K, V, Pad32] {
	return NewShardedMapWithExtra[K, V](shards, Pad32{}, opts...)
}

// NewShardedMapWithExtra is NewShardedMap with caller-owned state and padding.
// It copies extra into each shard. Any referenced objects remain shared;
// initialize those separately through ShardAt if each shard needs its own.
// No padding is added to E, and its layout is not checked at construction.
func NewShardedMapWithExtra[K comparable, V, E any](shards int, extra E, opts ...ShardedMapOption[K]) *ShardedMap[K, V, E] {
	if shards <= 0 {
		panic("collections: shard count must be positive")
	}
	var o shardedMapOptions[K]
	for _, opt := range opts {
		opt(&o)
	}
	if o.shardCapacity < 0 {
		panic("collections: shard capacity must be nonnegative")
	}
	if o.hash == nil {
		seed := maphash.MakeSeed()
		o.hash = func(key K) uint64 { return maphash.Comparable(seed, key) }
	}
	m := &ShardedMap[K, V, E]{
		shards: make([]MapShard[K, V, E], shards),
		hash:   o.hash,
	}
	for i := range m.shards {
		m.shards[i].Items = make(map[K]V, o.shardCapacity)
		m.shards[i].Extra = extra
	}
	return m
}

// Shard returns the shard for key. The caller is responsible for locking it.
func (m *ShardedMap[K, V, E]) Shard(key K) *MapShard[K, V, E] {
	index := m.hash(key)
	count := uint64(len(m.shards))
	if count&(count-1) == 0 {
		index &= count - 1
	} else {
		index %= count
	}
	return &m.shards[index]
}

// ShardCount returns the number of shards.
func (m *ShardedMap[K, V, E]) ShardCount() int { return len(m.shards) }

// ShardAt returns the shard at index. The caller is responsible for locking it.
// It panics if index is outside [0, ShardCount()).
func (m *ShardedMap[K, V, E]) ShardAt(index int) *MapShard[K, V, E] {
	return &m.shards[index]
}

// Put inserts or replaces the value for key.
func (m *ShardedMap[K, V, E]) Put(key K, value V) {
	s := m.Shard(key)
	s.Lock()
	defer s.Unlock()
	s.Items[key] = value
}

// Get returns the value for key and whether it exists.
func (m *ShardedMap[K, V, E]) Get(key K) (V, bool) {
	s := m.Shard(key)
	s.RLock()
	defer s.RUnlock()
	value, ok := s.Items[key]
	return value, ok
}

// Delete removes key and reports whether it existed.
func (m *ShardedMap[K, V, E]) Delete(key K) bool {
	s := m.Shard(key)
	s.Lock()
	defer s.Unlock()
	_, existed := s.Items[key]
	delete(s.Items, key)
	return existed
}

// ForEach visits entries with read-only access to their shard's Extra state.
// Returning false stops iteration. The callback runs under the shard's read
// lock: it must not mutate Items or Extra, reenter this map, or use the Extra
// pointer outside the callback. Locks are released on return or panic.
func (m *ShardedMap[K, V, E]) ForEach(visit func(K, V, *E) bool) {
	for i := range m.shards {
		if !m.shards[i].forEach(visit) {
			return
		}
	}
}

func (s *MapShard[K, V, E]) forEach(visit func(K, V, *E) bool) bool {
	s.RLock()
	defer s.RUnlock()
	for key, value := range s.Items {
		if !visit(key, value, &s.Extra) {
			return false
		}
	}
	return true
}

// All returns an iterator over key-value pairs. The loop body runs under a
// shard's read lock and must follow the same locking restrictions as ForEach.
func (m *ShardedMap[K, V, E]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		m.ForEach(func(key K, value V, _ *E) bool { return yield(key, value) })
	}
}

// Keys returns an iterator over keys, with the same locking restrictions as All.
func (m *ShardedMap[K, V, E]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		for key := range m.All() {
			if !yield(key) {
				return
			}
		}
	}
}

// Values returns an iterator over values, with the same locking restrictions as All.
func (m *ShardedMap[K, V, E]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		for _, value := range m.All() {
			if !yield(value) {
				return
			}
		}
	}
}

// Len sums the number of entries across shards. Concurrent updates may affect
// the result while the shards are visited.
func (m *ShardedMap[K, V, E]) Len() int {
	count := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.RLock()
		count += len(s.Items)
		s.RUnlock()
	}
	return count
}

// Collect appends all values to dst and returns the extended slice.
func (m *ShardedMap[K, V, E]) Collect(dst []V) []V {
	return slices.AppendSeq(dst, m.Values())
}

// Clear removes entries from each shard and leaves Extra unchanged. It does
// not prevent concurrent inserts into shards already cleared. Callers own any
// resource cleanup their entries need.
func (m *ShardedMap[K, V, E]) Clear() {
	for i := range m.shards {
		s := &m.shards[i]
		s.Lock()
		clear(s.Items)
		s.Unlock()
	}
}
