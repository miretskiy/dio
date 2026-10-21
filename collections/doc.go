// Package collections provides slice-backed lists and a concurrent sharded map.
// List grows as needed; FixedList keeps values at stable addresses in a fixed
// number of reusable slots. Both lists use generation-checked handles.
// ShardedMap supports generic keys and caller-owned state under each shard lock.
package collections
