//go:build !allocstats

package alloc

// A compile-time switch removes accounting from the default acquisition path.
const allocationStatsEnabled = false
