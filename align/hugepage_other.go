//go:build !linux

package align

// HugepageSize is the conventional Linux huge page size (2 MiB).
// Provided on all platforms so that callers can round sizes consistently.
const HugepageSize = 2 << 20

// HugepageSupported reports whether the OS supports MAP_HUGETLB.
// Always false on non-Linux platforms.
const HugepageSupported = false

// hugePageMmapExtraFlags is 0 on non-Linux; no MAP_HUGETLB available.
const hugePageMmapExtraFlags = 0
