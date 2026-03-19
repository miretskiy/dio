//go:build linux

package align

import "golang.org/x/sys/unix"

// HugepageSize is the size of a Linux transparent huge page (2 MiB).
const HugepageSize = 2 << 20

// HugepageSupported reports whether the OS supports MAP_HUGETLB.
const HugepageSupported = true

// hugePageMmapExtraFlags is OR'd into the mmap flags when allocating a
// hugepage-eligible region. On Linux this adds MAP_HUGETLB.
const hugePageMmapExtraFlags = unix.MAP_HUGETLB
