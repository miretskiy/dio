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

// adviseHugepages asks for transparent huge pages, which need no reserved
// pool. It is a hint: with THP disabled, the mapping keeps base pages.
func adviseHugepages(data []byte) { _ = unix.Madvise(data, unix.MADV_HUGEPAGE) }

// populate commits data's pages with one madvise call. It reports false when
// the kernel lacks MADV_POPULATE_WRITE (before Linux 5.14).
func populate(data []byte) bool { return unix.Madvise(data, unix.MADV_POPULATE_WRITE) == nil }
