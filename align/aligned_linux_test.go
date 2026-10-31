package align

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestWarmupCommitsPages(t *testing.T) {
	const pages = 16
	data := AllocAligned(pages * BlockSize)
	defer FreeAligned(data)
	Warmup(data, BlockSize)
	resident := make([]byte, pages)
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(unsafe.Pointer(&resident[0])))
	require.Zero(t, errno)
	for i, v := range resident {
		require.NotZero(t, v&1, "page %d is not resident", i)
	}
}

// A 2 MiB multiple gets huge pages: hugetlb when a pool is reserved, otherwise
// transparent huge pages, unless THP is disabled.
func TestAllocAlignedHugepages(t *testing.T) {
	thp, err := os.ReadFile("/sys/kernel/mm/transparent_hugepage/enabled")
	if err != nil || bytes.Contains(thp, []byte("[never]")) {
		t.Skip("transparent huge pages are disabled")
	}
	data := AllocAligned(4 * HugepageSize)
	defer FreeAligned(data)
	Warmup(data, BlockSize)
	pageKB, anonHugeKB := smapsEntry(t, data)
	require.True(t, pageKB == HugepageSize>>10 || anonHugeKB > 0,
		"KernelPageSize=%d kB AnonHugePages=%d kB", pageKB, anonHugeKB)
}

// smapsEntry reports the kernel page size and transparent huge page bytes of
// the mapping containing data.
func smapsEntry(t *testing.T, data []byte) (pageKB, anonHugeKB int) {
	t.Helper()
	smaps, err := os.ReadFile("/proc/self/smaps")
	require.NoError(t, err)
	start := fmt.Sprintf("%x-", uintptr(unsafe.Pointer(&data[0])))
	found := false
	for line := range strings.Lines(string(smaps)) {
		// Mapping headers start with an address range; fields start with "Key:".
		if f := strings.Fields(line); len(f) == 0 || !strings.HasSuffix(f[0], ":") {
			if found {
				break
			}
			found = strings.HasPrefix(line, start)
			continue
		}
		if !found {
			continue
		}
		var kb int
		if n, _ := fmt.Sscanf(line, "KernelPageSize: %d kB", &kb); n == 1 {
			pageKB = kb
		}
		if n, _ := fmt.Sscanf(line, "AnonHugePages: %d kB", &kb); n == 1 {
			anonHugeKB = kb
		}
	}
	require.True(t, found, "mapping not found in smaps")
	return pageKB, anonHugeKB
}
