//go:build linux

package mempool_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/mempool"
)

func TestSlabPool_HugepageInfo(t *testing.T) {
	p, err := mempool.NewSlabPool(align.HugepageSize, align.BlockSize)
	require.NoError(t, err)
	defer p.Close()
	t.Logf("slab backing: %s", slabHugepageInfo(p))
}

// slabHugepageInfo reports the KernelPageSize for the slab's first page
// by scanning /proc/self/smaps. Returns a diagnostic string for t.Log.
func slabHugepageInfo(p *mempool.SlabPool) string {
	raw := p.RawData()
	if len(raw) == 0 {
		return "empty slab"
	}
	addr := uintptr(unsafe.Pointer(&raw[0]))

	data, err := os.ReadFile("/proc/self/smaps")
	if err != nil {
		return fmt.Sprintf("cannot read smaps: %v", err)
	}

	var inVMA bool
	for line := range strings.SplitSeq(string(data), "\n") {
		if !inVMA {
			var start, end uintptr
			if n, _ := fmt.Sscanf(line, "%x-%x", &start, &end); n == 2 && addr >= start && addr < end {
				inVMA = true
			}
			continue
		}
		if kps, ok := strings.CutPrefix(line, "KernelPageSize:"); ok {
			kps = strings.TrimSpace(kps)
			if strings.HasPrefix(kps, "2048") {
				return fmt.Sprintf("hugepage-backed (KernelPageSize: %s)", kps)
			}
			return fmt.Sprintf("standard pages (KernelPageSize: %s)", kps)
		}
		// New VMA header means we left our entry without finding KernelPageSize.
		if len(line) > 0 && line[0] >= '0' && line[0] <= '9' {
			break
		}
	}
	return fmt.Sprintf("KernelPageSize not found for addr %#x", addr)
}
