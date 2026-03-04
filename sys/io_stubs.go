//go:build !linux && !darwin

// Stub implementations for platforms that support neither O_DIRECT nor
// F_NOCACHE. All operations degrade gracefully: Fallocate and PunchHole
// are no-ops, Fdatasync falls back to f.Sync(), and CreateDirect/OpenDirect
// open without any bypass flags.
package sys

import "os"

const (
	UseFadvise           = false
	RequiresAlignment    = false
	RequiresExplicitSync = true
)

func Fdatasync(f *os.File) error          { return f.Sync() }
func Fallocate(f *os.File, _ int64) error { return nil }

func PunchHole(f *os.File, _, _ int64) (int64, error) { return 0, nil }

func Fadvise(f *os.File, _ Offset_t, _ int64, _ FadviseHint) error { return nil }

func CreateDirect(path string, _ OpenFlag) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
}

func OpenDirect(path string, _ OpenFlag) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY, 0)
}

func CopyFileRange(src, dst *os.File, srcOff, dstOff *int64, length int) (int, error) {
	return copyFileRangeEmulated(src, dst, srcOff, dstOff, length)
}

func PreadAligned(f *os.File, buf []byte, offset int64, _ OpenFlag) (int, error) {
	return f.ReadAt(buf, offset)
}

func (fl OpenFlag) OpenFlags() int { return 0 }

// copyFileRangeEmulated is duplicated here (also in io_darwin.go) so that
// each platform file is self-contained. The two files never compile together.
func copyFileRangeEmulated(src, dst *os.File, srcOff, dstOff *int64, length int) (int, error) {
	const maxChunk = 1 << 20
	buf := make([]byte, min(length, maxChunk))
	var total int
	for total < length {
		toRead := min(length-total, len(buf))
		n, err := src.ReadAt(buf[:toRead], *srcOff)
		if n > 0 {
			nw, werr := dst.WriteAt(buf[:n], *dstOff)
			*srcOff += int64(nw)
			*dstOff += int64(nw)
			total += nw
			if werr != nil {
				return total, werr
			}
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

