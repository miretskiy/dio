//go:build !linux

package nvme

import (
	"errors"
	"os"
)

// identify fails off Linux: namespaces are identified through Linux ioctls
// and sysfs.
func identify(*os.File) (Info, error) {
	return Info{}, errors.New("nvme: NVMe namespaces require Linux")
}
