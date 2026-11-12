//go:build !linux

package nvme

// Device names an NVMe namespace handle in portable code.
// Linux supplies the device operations.
type Device struct{}
