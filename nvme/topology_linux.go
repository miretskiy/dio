//go:build linux

package nvme

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// Topology is a tree of block-device dependencies rooted at the inspected path.
// Each Backing entry is one device supplying storage to its parent. For example,
// a filesystem on a partition-mapped LVM volume can produce:
//
//	/dev/dm-3                  (volume1p1)
//	  Backing[0]: /dev/dm-1    (volume1)
//	    Backing[0]: /dev/nvme1n1, CharDevice: /dev/ng1n1
//
// A RAID device can have several Backing entries, one per member. Device-mapper
// sector tables and RAID layout rules determine how their address spaces map
// onto these dependencies. SizeBytes and StartBytes are expressed in bytes.
type Topology struct {
	Path       string     // Block-device path for this node, such as /dev/dm-3.
	SysPath    string     // Resolved sysfs path identifying this block device.
	SizeBytes  uint64     // Addressable bytes on this node.
	StartBytes uint64     // A real partition's byte offset in its parent; zero for other nodes.
	Model      string     // NVMe controller model, populated on namespace nodes.
	Serial     string     // NVMe controller serial, populated on namespace nodes.
	CharDevice string     // Generic namespace handle, populated when accessible.
	Backing    []Topology // Immediate storage dependencies, recursively described.
}

type deviceClass string

const (
	deviceBlock deviceClass = "block"
	deviceChar  deviceClass = "char"
)

// DiscoverTopology resolves a filesystem path, block device, or NVMe namespace
// character device into its block-device dependency tree. For example,
// DiscoverTopology("/instance_storage") traces its volume down to the physical
// NVMe namespace and fills in that node's CharDevice using its controller and
// namespace ID. This makes it useful before Open, while choosing a device.
// Resolution follows the calling process's device and mount namespaces.
func DiscoverTopology(path string) (Topology, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return Topology{}, err
	}
	dev := uint64(st.Dev)
	class := deviceBlock
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFBLK:
		dev = uint64(st.Rdev)
	case unix.S_IFCHR:
		dev = uint64(st.Rdev)
		class = deviceChar
	}
	sys, err := filepath.EvalSymlinks(fmt.Sprintf("/sys/dev/%s/%d:%d", class, unix.Major(dev), unix.Minor(dev)))
	if err != nil {
		return Topology{}, err
	}
	if class == deviceChar {
		sys, err = blockForCharacter(path, sys)
		if err != nil {
			return Topology{}, err
		}
	}
	return topologyAt(sys, make(map[string]bool))
}

func blockForCharacter(path, sys string) (string, error) {
	if _, err := os.Stat(filepath.Join("/sys/class/nvme-generic", filepath.Base(sys))); err != nil {
		return "", fmt.Errorf("nvme: expected namespace character device: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	id, err := namespaceID(f)
	if err != nil {
		return "", err
	}
	return blockForNamespace(sys, id)
}

func topologyAt(sys string, seen map[string]bool) (Topology, error) {
	canonical, err := filepath.EvalSymlinks(sys)
	if err != nil {
		return Topology{}, err
	}
	if seen[canonical] {
		return Topology{}, fmt.Errorf("nvme: cyclic block topology at %s", canonical)
	}
	seen[canonical] = true
	defer delete(seen, canonical)
	t := Topology{Path: "/dev/" + filepath.Base(canonical), SysPath: canonical}
	sectors, err := readUint(filepath.Join(canonical, "size"))
	if err != nil {
		return t, err
	}
	if sectors > ^uint64(0)/512 {
		return t, fmt.Errorf("nvme: block capacity overflow")
	}
	t.SizeBytes = sectors * 512
	if _, err := os.Stat(filepath.Join(canonical, "partition")); err == nil {
		start, err := readUint(filepath.Join(canonical, "start"))
		if err != nil {
			return t, err
		}
		if start > ^uint64(0)/512 {
			return t, fmt.Errorf("nvme: partition offset overflow")
		}
		t.StartBytes = start * 512
		parent, err := topologyAt(filepath.Dir(canonical), seen)
		if err != nil {
			return t, err
		}
		t.Backing = append(t.Backing, parent)
		return t, nil
	}
	slaves, err := os.ReadDir(filepath.Join(canonical, "slaves"))
	if err != nil {
		return t, err
	}
	for _, s := range slaves {
		child, err := topologyAt(filepath.Join(canonical, "slaves", s.Name()), seen)
		if err != nil {
			return t, err
		}
		t.Backing = append(t.Backing, child)
	}
	// Match sibling block and generic interfaces by controller and namespace ID.
	id, err := readUint(filepath.Join(canonical, "nsid"))
	if err == nil {
		ctrl := filepath.Dir(canonical)
		t.Model = readText(filepath.Join(ctrl, "model"))
		t.Serial = readText(filepath.Join(ctrl, "serial"))
		entries, err := os.ReadDir(ctrl)
		if err != nil {
			return t, err
		}
		for _, e := range entries {
			if _, err := os.Stat(filepath.Join("/sys/class/nvme-generic", e.Name())); err != nil {
				continue
			}
			f, err := os.Open("/dev/" + e.Name())
			if err != nil {
				continue
			}
			n, err := namespaceID(f)
			f.Close()
			if err == nil && uint64(n) == id {
				t.CharDevice = "/dev/" + e.Name()
				break
			}
		}
	}
	return t, nil
}
func readText(path string) string { b, _ := os.ReadFile(path); return strings.TrimSpace(string(b)) }
