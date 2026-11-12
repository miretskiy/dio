//go:build linux

// nvme-example inspects topology and demonstrates two bounded namespace regions.
// By default it only identifies the device. -read reads one block per region.
// -write-check overwrites those two blocks and verifies them; use a dedicated range.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/nvme"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	devicePath := flag.String("device", "", "generic namespace character device")
	topology := flag.String("topology", "", "inspect a device or mounted path without opening it")
	offset := flag.Uint64("offset", 0, "absolute byte offset of dedicated range")
	size := flag.Uint64("size", 0, "length of dedicated range in bytes")
	indexSize := flag.Uint64("index-size", 16<<20, "bytes reserved for index within range")
	read := flag.Bool("read", false, "read one block from each region")
	write := flag.Bool("write-check", false, "overwrite and verify one block in each region")
	flag.Parse()
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if *topology != "" {
		t, err := nvme.DiscoverTopology(*topology)
		if err != nil {
			return err
		}
		return enc.Encode(t)
	}
	if *devicePath == "" {
		return fmt.Errorf("provide -device or -topology")
	}
	device, err := nvme.Open(*devicePath)
	if err != nil {
		return err
	}
	defer device.Close()
	if err := enc.Encode(device.Info()); err != nil {
		return err
	}
	if !*read && !*write {
		return nil
	}
	explicitOffset, explicitSize := false, false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "offset" {
			explicitOffset = true
		}
		if f.Name == "size" {
			explicitSize = true
		}
	})
	if !explicitOffset || !explicitSize || *size <= *indexSize {
		return fmt.Errorf("I/O requires explicit -offset and -size, with size > index-size")
	}
	entire, err := nvme.Open(*devicePath, nvme.WithRange(*offset, *size))
	if err != nil {
		return err
	}
	defer entire.Close()
	index, err := nvme.Open(*devicePath, nvme.WithRange(entire.Offset(), *indexSize))
	if err != nil {
		return err
	}
	defer index.Close()
	data, err := nvme.Open(*devicePath, nvme.WithRange(entire.Offset()+index.Size(), entire.Size()-index.Size()))
	if err != nil {
		return err
	}
	defer data.Close()
	scheduler, err := iosched.NewURingScheduler(iosched.WithRingDepth(8), iosched.WithNVMeCommands())
	if err != nil {
		return err
	}
	defer scheduler.Close()
	for i, region := range []*nvme.Device{index, data} {
		expected := bytes.Repeat([]byte{byte(0xa5 + i)}, int(device.Info().BlockSize))
		if *write {
			if err := execute(scheduler, iosched.NVMeWriteOp(region, expected, 0)); err != nil {
				return err
			}
		}
		got := make([]byte, len(expected))
		if err := execute(scheduler, iosched.NVMeReadOp(region, got, 0)); err != nil {
			return err
		}
		if *write && !bytes.Equal(got, expected) {
			return fmt.Errorf("region %d: readback mismatch", i)
		}
		fmt.Printf("region %d at byte %d: success (%d bytes)\n", i, region.Offset(), len(got))
	}
	if *write {
		return execute(scheduler, iosched.NVMeFlushOp(entire))
	}
	return nil
}
func execute(scheduler iosched.Scheduler, op iosched.Op) error {
	ticket, err := scheduler.Submit(op)
	if err != nil {
		return err
	}
	_, err = ticket.Wait()
	return err
}
