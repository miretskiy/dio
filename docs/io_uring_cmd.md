# Using io_uring_cmd in dio

Dio can use one NVMe namespace character device as multiple bounded byte ranges.
`nvme.Device` opens the namespace, optionally bounded to a byte range with
`nvme.WithRange(base, length)`.
`iosched` schedules reads, writes, and flushes through its ringo backend and
returns completion tickets. Blobcache's record format, index,
allocation, and recovery remain above this interface.

In NVMe terminology, a **namespace** is a storage volume with its own identifier,
logical block format, and capacity. One controller can expose several volumes.
For example, `/dev/nvme0` is controller 0; `/dev/nvme0n1` and `/dev/ng0n1`
are its block and character interfaces to namespace ID 1. Controller numbering
starts at 0; [namespace ID 0 is reserved](https://nvmexpress.org/wp-content/uploads/NVM-Express-Base-Specification-2.0d-2024.01.11-Ratified.pdf#page=82). On the instance-store device
used here, `/dev/nvme1n1` and `/dev/ng1n1` expose namespace ID 1. See the
[NVMe namespace definition](https://nvmexpress.org/resource/nvme-namespaces/).

| Value | Contains | Application use |
| --- | --- | --- |
| `nvme.Device` | Open character-device handle, byte range, and discovered information | Pass to iosched operations; close after I/O completes |
| `nvme.Info` | Snapshot of identity, block format, capacity, and transfer limits | Choose block-aligned allocation sizes |
| `iosched.Op` | Device, buffer, relative byte offset, and operation flags | Submit and wait for its ticket |

The scheduler's NVMe operations use the Device's information to form
commands: namespace ID, starting LBA, zero-based block count, and buffer length.
The application supplies byte offsets and block-sized buffers. `Device.File()`
is available for direct ringo command submission.

The runnable example is [cmd/nvme-example](../cmd/nvme-example/main_linux.go).
Everything here is Linux-only. Start with Linux 5.19 or later; a particular device,
driver, and kernel configuration must also permit NVMe passthrough.

## 1. Find the right namespace

These three handles serve different purposes:

| Handle | Use |
| --- | --- |
| `/dev/nvme1` | Controller administration |
| `/dev/nvme1n1` | Namespace block interface; partitions and filesystems use this |
| `/dev/ng1n1` | Generic namespace character interface; NVMe io_uring_cmd uses this |

The block and character handles address the **same bytes**. Opening the character
device does not exclude a mounted filesystem, swap, LVM, or another process.

```sh
go build -o /tmp/nvme-example ./cmd/nvme-example
/tmp/nvme-example -topology /instance_storage
/tmp/nvme-example -device /dev/ng1n1
```

Configure device access as described in section 6 before opening the namespace.
The topology command walks sysfs dependencies, including partitions and
device-mapper slaves. It accepts a device, a device symlink, or a mounted path.
Matching a generic handle to a namespace uses `NVME_IOCTL_ID`, rather than
assuming the numbers in `/dev/ng*` and `/dev/nvme*` match. `CharDevice` can be empty
when node permissions prevent that query; run topology inspection with the needed
read permissions. A dependency graph does **not** translate a device-mapper
logical offset into a namespace offset. Striping, thin provisioning, encryption,
and RAID need their own mapping logic; dio does not guess.

`nvme.Open` validates the character device, obtains its namespace ID, and issues
Identify Namespace and Identify Controller, following fio's
`fio_nvme_get_info()`. It obtains the active LBA format, including extended format
index bits, rather than assuming 4 KiB blocks. The Device implementation
rejects zoned command sets and formats with active metadata or protection
information. The raw `nvme.Command` API exposes metadata for callers that implement
those protocols themselves.

`io_uring_cmd` itself has no universal transfer size or automatic splitting. On
the tested instance-store namespace, ordinary reads/writes use 512-byte logical
blocks and a discovered 128 KiB single-command limit. The 128-byte SQE contains
up to 80 inline command bytes, of which NVMe uses 72; the data buffer is supplied
separately. Raw commands follow their protocol's transfer units and limits.

`Info.MaxTransferBytes` caps one command using the Linux queue's hardware and
software sector limits, memory-segment capacity, the 65,536-block command limit,
and the command's 32-bit byte-length field. Linux has already accounted for the
controller's **MDTS** (Maximum Data Transfer Size) and minimum controller page size.
Multiplying MDTS by the host's page size is unreliable, particularly on ARM.

## 2. Turn byte ranges into LBAs

An **LBA**, Logical Block Address, is a logical block number in the namespace.
It describes the controller's address space, not a physical flash location.
The active block size can be 512 bytes even when your index pages are 4 KiB.

For an LVM-backed range, first obtain its absolute byte offset on the NVMe volume
from the device-mapper sector table. Pass that base and its byte length to
`nvme.Open(path, nvme.WithRange(base, length))`. Device command builders perform
the byte-to-LBA conversion using the discovered block size. An LBA is an address;
the Device represents an open handle and its bounded range.

For a region starting at byte `base`, a request at relative byte `offset` becomes:

```text
SLBA = (base + offset) / block_size
blocks = length / block_size
```

Both divisions must be exact. **SLBA** is the starting LBA. NVMe's **NLB** field
encodes the number of logical blocks **minus one**, so a 4 KiB command on a
512-byte namespace encodes eight blocks as `7`.

The scheduler validates the complete byte range at Submit, before issuing any
command. Offsets and buffer lengths must be block aligned. A zero-length request
at an aligned offset through `device.Size()` succeeds with a zero count. The
scheduler splits larger requests at `Info.MaxTransferBytes`, using bounded batches
that also fit the ring. Each caller operation produces one result, regardless of
its command count. A successful transfer reports the full byte count. On error,
the result reports the contiguous successful prefix; other commands already in
flight are drained, and writes beyond that prefix may have reached storage.

Range bounds apply to typed Device command builders and iosched NVMe operations.
`Device.File()` and the raw Command API give callers direct namespace access.
The application remains responsible for reserving its range against other users.

## 3. Make an index region and a data region

The following fragment assumes `base` and `length` describe an exclusively owned
physical range in the identified namespace. For a whole dedicated namespace use
`base = 0`, `length = device.Info().SizeBytes`.

```go
indexBytes := uint64(16 << 30)
if length <= indexBytes { return fmt.Errorf("range too small") }

// Validate the complete allocation before computing its subdivisions.
whole, err := nvme.Open("/dev/ng1n1", nvme.WithRange(base, length))
if err != nil { return err }
defer whole.Close()
index, err := nvme.Open("/dev/ng1n1", nvme.WithRange(base, indexBytes))
if err != nil { return err }
defer index.Close()
data, err := nvme.Open("/dev/ng1n1", nvme.WithRange(base+indexBytes, length-indexBytes))
if err != nil { return err }
defer data.Close() // After outstanding operations complete.
```

Each bounded Device owns its handle and enforces its own byte range. `nvme.Open`
without `WithRange` exposes the whole namespace. `Device.Offset()` and `Size()`
describe the handle's range; `Info.SizeBytes` describes the whole namespace.
A partition table, `mkfs`, or namespace Format command is unnecessary. When
sharing with a filesystem, reserve the raw range outside its backing extents.

## 4. Submit through iosched

```go
scheduler, err := iosched.NewURingScheduler(
    iosched.WithRingDepth(64),
    iosched.WithNVMeCommands(),
)
if err != nil { return err }
defer scheduler.Close()

buffer := make([]byte, 4096) // Fill the data before submitting a write.
ticket, err := scheduler.Submit(iosched.NVMeWriteOp(data, buffer, 0).Durable())
if err != nil { return err }
_, err = ticket.Wait()
return err
```

`NVMeReadOp(data, buffer, offset)` reads a device-relative byte offset.
`NVMeWriteOp(...).Durable()` sets FUA on every command of that write. `NVMeFlushOp(data)` flushes
the entire namespace containing the Device. Successful read/write tickets return
`len(buffer)`; flush tickets return zero. Tickets report both Linux and NVMe errors.
The scheduler uses its existing admission accounting, coordinator, and ringo
operation pool for these commands. Submit several operations before waiting for
concurrent I/O. Keep Devices open and buffers valid until their tickets complete.

`Link` and `HardLink` accept NVMe operations and use native kernel completion
semantics. Linux 6.8's [command completion handler](https://github.com/torvalds/linux/blob/v6.8/io_uring/uring_cmd.c)
marks a link failure for a negative Linux return value. Positive NVMe device
statuses are exposed as ticket errors but permit soft-link successors to run.
Hard links continue after either kind of error. The hardware test demonstrates
both link kinds with an out-of-range command followed by a Nop.

For a split request, iosched waits for the complete logical operation before
starting a linked successor. Negative Linux errors break a soft chain; positive
NVMe statuses allow continuation, consistent with the kernel's command path.
For status-dependent data-before-index publication, check the data ticket before
submitting the index write. The POSIX backend reports EOPNOTSUPP for NVMe ops.

### Raw NVMe commands through iosched

`iosched.RawNVMeCmdOp(file, request, &completion)` submits a caller-built NVMe command as a
`ringo.Op` through the scheduler. It retains the target file, participates in `DrainOp`, and
returns a ticket. A successful Submit transfers the request permanently: discard
all aliases of the ringo request and its RawNVMeCmdOp wrapper. A rejected Submit leaves
the request with the caller for retry.

For example, submit one namespace command directly:

```go
command, err := data.ReadCommand(buffer, 0)
if err != nil { return err }
request, err := command.Op(ringo.FileFD(data.File()))
if err != nil { return err }
var completion ringo.Completion
ticket, err := scheduler.Submit(iosched.RawNVMeCmdOp(data.File(), request, &completion))
if err != nil { return err }
if _, err = ticket.Wait(); err != nil { return err }
return nvme.Status(completion)
```

The completion destination may be nil. When supplied, it receives the final
CQE, including command-specific `Extra` fields, before the ticket completes.
Raw ticket results preserve driver results: a successful NVMe read returns zero;
a positive NVMe error status remains positive and can be decoded with
`nvme.Status(completion)`. Link and HardLink retain native kernel semantics.
The ring consumes intermediate multishot completions and the ticket completes
on the final one.

Raw commands are submitted intact with caller-defined addressing and transfer
sizes. The caller supplies command-specific chunking and bounds. DiskPolicy
models typed read/write operations; RawNVMeCmdOp uses ring capacity and file barriers.
The POSIX scheduler reports EOPNOTSUPP for RawNVMeCmdOp. A plain `os.File` can target
NVMe namespaces with other command sets, including zoned namespaces, while
`nvme.Device` supplies the ordinary NVM byte-range helpers.

`nvme.Command.AdminOp` targets a controller handle such as `/dev/nvme1`. This
allows read-only Identify and Get Log Page commands, including AWS's performance
statistics page:

```go
controller, err := os.Open("/dev/nvme1")
if err != nil { return err }
defer controller.Close()
stats := make([]byte, 4096)
command := nvme.Command{
    Opcode: nvme.OpcodeAdminGetLogPage,
    NSID: 1,
    CDW10: ((4096/4 - 1) << 16) | 0xd0,
    Data: stats,
}
request, err := command.AdminOp(ringo.FileFD(controller))
if err != nil { return err }
var completion ringo.Completion
ticket, err := scheduler.Submit(iosched.RawNVMeCmdOp(controller, request, &completion))
if err != nil { return err }
if _, err = ticket.Wait(); err != nil { return err }
if err := nvme.Status(completion); err != nil { return err }
// Decode stats using the AWS log-page layout.
```

Get Log Page counts its transfer in 32-bit words, hence `4096/4 - 1` in CDW10.
Its page ID is `0xd0`. The [Amazon nvme-cli plugin](https://github.com/linux-nvme/nvme-cli/blob/master/plugins/amzn/amzn-nvme.c)
defines this payload; [AWS documents the counters and histograms](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/nvme-detailed-performance-stats.html).
The controller node needs its own read permission; namespace-node access applies
to that namespace node. The device/driver still enforces command privileges.

### Direct ringo submission

For explicit ring control, prepare one command from the bounded Device, then build a
low-level ringo operation:

```go
ring, err := ringo.New(ringo.WithDepth(64), ringo.WithUringCmd())
if err != nil { return err }
defer ring.Close()

command, err := data.ReadCommand(buffer, 0)
if err != nil { return err }
op, err := command.Op(ringo.FileFD(data.File()))
if err != nil { return err }
handle, err := ring.Push(op)
if err != nil { return err }
for {
    if _, err := ring.SubmitAndWait(1); err != nil { return err }
    done := false
    var result error
    for c := range ring.Reap() {
        if c.Handle == handle {
            result, done = nvme.Status(c), true
        }
    }
    if done { return result }
}
```

Use `ringo.WithAlloc(&alloc)` with `command.Op` to recycle operation objects.
Direct ring callers serialize submission and reaping, match handles to requests,
and handle submission errors. Under resource pressure, EAGAIN/EBUSY calls for
reaping and retrying; pushed operations remain owned by the ring until completion.

`iosched.WithNVMeCommands()` enables the same extended entry layout as
`ringo.WithUringCmd()`: 128-byte **SQEs** (Submission Queue Entries) and 32-byte
**CQEs** (Completion Queue Entries). Use this option on schedulers or rings that
submit device commands. File-only rings keep the default 64/16-byte entries;
ordinary file operations can share an extended ring with device commands.
At the command level there are three selectors:

| Field | Value | Purpose |
| --- | --- | --- |
| SQE opcode | `IORING_OP_URING_CMD` | Ask the driver to execute a command |
| SQE cmd_op | `NVME_URING_CMD_IO` | Non-vectored NVMe namespace command |
| NVMe opcode | `0x01` / `0x02` | Write / read |

The 72-byte `nvme_uring_cmd` payload begins at SQE byte 48. It specifies the
namespace ID, buffer addresses and lengths, and **CDWs** (Command Dwords):
`CDW10/11` contain the low/high halves of SLBA, and `CDW12` bits 15:0 contain NLB.
Linux's ioctl/uring payload fields use host byte order; Identify response fields
use little endian. These are separate encodings.

Submission progress and device success are separate results:

| Completion | Interpretation |
| --- | --- |
| `c.Err != nil` | Linux error, from a negative CQE result |
| `c.Result > 0` | NVMe error status; `nvme.Status` returns a `StatusError` |
| `c.Result == 0`, `c.Err == nil` | Successful command, including reads/writes |
| `c.Extra[0]` | Command-specific result, for commands defining one |

**At the raw ringo level, a successful 4 KiB transfer returns zero.** Check it
with `nvme.Status`. The iosched adapter translates that into a 4096-byte ticket
result and decodes device errors for you.
Do not use a positive device status as a transferred byte count. The generic ring
cannot infer device-specific success; for dependent NVMe commands, inspect each
completion before submitting its successor instead of assuming a soft link will
interpret positive status correctly.

Ringo retains typed buffer references and the file through completion, encoding
addresses during Push after their backing storage escapes to the heap. A
`KeepAlive` immediately after submission would not be enough. Do not modify write
buffers, access read buffers, close descriptors, or release/unmap allocator
buffers while their operations are pending. Normal commands do not require
`O_DIRECT` or registered buffers. The initial NVMe helpers use ordinary data buffers; registered-file descriptors
remain available through the raw `Command.Op` API.

For a nonstandard command, construct `nvme.Command` with explicit CDWs and
`Data`/`Metadata`, then call `.Op(fd)`. The caller owns capability checks and
command semantics. For another driver's protocol, `ringo.UringCmd` copies up to
80 payload bytes and binds address slots using `ringo.CommandBuffer`:

| Argument | Responsibility |
| --- | --- |
| `fd` | Target character-device handle |
| `driverOp` | Select the driver interface, such as `nvme.UringCmdIO` |
| `payload` | Encoded command parameters, including the NVMe opcode |
| `buffers` | Connect payload address slots to retained Go buffers |

For NVMe, `{Offset: 24, Buffer: data}` writes data's address into payload bytes
`[24, 32)` during Push. Each descriptor selects an eight-byte slot; these slots
must fit inside the payload and be disjoint. The data buffer holds the actual
transferred bytes. Command dwords 4-5 and 6-9 hold metadata/data pointers; Linux
maps the supplied buffers into those fields. CDW10 and later carry parameters
such as the starting block address and count.

## 5. Arbitrary-size Put/Get belongs above this layer

Use block-aligned index pages and allocate each record in whole blocks initially.
A record can store its key identity, generation, actual value length, and checksum;
initialize padding before writing and return only the actual value on Get.
An allocator-backed aligned buffer is convenient, but memory alignment and
on-disk block alignment are different requirements.

Submit a large block-aligned record as one iosched operation. The scheduler
splits it at `Info.MaxTransferBytes` and completes its ticket after all commands
finish. Direct ringo callers perform their own splitting and completion
aggregation. Check the complete record write before exposing the record; a
failed command supplies a status, and iosched reports the successful prefix
across commands.

For Put: allocate a fresh extent, write its data, check all completions, then
update/publish its index page. Serialize updates sharing an index page. Keep old
extents allocated until readers and outstanding commands have released them.
For Get: resolve the index entry, read its blocks, validate identity, generation,
length, and checksum, then return the requested bytes. Validation failure can
become a cache miss.

`iosched.NVMeFlushOp(data)` requests an NVMe namespace-wide flush.
`iosched.NVMeWriteOp(...).Durable()` sets **FUA** (Force Unit Access) for that
write. Direct ring users can request the same through `Device.FlushCommand()`
and `Device.WriteCommand(..., true)`. Neither makes data and index
updates one transaction. A cache discarded on process restart can avoid a durable
publication protocol. A persistent cache needs device-appropriate durability
ordering plus a recoverable index update scheme, such as copy-on-write pages or
a log. Apply atomic-write guarantees only when discovered for the target
namespace, including its alignment and boundary requirements.

**PI** (Protection Information) is per-block integrity metadata, potentially
including guard, application, and reference tags. It depends on the active
namespace format; it is not enabled by setting a checksum flag on an ordinary
metadata-free namespace. Keep application record checksums even with PI.
**FDP** (Flexible Data Placement) optionally groups writes by expected lifetime
on supporting drives. It neither partitions address space nor implements an
index; this integration leaves placement disabled. Device commands execute
operations implemented by the controller. They do not imply programmable
compute on the drive.

## 6. Permissions and experiments

Opening `/dev/ng*`, issuing Identify, and submitting namespace commands can have
different permission checks. Device-node ACLs alone may not permit commands:
the installed NVMe driver also evaluates command privilege, including command
effects support. Test the actual kernel/device rather than assuming that
changing the node's owner permits unprivileged passthrough.

Grant your user access to the current character-device node:

```sh
sudo setfacl -m "u:$(id -un):rw" /dev/ng1n1
```

`setfacl` and `getfacl` are provided by the `acl` package on Ubuntu. These ACLs
apply to the current nodes. For persistent service access, use a serial-matched
host udev rule and arrange permissions in the application's device namespace too;
containers can have their own device nodes.

If submission still requires `CAP_SYS_ADMIN`, grant that capability to the built
application, then launch it as your normal user:

```sh
sudo setcap cap_sys_admin=ep /absolute/path/to/your-program
getcap /absolute/path/to/your-program
/absolute/path/to/your-program
```

This runs with a non-root user ID but an elevated capability, not with zero
privileges. `CAP_SYS_ADMIN` is broad. Reapply file capabilities after rebuilding
or replacing the executable; `go run` does not use capabilities on a separately
built binary. A service can instead receive the capability through systemd.

Read-only hardware validation of the scheduled path as a non-root user (apply
the capability only when the driver requires it):

```sh
go test ./...
go test -c -o /tmp/iosched.test ./iosched
sudo setcap cap_sys_admin=ep /tmp/iosched.test
DIO_NVME_DEVICE=/dev/ng1n1 DIO_NVME_REQUIRE_NONROOT=1 \
  /tmp/iosched.test -test.run '^TestNVMeHardware$' -test.v
```

This checks NVMe ticket results, bounds, links, and ordinary file operations on
the same ring. The test logs its user IDs and capability masks;
`DIO_NVME_REQUIRE_NONROOT=1` rejects a root test process. For direct ringo command
validation, compile `./nvme` instead and run `TestNamespaceHardware`; that test
also needs a read ACL on the block device for its block/character comparison:

```sh
go test -c -o /tmp/nvme.test ./nvme
sudo setfacl -m "u:$(id -un):r" /dev/nvme1n1
sudo setcap cap_sys_admin=ep /tmp/nvme.test
DIO_NVME_DEVICE=/dev/ng1n1 DIO_NVME_REQUIRE_NONROOT=1 \
  /tmp/nvme.test -test.run '^TestNamespaceHardware$' -test.v
```

Both tests opt into writes with `DIO_NVME_WRITE=1`, `DIO_NVME_OFFSET`, and
`DIO_NVME_SIZE`, naming an exclusively owned raw range. The example's
`-write-check` requires explicit `-offset` and `-size`; it overwrites the first
block of both regions.

Sources: [fio geometry and encoding](https://github.com/axboe/fio/blob/master/engines/nvme.c),
[fio engine transport](https://github.com/axboe/fio/blob/master/engines/io_uring.c),
[fio FDP range example](https://github.com/axboe/fio/blob/master/examples/uring-cmd-fdp.fio),
[Linux NVMe userspace interface](https://github.com/torvalds/linux/blob/master/include/uapi/linux/nvme_ioctl.h),
[NVMe command fields and opcodes](https://github.com/torvalds/linux/blob/v6.8/include/linux/nvme.h),
[Linux NVMe passthrough permissions](https://github.com/torvalds/linux/blob/master/drivers/nvme/host/ioctl.c),
and the [LPC passthrough presentation](https://lpc.events/event/16/contributions/1382/attachments/1119/2151/LPC2022_uring-passthru.pdf).
The last is background reading; the runnable code and tests define dio's API.
