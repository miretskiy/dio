# Resize instance storage on a workspace for raw NVMe testing

This is the procedure used on 12 November 2026 to change an m7gd.8xlarge workspace from a large instance-store filesystem into 128 GiB of swap, a 256 GiB XFS filesystem, and roughly 1.35 TiB reserved for raw NVMe command I/O. The retained files were backed up, restored, and verified. The same workspace container was restarted; the host was not rebooted.

Use the sequence for a new workspace, but rediscover its devices, volume names, serials, and physical ranges. The example offsets near the end belong to the completed migration and must not be copied to another instance.

## Discover the actual storage layout

Run the storage operations below in the host namespace as root. First collect:

```sh
hostname
lsblk -o NAME,PATH,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL,SERIAL
nvme list
findmnt -T /instance_storage
findmnt -T /home/bits/.cache
swapon --show
pvs
vgs
lvs --segments -o lv_name,vg_name,lv_size,segtype,devices
dmsetup ls --tree
dmsetup table
```

Identify the physical instance-store NVMe device by model and serial. In `nvme list`, the `Node` and `Generic` columns pair the block interface with its character interface. Trace the filesystem's device-mapper dependencies with `dmsetup ls --tree`, then inspect their physical ranges with `dmsetup table`. A workspace container's `lsblk` output can omit the mapper layers even when `findmnt` reports the mounted filesystem. Do not guess the character-device name from the block-device number.

On the completed instance, the 400 GiB EBS device backed the host/root storage. A separate 1,900,000,000,000-byte instance-store namespace backed the LVM group `vg_ws_ephemeral`. Its `volume0` was swap; `volume1`, through a GPT partition mapping, contained XFS.

XFS cannot shrink in place. Reducing the LV underneath it would truncate its address space. This procedure therefore recreates the data LV/filesystem and restores its contents.

## Establish a host session before stopping the workspace

Port 22 on this setup enters the workspace container as `bits`; port 21 enters the host as `ubuntu`. Sudo inside the container does not change its PID or mount namespace. In particular, `nsenter -t 1 -m` there still refers to the container's PID 1.

The workspaces source provides a host administration helper:

```sh
/path/to/dd-source/domains/devex/workspaces/scripts/admin-ws-host.sh \
  -e prod -r YOUR_REGION -i YOUR_INSTANCE_ID
```

It uses the appropriate AWS credentials and EC2 Instance Connect. Validate the host identity and sudo access before beginning. Keep this session independent of container SSH so it survives the container shutdown.

The `workspaces` CLI manages lifecycle and authentication, but had no instance-store repartitioning command. `expand-storage` operates on EBS and does not shrink the instance-store XFS filesystem.

In this migration, SSO authentication succeeded but the account had no usable workspaces AWS role. The requested temporary AWS admin membership was denied, so no broader AWS permissions were granted. Host access instead used the explicitly authorized, temporary public-key procedure described at the end.

## Inspect the installed boot provisioning

Read the installed host files, including:

- `/usr/local/lib/workspaces/setup_instance_storage.sh`

- `dd.instance-storage.service`

- `dd.workspace-container.service`

- `/etc/fstab`

The installed setup script owned LVM creation, GPT partition mappings, filesystem mounting, swap activation, and the cache bind mount. Editing fstab alone would not have changed this layout.

The installed script used XFS even though the newer workspaces source available locally used btrfs. Preserve the installed script's filesystem checks, readiness behavior, and recovery logic. Save its original contents and a checksum before editing it.

Confirm the required tools are available: LVM, `dmsetup`, `kpartx`, `parted`, `mkfs.xfs`, GNU `cp`, and Python 3. The host used here did not have rsync.

## Stage a backup on persistent EBS

Choose which data to retain before changing the filesystem. For the completed migration, three old test directories totaling about 928 GiB were explicitly discarded; all other entries were retained.

The retained files occupied about 10 GiB, but their logical sizes totaled about 411 GiB because many were sparse. Compare both measurements:

```sh
du -sx -B1 /instance_storage
du --apparent-size -sx -B1 /instance_storage
```

Apply the chosen exclusions to both commands when sizing the retained set. Confirm the backup filesystem is on EBS, has adequate free space, and is different from the source filesystem. The shared `/home/bits/go` path was on persistent EBS on this instance. The container's `/tmp` was tmpfs and was unsuitable for a backup or binaries needed after restarting the container.

Stage retained entries with:

```sh
cp -a --sparse=always --reflink=auto -- RETAINED_ENTRIES BACKUP_DIRECTORY/
```

Pass the retained top-level entries in one invocation so hard links between them can be preserved. Archive mode preserves ownership, modes, links, and extended metadata. Sparse copying avoids materializing holes. Cross-filesystem copying falls back from reflinking to a regular sparse copy.

An initial `rsync -aHAXS` attempt spent time scanning logical holes; sparse-aware `cp` was more suitable here. A later comparison checked metadata. The final migration helper also removed obsolete entries only inside the task's backup destination before refreshing it.

This initial copy is staging. Take a final consistent copy after stopping all users of the filesystem.

## Stop the services and release every mount

Record the existing container ID, its mounts, and which host services are active. On this image the workspace container was identified by:

```sh
docker ps -q --filter label=com.amazonaws.ecs.container-name=workspace-shell
```

Require exactly the intended container. Record its identity so the same container can be restarted.

The migration stopped `dd.taskapi`, `ecs`, and `dd.bb-clientd`, then stopped the workspace container. Stopping the control services prevented recreation during maintenance. Stopping Buildbarn released its cache and FUSE users; its cache includes a Unix socket and daemon state. Its shutdown reached the stop timeout on this host, and its failed state was reset before it was restarted later.

```sh
systemctl stop dd.taskapi ecs dd.bb-clientd
docker stop -t 30 "$workspace_container"
umount /home/bits/.cache
umount /instance_storage
```

Inspect all other mounts and users on the particular image. A container-only unmount is insufficient when the host or another namespace still owns a mount. Use normal unmounts; if a target is busy, release its users rather than using a lazy or forced unmount.

Confirm the data partition mapping has no remaining open users. Keep the swap LV active and separate from every operation on the data LV.

## Verify a final consistent backup

With the services and workspace stopped, mount the old data partition privately, refresh the retained-data backup, verify it, and normally unmount it again. The migration used `/mnt/dio-old-instance-storage` for this temporary mount.

Verification checked file types, logical sizes, symlink targets, hard links, and file contents. To avoid reading hundreds of gigabytes of holes, the verifier used `SEEK_DATA` and `SEEK_HOLE` and compared the union of source and destination data extents. A hole on both sides needs no read; an extent on either side is compared on both.

Record the successful backup checkpoint on EBS before removing the old LV. The final copy here contained 10,533 regular files. Save the LVM metadata as well:

```sh
vgcfgbackup -f "$maintenance_directory/vg-before.conf" "$vg"
```

The automation used persistent phases: `initial`, `backed_up`, `formatted`, `restored`, and `complete`. After an interrupted migration, do not overwrite the verified backup with a partially restored new filesystem.

## Recreate the data LV and restore XFS

The following names are examples from the completed single-PV layout. Set them from the discovered layout and the installed provisioner's expected labels.

```sh
set -euo pipefail
: "${pv:?Set pv to the verified instance-store namespace block device}"
test -b "$pv"
vg=vg_ws_ephemeral
data_lv=/dev/$vg/volume1
data_partition=/dev/mapper/vg_ws_ephemeral-volume1p1
# pv must be the verified instance-store namespace block device.

kpartx -d -s "$data_lv"
udevadm settle --timeout=30
lvremove -y "$data_lv"

lvcreate -L 256G --alloc contiguous -n volume1 "$vg" "$pv"
parted -s "$data_lv" mklabel gpt
parted -s "$data_lv" mkpart userdata_1 xfs 1MiB 100%
kpartx -a -s "$data_lv"
udevadm settle --timeout=30
mkfs.xfs -f -L userdata_1 "$data_partition"
```

These are the destructive steps. They apply only after the verified backup and normal unmounts. Remove the data LV, not the VG, PV, root volume, or swap LV. Do not force removal of an in-use device.

The explicit XFS format is necessary: reused blocks may still contain the old filesystem signature, and the old filesystem geometry is too large for the replacement LV.

Mount the new partition privately, restore the retained tree with sparse files and metadata preserved, and run the same content verification again. Flush the restored filesystem and unmount the private mount before proceeding.

## Reserve the remaining physical extents

Allocate the remaining free extents to an unformatted LV:

```sh
lvcreate -l 100%FREE --alloc contiguous -n dio_raw "$vg" "$pv"
dmsetup table "/dev/$vg/dio_raw"
```

Do not partition, format, mount, or activate swap on `dio_raw`. Its purpose is to reserve extents against future LVM allocation. The application opens the NVMe character device and uses the raw LV's physical address interval; it does not submit command I/O through the LV handle.

For the simple case, require exactly one row:

```text
0 SECTOR_COUNT linear BACKING_MAJOR:MINOR PHYSICAL_START_SECTOR
```

Verify the backing major/minor identifies the intended physical NVMe namespace. Then calculate:

```text
base_bytes   = PHYSICAL_START_SECTOR * 512
length_bytes = SECTOR_COUNT * 512
```

Device-mapper sectors are 512 bytes regardless of the namespace's logical block size. Confirm both byte values align to the discovered NVMe block size and fit inside its capacity. Compare the raw interval against the entire swap and filesystem LV intervals, including their partition metadata; they must not overlap.

A list of slaves alone is not an address translation. Multiple rows, striped/RAID/thin/crypt mappings, or multiple backing namespaces need explicit mapping logic. This procedure was validated for a single physical namespace with contiguous linear LVs.

## Make the layout survive provisioning

Changing only the existing LVM layout is insufficient if the setup script would recreate a full-disk filesystem after instance-store loss. Changing only the script is also insufficient for an existing oversized LV: the installed script skipped LV creation when the LV already existed.

The installed script was changed in two places. First, the data LV allocation changed from the remaining free extents to a fixed size:

```diff
- {"mount_point": "/instance_storage", "group_id": $DOG_GROUP_ID, "portion": "*"}
+ {"mount_point": "/instance_storage", "group_id": $DOG_GROUP_ID, "absoluteSize": "256G"}
```

Second, after provisioning swap and the data filesystem, it reserved the remainder as `dio_raw` if that LV did not already exist. That reservation block required one instance-store device and checked for linear segments. It never partitioned or formatted the raw LV.

Preserve the existing swap handling, XFS/GPT recognition, cache bind, permissions, and `/run/workspaces/ready` behavior. The size and reservation changes were applied to the actual installed XFS script, not to the newer btrfs source.

Syntax-check the replacement, install it atomically, and keep the original. Re-running the installed setup script after migration reused the existing GPT, swap, XFS, and raw LV. No boot migration unit was installed and no host reboot was performed.

A stop/start or host replacement can erase instance storage or replace host customizations. On a new instance, reapply the intended provisioning configuration and rediscover device identity and ranges rather than trusting the previous manifest.

## Restart the existing workspace

Run the updated setup script to mount the restored filesystem and cache bind. Restart only services that were active before maintenance. The sequence used here was:

```sh
/usr/local/lib/workspaces/setup_instance_storage.sh
systemctl reset-failed dd.bb-clientd
systemctl start dd.bb-clientd
# Wait for the cache daemon to be ready.
docker start "$workspace_container"
systemctl start ecs dd.taskapi
```

Use the recorded service states and actual dependencies on a new image. Verify the same container is running, container SSH works, the cache bind refers to the restored filesystem, and swap remains active. Keep the independent host session until this and the raw I/O checks have completed.

## Test the reserved range and clean up

First verify the physical mapping again and write a manifest with the namespace serial, block and character paths, block size, raw byte offset, and raw byte length. Check the serial and geometry whenever that manifest is used on a different boot or instance.

### Enable non-root command execution

The initial hardware tests ran under sudo. A separate validation on the completed workspace established the permissions needed by the normal workspace user. The namespace reports no Command Effects log: node access alone allows Identify, but an actual read returns EACCES with zero capabilities. This kernel and device require CAP_SYS_ADMIN for namespace command submission.

Grant the workspace user read/write access to the character device. The additional read-only block-device ACL is needed only by the hardware test's block-versus-character comparison:

```sh
# Example paths: replace using the current machine's nvme list output.
character_device=/dev/ng1n1
namespace_block_device=/dev/nvme1n1
# Ubuntu: install these utilities if they are absent.
sudo apt-get install -y --no-install-recommends acl
sudo setfacl -m "u:$(id -un):rw" "$character_device"
sudo setfacl -m "u:$(id -un):r" "$namespace_block_device"
getfacl "$character_device" "$namespace_block_device"
```

Read-only Admin commands, such as AWS's statistics log, also need read access
to the matching controller node. On the recorded workspace this is `/dev/nvme1`:

```sh
controller_device=/dev/nvme1
sudo nvme id-ctrl "$controller_device" # Verify the instance-store model and serial.
sudo setfacl -m "u:$(id -un):r" "$controller_device"
getfacl "$controller_device"
```

Build the application or test executable as your normal user, then apply the capability once to each built executable. Reapply it after rebuilding or replacing that file; go run creates a different executable. CAP_SYS_ADMIN is a broad capability: a non-root process carrying it is not fully unprivileged.

```sh
sudo setcap cap_sys_admin=ep /absolute/path/to/your-program
getcap /absolute/path/to/your-program
/absolute/path/to/your-program
```

On this workspace, the root-owned executables are installed at `/usr/local/bin/dio-nvme-example`, `/usr/local/libexec/dio-nvme/nvme.test`, and `/usr/local/libexec/dio-nvme/iosched.test`, with execution restricted to the workspace group and only `cap_sys_admin=ep`. The ACLs apply to the current device nodes; reapply them when nodes are recreated. `/dev` is a private container tmpfs, so a persistent host udev rule alone does not establish the container's ACLs.

The installed dio example exercises two regions separated by a 16 GiB index reservation:

```sh
dio-nvme-example -device "$character_device" \
  -offset "$raw_base" -size "$raw_length" \
  -index-size 17179869184 -write-check
```

The direct ringo hardware test, the iosched hardware test, and the scheduled example all passed when launched as bits without sudo. Both tests recorded UID=2000, EUID=2000, CapEff=0000000000200000 (only CAP_SYS_ADMIN), and no ambient or inheritable capabilities. Their DIO_NVME_REQUIRE_NONROOT=1 guards reject a root test process. The write run covered 512-byte, 4 KiB, and 128 KiB transfers, FUA, namespace Flush, the last block of the raw range, and concurrent completion batches; all writes stayed in the revalidated raw LV range, with mappings unchanged.

```sh
DIO_NVME_DEVICE="$character_device" DIO_NVME_REQUIRE_NONROOT=1 \
  /usr/local/libexec/dio-nvme/nvme.test \
  -test.run '^TestNamespaceHardware$' -test.v
DIO_NVME_DEVICE="$character_device" DIO_NVME_REQUIRE_NONROOT=1 \
  /usr/local/libexec/dio-nvme/iosched.test \
  -test.run '^TestNVMeHardware$' -test.v
```

These commands perform read-only NVMe validation. To repeat write validation before putting live cache contents in the range, additionally set DIO_NVME_WRITE=1, DIO_NVME_OFFSET="$raw_base", and DIO_NVME_SIZE="$raw_length". The completed non-root run is recorded at `/home/bits/go/dio-storage-admin-20261112/nonroot-validation.txt`.

An independent fio check used its native engine:

```sh
sudo fio --name=raw-check \
  --ioengine=io_uring_cmd --cmd_type=nvme \
  --filename="$character_device" --allow_file_create=0 \
  --rw=write --bs=4k --iodepth=8 \
  --offset="$raw_base" --size=4M \
  --verify=crc32c --do_verify=1 --verify_fatal=1
```

These checks overwrite bytes in the reserved range; use them before putting live cache contents there. On the completed instance fio verified 4 MiB written and 4 MiB read with zero errors. The full Linux test suite, vet, and race checks also passed after the migration.

Remove any temporary host public key, verify a fresh login with that key is rejected, and then close the independent host connection. Remove the generated private key from the machine where it was created. The original authorized-key file was restored in this migration, and the inspection mount disappeared when the old container stopped.

## Host access exception used in this migration

When the ordinary AWS host-admin route was unavailable, the workspace already had privileged access to the host's root EBS partition. With explicit authorization for this host, that access was used to inspect the installed files and add a temporary SSH public key. This is an alternative for an already authorized administrator with those existing capabilities, not a replacement for obtaining host-administration permission.

The root partition was identified from the actual mounts. An initial read-only mount failed because the same ext4 superblock was already active read-write. The successful sequence briefly reused that filesystem read-write and immediately made the new inspection mount read-only:

```sh
mount -t ext4 -o rw,noatime "$host_root_partition" "$inspection_directory"
mount -o remount,bind,ro "$inspection_directory"
```

The per-mount read-only flag left the host's original root mount operating normally. This step briefly exposes the active host filesystem read-write; it was specifically approved. No filesystem-repair tool or raw debugfs write was used on the active root.

A new keypair was generated on the Mac; only its public key was copied to the workspace. The host's public SSH fingerprint was checked against the inspected host key. The temporary line was appended to `ubuntu`'s existing `authorized_keys`, preserving the other keys, owner, and mode. Its options were:

```text
restrict,expiry-time="YYYYMMDDHHMMSS" ssh-ed25519 PUBLIC_KEY temporary-task-label
```

The deadline was one hour ahead in the host's timezone. OpenSSH 8.9 on this host rejected the newer Z-suffixed timestamp form, so the UTC host used the 14-digit local-time form. After the edit, the inspection mount was returned to read-only. No SSH daemon or EC2 authentication-helper configuration was changed.

A foreground SSH control connection on port 21 kept recovery access alive while container SSH was unavailable. In a command runner that reaps detached children, keep that connection foreground with `ControlPersist=no` rather than relying on `ssh -f`. Disable unrelated agent/port forwarding for this maintenance connection. The temporary key and connection were removed after verification.

## Values and artifacts from the completed migration

These values describe the completed instance only.

| Item | Value |
| --- | --- |
| Instance-store namespace capacity | 1,900,000,000,000 bytes |
| Namespace interfaces | `/dev/nvme1n1` and `/dev/ng1n1` |
| Logical block size | 512 bytes |
| Swap LV | 128 GiB |
| Data filesystem LV | 256 GiB |
| Raw LV | `/dev/vg_ws_ephemeral/dio_raw` |
| Raw byte offset | `412317908992` |
| Raw byte length | `1487681880064` |
| Raw end, exclusive | `1899999789056` |
| Example index size | 16 GiB |
| Stable range manifest | `/home/bits/go/dio-nvme-range.json` |
| Retained-data backup | `/home/bits/go/dio-instance-storage-backup-20261112/data` |

The raw mapping was:

```text
0 2905628672 linear 259:3 805308416
```

The original one-off scripts and checkpoints were kept outside the dio repository: host state in `/var/lib/dio-storage-migration`, and prepared helpers and test binaries under `/home/bits/go/dio-storage-admin-20261112`. They contain guards for this host, serial, original sizes, and script checksum. For a new workspace, adapt and review those guards; do not reuse the old approval/checkpoint files.

The reusable API tutorial is [Using io_uring_cmd in dio](io_uring_cmd.md). The workspace migration is an operational setup procedure, separate from dio's library design.
