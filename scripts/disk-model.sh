#!/bin/bash
# Measures the disk model behind iosched's in-flight budget
# (WithReadBandwidth, WithReadIOPS, WithWriteBandwidth, WithWriteIOPS,
# WithLatencyGoal) on the file system that holds DIR.
#
# Usage: scripts/disk-model.sh DIR      (needs fio >= 3.36 with io_uring, python3)
#
# It writes 8 x 8 GiB read files and 8 GiB write files under DIR, and takes
# about 10 minutes. Steps:
#
#   1. IOPS and bandwidth at 256 requests in flight (8 jobs x depth 32) for
#      4 KiB, 16 KiB and 1 MiB requests. Reads are random over the read files;
#      writes are sequential into freshly fallocated files, the way an
#      append-only store writes. The 4 KiB IOPS and the 1 MiB bandwidth are the
#      four model values.
#   2. 16 KiB check. iosched costs an operation max(bytes/bandwidth, 1/IOPS),
#      which assumes the IOPS and bandwidth limits are independent. Then 16 KiB
#      requests reach min(IOPS, bandwidth/16 KiB). If they reach only about
#      1/(1/IOPS + 16 KiB/bandwidth), the limits are shared and the model
#      overestimates small and mid-size requests.
#   3. Reads and writes at once (1 MiB). iosched budgets reads and writes
#      separately, which assumes the disk reaches both ceilings together. If
#      the two bandwidths instead sum to about one ceiling, the budgets
#      overcommit the disk; halve both bandwidths as a first correction.
#   4. Depth sweep at 1 MiB, one job: throughput and latency at 1, 2, 3, 4 and
#      8 requests in flight. The latency goal should be at least the in-flight
#      time at which throughput reaches its ceiling (depth x size / bandwidth
#      at the first depth within ~5% of the ceiling); more only adds queueing.
set -u
dir=${1:?usage: $0 DIR}
rd=$dir/disk-model-read; wd=$dir/disk-model-write
mkdir -p "$rd" "$wd"
common="--ioengine=io_uring --direct=1 --time_based --ramp_time=5 --runtime=20"
reader() { echo "--name=reader --directory=$rd --rw=randread --norandommap --randrepeat=0 --size=8G $common --bs=$1"; }
writer() { echo "--name=writer --new_group --directory=$wd --rw=write --size=8G --fallocate=posix --refill_buffers $common --bs=$1"; }

summarize() {
  python3 - "$1" "$2" <<'PY'
import json, sys
label, path = sys.argv[1], sys.argv[2]
raw = open(path).read(); d = json.loads(raw[raw.index("{"):])
for j in d["jobs"]:
    for side in ("read", "write"):
        s = j[side]
        if s["bw_bytes"] == 0:
            continue
        p = s["clat_ns"]["percentile"]
        print(f"{label:<20} {side:<5} | {s['iops']:8.0f} IOPS {s['bw_bytes']/1e6:7.0f} MB/s"
              f" | clat p50 {p['50.000000']/1e3:8.0f} us", flush=True)
PY
}
run() {
  local label=$1; shift
  local out; out=$(mktemp)
  rm -f "$wd"/*
  if fio --output-format=json "$@" > "$out" 2> "$out.err"; then
    summarize "$label" "$out"
  else
    echo "$label: fio failed:"; cat "$out.err"
  fi
  rm -f "$out" "$out.err"
}

echo "=== 1. IOPS and bandwidth at 256 in flight"
for bs in 4k 16k 1M; do
  run "$bs read"  $(reader $bs) --numjobs=8 --iodepth=32 --group_reporting
  run "$bs write" $(writer $bs) --numjobs=8 --iodepth=32 --group_reporting
done
echo "=== 2. compare the 16 KiB IOPS with min(4 KiB IOPS, 1 MiB bandwidth / 16384)"
echo "=== 3. reads and writes at once, 1 MiB"
run "1M read+write" $(reader 1M) --numjobs=1 --iodepth=16 $(writer 1M) --numjobs=1 --iodepth=4
echo "=== 4. depth sweep, 1 MiB, one job"
for depth in 1 2 3 4 8; do
  run "1M read  depth $depth" $(reader 1M) --numjobs=1 --iodepth=$depth
  run "1M write depth $depth" $(writer 1M) --numjobs=1 --iodepth=$depth
done
rm -rf "$wd"
echo "=== done (read files left in $rd for reruns)"
