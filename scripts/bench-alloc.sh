#!/bin/sh

# Run after correctness checks, without other benchmarks/tests on the same host.
# Paths are relative to the repository root unless absolute.
set -eu

repository=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository"
output=${1:?usage: scripts/bench-alloc.sh OUTPUT_DIRECTORY}
mkdir -p "$output"
bench_cpus=${BENCH_CPUS:-1,8}
bench_count=${BENCH_COUNT:-3}
bench_time=${BENCH_TIME:-300ms}
bench_profile_cpu=${BENCH_PROFILE_CPU:-8}

{
	go version
	go env GOOS GOARCH CGO_ENABLED
	uname -srm
	case $(uname -s) in
		Linux) lscpu || true; uptime || true ;;
		Darwin) sysctl machdep.cpu.brand_string hw.logicalcpu 2>/dev/null || true; uptime || true ;;
	esac
	printf 'cpus=%s count=%s benchtime=%s profile_cpu=%s\n' \
		"$bench_cpus" "$bench_count" "$bench_time" "$bench_profile_cpu"
} > "$output/environment.txt"

go test ./alloc -run '^$' -bench '^BenchmarkAllocator$/^(TryAcquire|Chunks|Batch|SinglePath)$' \
	-benchmem -benchtime="$bench_time" -count="$bench_count" -cpu="$bench_cpus" \
	> "$output/throughput.txt"
go test ./alloc -run '^$' -bench '^BenchmarkAllocator$/^(Acquire|Latency|Cold)$' \
	-benchmem -benchtime="$bench_time" -count="$bench_count" -cpu="$bench_profile_cpu" \
	> "$output/latency-cold.txt"

bench_profiles=$(mktemp -d "${TMPDIR:-/tmp}/dio-alloc-profile.XXXXXX")
trap 'rm -rf "$bench_profiles"' EXIT
for workload in warm blocking; do
	case $workload in
		warm) pattern='^BenchmarkAllocator$/^TryAcquire$/^extent$/^one_open$/^workers64$' ;;
		blocking) pattern='^BenchmarkAllocator$/^Acquire$/^extent$/^chunks1$/^workers64$' ;;
	esac
	go test ./alloc -run '^$' -bench "$pattern" -benchtime=3s -cpu="$bench_profile_cpu" \
		-mutexprofile="$bench_profiles/mutex.pprof" -mutexprofilefraction=1 \
		-blockprofile="$bench_profiles/block.pprof" -blockprofilerate=1 \
		-cpuprofile="$bench_profiles/cpu.pprof" -o "$bench_profiles/alloc.test" \
		> "$output/$workload-profile-run.txt"
	for profile in mutex block cpu; do
		go tool pprof -top -nodecount=20 "$bench_profiles/alloc.test" "$bench_profiles/$profile.pprof" \
			> "$output/$workload-$profile-top.txt"
	done
	go tool pprof -top -sample_index=contentions -nodecount=20 \
		"$bench_profiles/alloc.test" "$bench_profiles/mutex.pprof" \
		> "$output/$workload-mutex-contentions.txt"
done
