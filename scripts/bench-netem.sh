#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
    cat >&2 <<'USAGE'
Usage: bash bench-netem.sh {none|1gbit|10gbit} LABEL [LINUX_BENCH_BINARY_OR_PY_DRIVER [BENCH_ARGS...]]

Always creates a fresh network namespace; host interfaces are never configured.
Calibration only when LINUX_BENCH_BINARY is omitted.
Results: results/local/benchmarks/netem/LABEL/; FAST_DUMP_NETEM_DIR overrides the output root.
Defaults: NETEM_DELAY=500us, NETEM_MTU=1500, CALIBRATION_MIB=256.
The single lo qdisc applies delay in both directions and SHARES its rate cap
between directions. It is not a full-duplex pair of independent rate limiters.
Benchmark network-profile is forced to loopback-localhost to avoid double shaping.
Example: bash bench-netem.sh 1gbit cal-1g
Example: CALIBRATION_MIB=1024 bash bench-netem.sh 10gbit cal-10g
Example: bash bench-netem.sh 10gbit candidate /tmp/loopback_send_dump \
  --dataset bigFiles,mixed --executor default --threads 4 --frames 64 --buffers 1024 --repeats 3
USAGE
    exit 2
fi

rate=$1
label=$2
shift 2
case "$rate" in none|1gbit|10gbit) ;; *) echo "Unsupported rate: $rate" >&2; exit 2 ;; esac
[[ "$label" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || { echo 'Invalid run label' >&2; exit 2; }
[[ "$label" != '.' && "$label" != '..' ]] || exit 2
[[ $(id -u) -eq 0 ]] || { echo 'Run as Linux root to create the network namespace.' >&2; exit 2; }
for executable in unshare ip tc python3 readlink; do command -v "$executable" >/dev/null; done
for argument in "$@"; do
    case "$argument" in --network-profile|--network-profile=*) echo 'Do not pass --network-profile: kernel shaping is already configured.' >&2; exit 2 ;; esac
done

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "$script_dir/.." && pwd)
output_root=$(python3 -c 'import os, sys; print(os.path.abspath(sys.argv[1]))' "${FAST_DUMP_NETEM_DIR:-${FAST_DUMP_RESULTS_RUN_DIR:-$repo_dir/results/local/benchmarks}/netem}")
parent_namespace=$(readlink /proc/self/ns/net)

# Все ip/tc находятся только в дочернем unshare. Повторная проверка inode
# не даёт продолжить, если создание отдельного namespace вдруг не состоялось.
exec unshare --net --fork bash -s -- "$parent_namespace" "$script_dir" "$output_root" "$rate" "$label" "$@" <<'NAMESPACE_SCRIPT'
set -euo pipefail
parent_namespace=$1
script_dir=$2
output_root=$3
rate=$4
label=$5
shift 5
current_namespace=$(readlink /proc/self/ns/net)
[[ "$current_namespace" != "$parent_namespace" ]] || { echo 'Refusing to configure parent network namespace.' >&2; exit 1; }
[[ $(ip -o link show | wc -l) -eq 1 ]] || { echo 'Fresh namespace unexpectedly contains more than loopback.' >&2; exit 1; }

mkdir -p -- "$output_root"
output_dir="$output_root/$label"
mkdir -- "$output_dir"
delay=${NETEM_DELAY:-500us}
mtu=${NETEM_MTU:-1500}
[[ "$rate" != none ]] || delay=0
[[ "$mtu" =~ ^[0-9]+$ ]] || { echo 'Invalid MTU' >&2; exit 2; }
ip link set dev lo mtu "$mtu"
ip link set dev lo up
if [[ "$rate" != none ]]; then
    tc qdisc add dev lo root netem limit 65536 delay "$delay" rate "$rate"
fi

printf 'parent_namespace=%s\nnamespace=%s\nrate=%s\ndelay_each_direction=%s\nmtu=%s\nrate_scope=shared_loopback_egress\n' \
    "$parent_namespace" "$current_namespace" "$rate" "$delay" "$mtu" > "$output_dir/link.txt"
uname -a >> "$output_dir/link.txt"
ip -details link show dev lo >> "$output_dir/link.txt"
tc -s -d qdisc show dev lo > "$output_dir/qdisc-before.txt"
trap 'tc -s -d qdisc show dev lo > "$output_dir/qdisc-final.txt"' EXIT

python3 "$script_dir/bench-calibrate-tcp.py" \
    --bytes-mib "${CALIBRATION_MIB:-256}" \
    --rtt-samples "${CALIBRATION_RTT_SAMPLES:-128}" \
    --rate-label "$rate" \
    --output "$output_dir/calibration.json" | tee "$output_dir/calibration.log"
tc -s -d qdisc show dev lo > "$output_dir/qdisc-after-calibration.txt"

if [[ $# -gt 0 ]]; then
    benchmark=$1
    shift
    if [[ "$benchmark" == *.py ]]; then
        [[ -f "$benchmark" ]] || { echo "Python driver does not exist: $benchmark" >&2; exit 2; }
        benchmark_command=(python3 "$benchmark")
    else
        [[ -x "$benchmark" ]] || { echo "Benchmark is not executable: $benchmark" >&2; exit 2; }
        benchmark_command=("$benchmark")
    fi
    export FAST_DUMP_BENCH_MATRIX_DIR="$output_dir/bench"
    export FAST_DUMP_BENCH_VERIFY_ARCHIVE=${FAST_DUMP_BENCH_VERIFY_ARCHIVE:-1}
    printf '%q ' "${benchmark_command[@]}" --network-profile loopback-localhost "$@" > "$output_dir/benchmark-command.txt"
    printf '\n' >> "$output_dir/benchmark-command.txt"
    "${benchmark_command[@]}" --network-profile loopback-localhost "$@" 2>&1 | tee "$output_dir/benchmark.log"
fi
NAMESPACE_SCRIPT
