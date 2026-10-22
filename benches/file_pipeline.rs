use std::path::PathBuf;

use anyhow::{Result, ensure};
use clap::Parser;
use fast_dump::sender::file_bench::{AllocationHooks, measure};

#[cfg(feature = "bench-allocations")]
#[path = "support/allocation_counter.rs"]
mod allocation_counter;

#[cfg(feature = "bench-allocations")]
#[global_allocator]
static ALLOCATOR: allocation_counter::CountingAllocator =
    allocation_counter::CountingAllocator::new(std::alloc::System);

#[cfg(feature = "bench-allocations")]
fn allocation_hooks() -> AllocationHooks {
    fn start() {
        ALLOCATOR.start();
    }
    fn stop() -> fast_dump::sender::file_bench::AllocationCounts {
        ALLOCATOR.stop()
    }
    AllocationHooks { start, stop }
}

#[derive(Parser)]
#[command(about = "Measure actual scanner/readers with verification outside timed intervals")]
struct Args {
    #[arg(long)]
    source: PathBuf,
    #[arg(long, value_parser = ["scan", "read", "noop"])]
    stage: String,
    #[arg(long, default_value_t = 1)]
    readers: usize,
    #[arg(long, default_value_t = 1)]
    repeats: usize,
    #[arg(long)]
    allocations: bool,
    #[arg(long = "bench", hide = true)]
    _bench: bool,
}

fn optional(value: Option<u64>) -> String {
    value.map_or_else(|| "unmeasured".to_owned(), |value| value.to_string())
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.repeats > 0, "repeats must be positive");
    let hooks: Option<AllocationHooks> = if args.allocations {
        #[cfg(feature = "bench-allocations")]
        {
            Some(allocation_hooks())
        }
        #[cfg(not(feature = "bench-allocations"))]
        {
            anyhow::bail!("--allocations requires a separate --features bench-allocations build")
        }
    } else {
        None
    };
    for repeat in 1..=args.repeats {
        let result = measure(&args.source, &args.stage, args.readers, hooks)?;
        println!(
            "[file-perf] stage={} repeat={} entries={} files={} bytes={} elapsed_ns={} work_ns={} drop_ns={} cpu_ns={} user_ns={} kernel_ns={} readers={} fingerprint={:016x} allocs={} alloc_bytes={} reallocs={} deallocs={} voluntary_switches={} involuntary_switches={} minor_faults={} major_faults={} process_peak_rss_bytes={} cpu_cycles={} cycle_source={} io_read_ops={} io_write_ops={} io_other_ops={} io_read_bytes={} io_write_bytes={} io_other_bytes={} storage_read_bytes={} storage_write_bytes={} page_faults={} io_source={} io_read_ops_raw={} io_read_bytes_raw={} io_observer_read_ops={} io_observer_read_bytes={} alloc_successes={} alloc_success_bytes={} realloc_successes={} realloc_success_bytes={} alloc_failures={} realloc_failures={} dealloc_bytes={} zeroed_bytes={} counters_schema=2 allocator_instrumented={} allocations_enabled={} verified=true",
            args.stage,
            repeat,
            result.entries,
            result.files,
            result.bytes,
            result.elapsed_ns,
            result.work_ns,
            result.drop_ns,
            result.user_ns + result.kernel_ns,
            result.user_ns,
            result.kernel_ns,
            args.readers,
            result.fingerprint,
            optional(result.allocations.map(|value| value.allocs)),
            optional(result.allocations.map(|value| value.alloc_bytes)),
            optional(result.allocations.map(|value| value.reallocs)),
            optional(result.allocations.map(|value| value.deallocs)),
            optional(result.voluntary_switches),
            optional(result.involuntary_switches),
            optional(result.minor_faults),
            optional(result.major_faults),
            result.process_peak_rss_bytes,
            optional(result.cpu_cycles),
            result.cycle_source,
            optional(result.io_read_ops),
            optional(result.io_write_ops),
            optional(result.io_other_ops),
            optional(result.io_read_bytes),
            optional(result.io_write_bytes),
            optional(result.io_other_bytes),
            optional(result.storage_read_bytes),
            optional(result.storage_write_bytes),
            optional(result.page_faults),
            result.io_source,
            optional(result.io_read_ops_raw),
            optional(result.io_read_bytes_raw),
            optional(result.io_observer_read_ops),
            optional(result.io_observer_read_bytes),
            optional(result.allocations.map(|value| value.alloc_successes)),
            optional(result.allocations.map(|value| value.alloc_success_bytes)),
            optional(result.allocations.map(|value| value.realloc_successes)),
            optional(result.allocations.map(|value| value.realloc_success_bytes)),
            optional(result.allocations.map(|value| value.alloc_failures)),
            optional(result.allocations.map(|value| value.realloc_failures)),
            optional(result.allocations.map(|value| value.dealloc_bytes)),
            optional(result.allocations.map(|value| value.zeroed_bytes)),
            cfg!(feature = "bench-allocations"),
            args.allocations,
        );
    }
    Ok(())
}
