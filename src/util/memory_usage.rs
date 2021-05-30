use log::info;
use psutil::process::os::linux::ProcessExt;
use std::fmt::Arguments;

const BYTES_IN_GB: f64 = 1024_f64 * 1024_f64 * 1024_f64;

#[allow(clippy::cast_precision_loss)]
pub fn print_memory_usage(msg: Arguments) {
    let process = psutil::process::Process::new(std::process::id()).expect("Error getting process");
    let mem = process.procfs_statm().expect("Error getting memory details");
    info!(
        "Memory usage: total = {:.6} GB, rss = {:.6} GB, shared = {:.6} GB [{}]",
        mem.size as f64 / BYTES_IN_GB,
        mem.resident as f64 / BYTES_IN_GB,
        mem.shared as f64 / BYTES_IN_GB,
        msg
    );
}
