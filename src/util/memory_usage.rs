use log::info;
use std::convert::TryFrom;
use std::fmt::Arguments;

const BYTES_IN_GB: f64 = 1024_f64 * 1024_f64 * 1024_f64;

#[allow(clippy::cast_precision_loss)]
pub fn print_memory_usage(msg: Arguments) {
    let pid = std::process::id();
    let mem = psutil::process::Memory::new(i32::try_from(pid).expect("PID overflow"))
        .expect("Could not get process memory");
    info!(
        "Memory usage: total = {:.6} GB, rss = {:.6} GB [{}]",
        mem.size as f64 / BYTES_IN_GB,
        mem.resident as f64 / BYTES_IN_GB,
        msg
    );
}
