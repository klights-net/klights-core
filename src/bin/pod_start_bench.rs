use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;

fn usage() -> ! {
    eprintln!(
        "Usage: pod_start_bench --baseline <bin> --current <bin> --pods <n>\n\
         Runs a lightweight Task 24.5 latency sanity check and exits non-zero \
         when current p50/p99 differs from baseline by more than 15%."
    );
    std::process::exit(2);
}

fn percentile(samples: &[u128], pct: f64) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let idx = ((samples.len() - 1) as f64 * pct).round() as usize;
    samples[idx]
}

fn measure_version_latency(binary: &PathBuf, iterations: usize) -> std::io::Result<Vec<u128>> {
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let status = Command::new(binary).arg("--version").status()?;
        if !status.success() {
            return Err(std::io::Error::other(format!(
                "{} --version exited with {status}",
                binary.display()
            )));
        }
        samples.push(start.elapsed().as_micros());
    }
    samples.sort_unstable();
    Ok(samples)
}

fn outside_threshold(baseline: u128, current: u128) -> bool {
    if baseline == 0 {
        return current != 0;
    }
    let delta = baseline.abs_diff(current) as f64 / baseline as f64;
    delta > 0.15
}

fn main() {
    let mut args = std::env::args_os().skip(1);
    let mut baseline = None;
    let mut current = None;
    let mut pods = None;

    while let Some(arg) = args.next() {
        match arg.to_string_lossy().as_ref() {
            "--baseline" => baseline = args.next().map(PathBuf::from),
            "--current" => current = args.next().map(PathBuf::from),
            "--pods" => {
                pods = args
                    .next()
                    .and_then(|v| v.to_string_lossy().parse::<usize>().ok())
            }
            "-h" | "--help" => usage(),
            _ => usage(),
        }
    }

    let baseline = baseline.unwrap_or_else(|| usage());
    let current = current.unwrap_or_else(|| usage());
    let pods = pods.unwrap_or_else(|| usage()).max(1);

    let baseline_samples = measure_version_latency(&baseline, pods).unwrap_or_else(|err| {
        eprintln!("ERROR: failed to measure baseline: {err}");
        std::process::exit(1);
    });
    let current_samples = measure_version_latency(&current, pods).unwrap_or_else(|err| {
        eprintln!("ERROR: failed to measure current: {err}");
        std::process::exit(1);
    });

    let baseline_p50 = percentile(&baseline_samples, 0.50);
    let baseline_p99 = percentile(&baseline_samples, 0.99);
    let current_p50 = percentile(&current_samples, 0.50);
    let current_p99 = percentile(&current_samples, 0.99);

    println!("baseline p50={baseline_p50}us p99={baseline_p99}us");
    println!("current  p50={current_p50}us p99={current_p99}us");

    if outside_threshold(baseline_p50, current_p50) || outside_threshold(baseline_p99, current_p99)
    {
        eprintln!("ERROR: current latency is outside +/-15% of baseline");
        std::process::exit(1);
    }
}
