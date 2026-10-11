//! Bounded observational resource sampling for the optional embedded launcher.
//! Blocking proc and filesystem reads never run on an executor thread. Incomplete scans are
//! lower bounds, not storage totals, and cannot change execution or consensus authority.

use std::{
    fs::{symlink_metadata, Metadata},
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use std::{
    ffi::CString,
    mem::MaybeUninit,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
};

/// Stateful sampler; callers choose their own shutdown-bound sampling interval.
pub(in crate::reth) struct ResourceCollector {
    paths: StoragePaths,
    last_storage_attempt: Option<Instant>,
    storage: Option<StorageSample>,
    previous_cpu: Option<(Instant, f64)>,
}

impl ResourceCollector {
    pub(in crate::reth) fn new(paths: StoragePaths) -> Self {
        Self { paths, last_storage_attempt: None, storage: None, previous_cpu: None }
    }

    /// Perform one bounded blocking job. Failed jobs and unavailable readings remain absent.
    pub(super) async fn sample(&mut self) -> Option<ResourceSample> {
        let scan_storage =
            self.last_storage_attempt.is_none_or(|last| last.elapsed() >= STORAGE_INTERVAL);
        let paths = self.paths.clone();
        let mut sample =
            tokio::task::spawn_blocking(move || collect(&paths, scan_storage)).await.ok()?;
        if scan_storage {
            self.last_storage_attempt = Some(sample.captured_at);
            self.storage = sample.storage.take();
        }
        sample.storage = self.storage.clone();
        sample.cpu_cores =
            cpu_rate(self.previous_cpu, sample.captured_at, sample.process.cpu_seconds);
        self.previous_cpu = sample.process.cpu_seconds.map(|cpu| (sample.captured_at, cpu));
        Some(sample)
    }
}

/// Exact paths supplied by the launched node, including any static-file override.
#[derive(Debug, Clone)]
pub(in crate::reth) struct StoragePaths {
    pub(in crate::reth) database: PathBuf,
    pub(in crate::reth) static_files: PathBuf,
    pub(in crate::reth) shared_data: PathBuf,
    pub(in crate::reth) volume: PathBuf,
}

/// Time-stamped readings; missing values must never be converted to healthy zeroes.
#[derive(Debug, Clone)]
pub(super) struct ResourceSample {
    pub(super) captured_at: Instant,
    pub(super) process: ProcessSample,
    pub(super) filesystem: Option<FilesystemSample>,
    pub(super) storage: Option<StorageSample>,
    /// CPU seconds per wall second, potentially greater than one for multiple cores.
    pub(super) cpu_cores: Option<f64>,
    pub(super) sampling_seconds: f64,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ProcessSample {
    pub(super) io: Option<ProcessIo>,
    pub(super) memory: Option<ProcessMemory>,
    pub(super) cpu_seconds: Option<f64>,
    pub(super) minor_faults: Option<u64>,
    pub(super) major_faults: Option<u64>,
    /// Enumeration includes the transient descriptor used to inspect this directory.
    pub(super) open_fds: Option<BoundedCount>,
}

/// Kernel cumulative process counters; rchar/wchar also include cached and non-file I/O.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ProcessIo {
    pub(super) rchar: Option<u64>,
    pub(super) wchar: Option<u64>,
    pub(super) syscr: Option<u64>,
    pub(super) syscw: Option<u64>,
    pub(super) read_bytes: Option<u64>,
    pub(super) write_bytes: Option<u64>,
    pub(super) cancelled_write_bytes: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ProcessMemory {
    pub(super) resident_bytes: Option<u64>,
    pub(super) virtual_bytes: Option<u64>,
    pub(super) swap_bytes: Option<u64>,
    pub(super) anonymous_bytes: Option<u64>,
    pub(super) file_backed_bytes: Option<u64>,
    pub(super) shared_bytes: Option<u64>,
    pub(super) peak_resident_bytes: Option<u64>,
    pub(super) peak_virtual_bytes: Option<u64>,
    pub(super) threads: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BoundedCount {
    pub(super) observed: u64,
    pub(super) complete: bool,
}

impl BoundedCount {
    pub(super) fn total(self) -> Option<u64> {
        self.complete.then_some(self.observed)
    }
}

#[derive(Debug, Clone)]
pub(super) struct StorageSample {
    pub(super) captured_at: Instant,
    pub(super) database: Option<FileUsage>,
    pub(super) static_files: Option<DirectoryUsage>,
    pub(super) shared_data: Option<DirectoryUsage>,
}

/// Regular-file bytes, excluding directory metadata and without following symlinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FileUsage {
    pub(super) logical_bytes: u64,
    pub(super) allocated_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DirectoryUsage {
    /// Lower bound when the enumeration was incomplete.
    pub(super) observed: FileUsage,
    pub(super) entries: u64,
    pub(super) complete: bool,
}

impl DirectoryUsage {
    /// Callers may publish a total only when every eligible entry was inspected.
    pub(super) fn total_usage(self) -> Option<FileUsage> {
        self.complete.then_some(self.observed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FilesystemSample {
    pub(super) total_bytes: u64,
    pub(super) free_bytes: u64,
    pub(super) available_bytes: u64,
    pub(super) total_inodes: Option<u64>,
    pub(super) free_inodes: Option<u64>,
    pub(super) available_inodes: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct ScanBudget {
    entries: usize,
    duration: Duration,
    depth: usize,
}

const PROC_READ_LIMIT: u64 = 128 * 1024;
const FD_LIMIT: usize = 4096;
const STORAGE_INTERVAL: Duration = Duration::from_secs(120);
const STORAGE_BUDGET: ScanBudget =
    ScanBudget { entries: 4096, duration: Duration::from_millis(100), depth: 8 };

fn collect(paths: &StoragePaths, scan_storage: bool) -> ResourceSample {
    let started = Instant::now();
    let process = collect_process();
    let filesystem = filesystem_usage(&paths.volume);
    let storage = scan_storage.then(|| collect_storage(paths, STORAGE_BUDGET));
    ResourceSample {
        captured_at: started,
        process,
        filesystem,
        storage,
        cpu_cores: None,
        sampling_seconds: started.elapsed().as_secs_f64(),
    }
}

fn collect_storage(paths: &StoragePaths, budget: ScanBudget) -> StorageSample {
    let captured_at = Instant::now();
    let database = file_usage(&paths.database);
    let static_files = directory_usage(&paths.static_files, ScanBudget { depth: 0, ..budget });
    let shared_budget = ScanBudget {
        entries: budget
            .entries
            .saturating_sub(static_files.map_or(0, |usage| usage.entries as usize)),
        duration: budget.duration.saturating_sub(captured_at.elapsed()),
        ..budget
    };
    let shared_data = directory_usage(&paths.shared_data, shared_budget);
    StorageSample { captured_at, database, static_files, shared_data }
}

fn cpu_rate(previous: Option<(Instant, f64)>, now: Instant, cpu: Option<f64>) -> Option<f64> {
    let ((previous_time, previous_cpu), cpu) = (previous?, cpu?);
    let elapsed = now.checked_duration_since(previous_time)?.as_secs_f64();
    let consumed = cpu - previous_cpu;
    if elapsed > 0.0 && consumed >= 0.0 && consumed.is_finite() {
        Some(consumed / elapsed)
    } else {
        None
    }
}

#[cfg(target_os = "linux")]
fn collect_process() -> ProcessSample {
    let stat = bounded_read(Path::new("/proc/self/stat")).and_then(|text| parse_stat(&text));
    // SAFETY: sysconf accepts this defined constant and does not access caller memory.
    let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    ProcessSample {
        io: bounded_read(Path::new("/proc/self/io")).map(|text| parse_io(&text)),
        memory: bounded_read(Path::new("/proc/self/status")).map(|text| parse_memory(&text)),
        cpu_seconds: stat
            .as_ref()
            .and_then(|stat| stat.cpu_ticks)
            .filter(|_| ticks_per_second > 0)
            .map(|ticks| ticks as f64 / ticks_per_second as f64),
        minor_faults: stat.as_ref().and_then(|stat| stat.minor_faults),
        major_faults: stat.as_ref().and_then(|stat| stat.major_faults),
        open_fds: bounded_count(Path::new("/proc/self/fd"), FD_LIMIT),
    }
}

#[cfg(not(target_os = "linux"))]
fn collect_process() -> ProcessSample {
    ProcessSample::default()
}

fn bounded_read(path: &Path) -> Option<String> {
    let mut text = String::new();
    reth_fs_util::open(path).ok()?.take(PROC_READ_LIMIT + 1).read_to_string(&mut text).ok()?;
    (text.len() <= PROC_READ_LIMIT as usize).then_some(text)
}

fn parse_io(text: &str) -> ProcessIo {
    let mut result = ProcessIo::default();
    for (name, value) in text.lines().filter_map(|line| line.split_once(':')) {
        let parsed = parse_unsigned(value, None);
        match name {
            "rchar" => result.rchar = parsed,
            "wchar" => result.wchar = parsed,
            "syscr" => result.syscr = parsed,
            "syscw" => result.syscw = parsed,
            "read_bytes" => result.read_bytes = parsed,
            "write_bytes" => result.write_bytes = parsed,
            "cancelled_write_bytes" => result.cancelled_write_bytes = parsed,
            _ => {}
        }
    }
    result
}

fn parse_memory(text: &str) -> ProcessMemory {
    let mut result = ProcessMemory::default();
    for (name, value) in text.lines().filter_map(|line| line.split_once(':')) {
        if name == "Threads" {
            result.threads = parse_unsigned(value, None);
            continue;
        }
        let bytes = parse_unsigned(value, Some("kB")).and_then(|value| value.checked_mul(1024));
        match name {
            "VmRSS" => result.resident_bytes = bytes,
            "VmSize" => result.virtual_bytes = bytes,
            "VmSwap" => result.swap_bytes = bytes,
            "RssAnon" => result.anonymous_bytes = bytes,
            "RssFile" => result.file_backed_bytes = bytes,
            "RssShmem" => result.shared_bytes = bytes,
            "VmHWM" => result.peak_resident_bytes = bytes,
            "VmPeak" => result.peak_virtual_bytes = bytes,
            _ => {}
        }
    }
    result
}

fn parse_unsigned(text: &str, unit: Option<&str>) -> Option<u64> {
    let mut tokens = text.split_whitespace();
    let value = tokens.next()?.parse().ok()?;
    (tokens.next() == unit && tokens.next().is_none()).then_some(value)
}

#[derive(Debug)]
struct ProcessStat {
    cpu_ticks: Option<u64>,
    minor_faults: Option<u64>,
    major_faults: Option<u64>,
}

fn parse_stat(text: &str) -> Option<ProcessStat> {
    // Linux comm may contain whitespace and parentheses; the last ')' ends field 2.
    let (pid, _) = text.split_once('(')?;
    pid.trim().parse::<u32>().ok()?;
    let fields = text.get(text.rfind(')')? + 1..)?.split_whitespace().collect::<Vec<_>>();
    if fields.len() < 13 || fields[0].len() != 1 || !fields[0].as_bytes()[0].is_ascii_alphabetic() {
        return None;
    }
    let user_ticks = fields[11].parse::<u64>().ok();
    let system_ticks = fields[12].parse::<u64>().ok();
    Some(ProcessStat {
        cpu_ticks: user_ticks.zip(system_ticks).and_then(|(user, system)| user.checked_add(system)),
        minor_faults: fields[7].parse().ok(),
        major_faults: fields[9].parse().ok(),
    })
}

fn bounded_count(path: &Path, limit: usize) -> Option<BoundedCount> {
    let entries = reth_fs_util::read_dir(path).ok()?;
    let mut result = BoundedCount { observed: 0, complete: true };
    for (index, entry) in entries.enumerate() {
        if index >= limit {
            result.complete = false;
            break;
        }
        if entry.is_ok() {
            result.observed += 1;
        } else {
            result.complete = false;
        }
    }
    Some(result)
}

fn file_usage(path: &Path) -> Option<FileUsage> {
    let metadata = symlink_metadata(path).ok()?;
    metadata.is_file().then(|| usage_from_metadata(&metadata))
}

fn usage_from_metadata(metadata: &Metadata) -> FileUsage {
    #[cfg(target_os = "linux")]
    let allocated_bytes = metadata.blocks().checked_mul(512);
    #[cfg(not(target_os = "linux"))]
    let allocated_bytes = None;
    FileUsage { logical_bytes: metadata.len(), allocated_bytes }
}

fn empty_usage() -> FileUsage {
    FileUsage { logical_bytes: 0, allocated_bytes: cfg!(target_os = "linux").then_some(0) }
}

fn directory_usage(path: &Path, budget: ScanBudget) -> Option<DirectoryUsage> {
    if !symlink_metadata(path).ok()?.is_dir() {
        return None;
    }
    let started = Instant::now();
    let mut pending = vec![(path.to_path_buf(), 0)];
    let mut result = DirectoryUsage { observed: empty_usage(), entries: 0, complete: true };
    while let Some((directory, depth)) = pending.pop() {
        if started.elapsed() >= budget.duration {
            result.complete = false;
            break;
        }
        let Ok(entries) = reth_fs_util::read_dir(&directory) else {
            result.complete = false;
            continue;
        };
        for entry in entries {
            // This is an inspection budget, not a deadline that can preempt filesystem syscalls.
            if result.entries >= budget.entries as u64 || started.elapsed() >= budget.duration {
                result.complete = false;
                return Some(result);
            }
            result.entries += 1;
            let Ok(entry) = entry else {
                result.complete = false;
                continue;
            };
            let Ok(metadata) = symlink_metadata(entry.path()) else {
                result.complete = false;
                continue;
            };
            if metadata.is_file() {
                let usage = usage_from_metadata(&metadata);
                if let Some(total) = result.observed.logical_bytes.checked_add(usage.logical_bytes)
                {
                    result.observed.logical_bytes = total;
                } else {
                    result.complete = false;
                }
                result.observed.allocated_bytes = result
                    .observed
                    .allocated_bytes
                    .zip(usage.allocated_bytes)
                    .and_then(|(total, size)| total.checked_add(size));
            } else if metadata.is_dir() && depth < budget.depth {
                pending.push((entry.path(), depth + 1));
            } else {
                // Symlinks, unsupported entries and depth truncation cannot claim a full total.
                result.complete = false;
            }
        }
    }
    Some(result)
}

#[cfg(target_os = "linux")]
fn filesystem_usage(path: &Path) -> Option<FilesystemSample> {
    let path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut output = MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: path is NUL-terminated and output points to correctly sized writable storage.
    if unsafe { libc::statvfs(path.as_ptr(), output.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: a successful statvfs call initialized every field in output.
    let output = unsafe { output.assume_init() };
    let fragment_size = if output.f_frsize == 0 { output.f_bsize } else { output.f_frsize };
    let unit = u64::try_from(fragment_size).ok().filter(|size| *size > 0)?;
    Some(FilesystemSample {
        total_bytes: u64::try_from(output.f_blocks).ok()?.checked_mul(unit)?,
        free_bytes: u64::try_from(output.f_bfree).ok()?.checked_mul(unit)?,
        available_bytes: u64::try_from(output.f_bavail).ok()?.checked_mul(unit)?,
        total_inodes: u64::try_from(output.f_files).ok(),
        free_inodes: u64::try_from(output.f_ffree).ok(),
        available_inodes: u64::try_from(output.f_favail).ok(),
    })
}

#[cfg(not(target_os = "linux"))]
fn filesystem_usage(_path: &Path) -> Option<FilesystemSample> {
    None
}

#[cfg(test)]
mod tests;
