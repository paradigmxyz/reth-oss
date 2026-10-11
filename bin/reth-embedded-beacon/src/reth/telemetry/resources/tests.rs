//! Resource observations never substitute missing values or partial scans with valid totals.

use super::*;
use std::fs;
use tempfile::tempdir;

#[cfg(target_os = "linux")]
use std::os::unix::fs::symlink;

fn budget(entries: usize, depth: usize) -> ScanBudget {
    ScanBudget { entries, duration: Duration::from_secs(5), depth }
}

fn storage_paths(root: &Path) -> StoragePaths {
    StoragePaths {
        database: root.join("mdbx.dat"),
        static_files: root.join("static_files"),
        shared_data: root.join("shared"),
        volume: root.to_path_buf(),
    }
}

#[test]
fn proc_io_preserves_zero_missing_and_malformed_fields() {
    let io = parse_io("rchar: 42\nwchar: 0\nsyscr: 9\nsyscw: 7\nread_bytes: 512\nwrite_bytes: 1024\ncancelled_write_bytes: 3\nunknown: 99\n");
    assert_eq!(
        io,
        ProcessIo {
            rchar: Some(42),
            wchar: Some(0),
            syscr: Some(9),
            syscw: Some(7),
            read_bytes: Some(512),
            write_bytes: Some(1024),
            cancelled_write_bytes: Some(3),
        }
    );
    assert_eq!(
        parse_io(
            "wchar: 0\nrchar: -1\nread_bytes: 18446744073709551616\nwrite_bytes: 1 trailing\n"
        ),
        ProcessIo { wchar: Some(0), ..Default::default() }
    );
    assert_eq!(parse_io(""), ProcessIo::default());
}

#[test]
fn proc_memory_requires_kilobytes_and_checked_conversion() {
    assert_eq!(parse_memory("VmRSS: 2 kB\nVmSize: 8 kB\nVmSwap: 0 kB\nRssAnon: 1 kB\nRssFile: 1 kB\nRssShmem: 0 kB\nVmHWM: 3 kB\nVmPeak: 9 kB\nThreads: 32\n"),
        ProcessMemory {
            resident_bytes: Some(2048), virtual_bytes: Some(8192), swap_bytes: Some(0),
            anonymous_bytes: Some(1024), file_backed_bytes: Some(1024), shared_bytes: Some(0),
            peak_resident_bytes: Some(3072), peak_virtual_bytes: Some(9216), threads: Some(32),
        });
    assert_eq!(
        parse_memory(
            "VmRSS: 2 bytes\nVmSize: 18446744073709551615 kB\nVmSwap: 0\nThreads: 32 kB\n"
        ),
        ProcessMemory::default()
    );
}

#[test]
fn proc_stat_handles_spaces_and_parentheses_in_comm() {
    let stat = parse_stat("42 (worker ) name) R 1 2 3 4 5 6 17 8 19 10 120 30").unwrap();
    assert_eq!(stat.cpu_ticks, Some(150));
    assert_eq!(stat.minor_faults, Some(17));
    assert_eq!(stat.major_faults, Some(19));
    assert!(parse_stat("42 (name) R 1 2").is_none());
    assert!(parse_stat("unknown (name) R 1 2 3 4 5 6 17 8 19 10 120 30").is_none());
    let overflow = parse_stat("42 (name) R 1 2 3 4 5 6 17 8 19 10 18446744073709551615 1").unwrap();
    assert_eq!(overflow.cpu_ticks, None);
    assert_eq!(overflow.minor_faults, Some(17));
}

#[test]
fn cpu_rate_needs_real_monotonic_delta_without_counter_regression() {
    let previous = Instant::now();
    let now = previous + Duration::from_secs(4);
    assert_eq!(cpu_rate(Some((previous, 10.0)), now, Some(18.0)), Some(2.0));
    assert_eq!(cpu_rate(Some((previous, 10.0)), now, Some(10.0)), Some(0.0));
    assert_eq!(cpu_rate(None, now, Some(18.0)), None);
    assert_eq!(cpu_rate(Some((previous, 10.0)), now, None), None);
    assert_eq!(cpu_rate(Some((previous, 10.0)), now, Some(9.0)), None);
    assert_eq!(cpu_rate(Some((previous, 10.0)), previous, Some(10.0)), None);
    assert_eq!(cpu_rate(Some((now, 10.0)), previous, Some(10.0)), None);
    assert_eq!(cpu_rate(Some((previous, 10.0)), now, Some(f64::NAN)), None);
}

#[test]
fn proc_read_is_bounded_and_missing_is_unavailable() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("input");
    assert_eq!(bounded_read(&path), None);
    fs::write(&path, vec![b'x'; PROC_READ_LIMIT as usize]).unwrap();
    assert_eq!(bounded_read(&path).unwrap().len(), PROC_READ_LIMIT as usize);
    fs::write(&path, vec![b'x'; PROC_READ_LIMIT as usize + 1]).unwrap();
    assert_eq!(bounded_read(&path), None);
}

#[test]
fn bounded_enumeration_never_exposes_partial_count_as_total() {
    let directory = tempdir().unwrap();
    for name in ["a", "b", "c"] {
        fs::write(directory.path().join(name), []).unwrap();
    }
    let partial = bounded_count(directory.path(), 2).unwrap();
    assert_eq!(partial, BoundedCount { observed: 2, complete: false });
    assert_eq!(partial.total(), None);
    assert_eq!(bounded_count(directory.path(), 3).unwrap().total(), Some(3));
    assert_eq!(bounded_count(&directory.path().join("missing"), 2), None);
}

#[test]
fn storage_distinguishes_missing_empty_and_partial_scans() {
    let directory = tempdir().unwrap();
    assert_eq!(directory_usage(&directory.path().join("missing"), budget(4, 0)), None);
    let empty = directory_usage(directory.path(), budget(4, 0)).unwrap();
    assert_eq!(empty.total_usage().unwrap().logical_bytes, 0);
    for name in ["a", "b", "c"] {
        fs::write(directory.path().join(name), [1, 2, 3]).unwrap();
    }
    let partial = directory_usage(directory.path(), budget(2, 0)).unwrap();
    assert_eq!(partial.entries, 2);
    assert_eq!(partial.observed.logical_bytes, 6);
    assert_eq!(partial.total_usage(), None);
    assert_eq!(
        directory_usage(directory.path(), budget(3, 0))
            .unwrap()
            .total_usage()
            .unwrap()
            .logical_bytes,
        9
    );
    let expired =
        directory_usage(directory.path(), ScanBudget { duration: Duration::ZERO, ..budget(4, 0) })
            .unwrap();
    assert!(!expired.complete);
    assert_eq!(expired.total_usage(), None);
}

#[test]
fn depth_truncation_is_reported_without_following_children() {
    let directory = tempdir().unwrap();
    fs::create_dir_all(directory.path().join("a/b")).unwrap();
    fs::write(directory.path().join("a/b/object"), [1, 2, 3]).unwrap();
    let flat = directory_usage(directory.path(), budget(10, 0)).unwrap();
    assert_eq!(flat.observed.logical_bytes, 0);
    assert_eq!(flat.total_usage(), None);
    assert_eq!(
        directory_usage(directory.path(), budget(10, 2))
            .unwrap()
            .total_usage()
            .unwrap()
            .logical_bytes,
        3
    );
}

#[test]
fn storage_areas_share_one_entry_budget() {
    let directory = tempdir().unwrap();
    let paths = storage_paths(directory.path());
    fs::create_dir(&paths.static_files).unwrap();
    fs::create_dir(&paths.shared_data).unwrap();
    fs::write(&paths.database, [1, 2]).unwrap();
    for name in ["a", "b", "c"] {
        fs::write(paths.static_files.join(name), [1]).unwrap();
    }
    fs::write(paths.shared_data.join("object"), [2]).unwrap();
    let sample = collect_storage(&paths, budget(3, 8));
    assert_eq!(sample.database.unwrap().logical_bytes, 2);
    let static_files = sample.static_files.unwrap();
    let shared_data = sample.shared_data.unwrap();
    assert_eq!(static_files.total_usage().unwrap().logical_bytes, 3);
    assert_eq!(static_files.entries + shared_data.entries, 3);
    assert_eq!(shared_data.total_usage(), None);
}

#[tokio::test]
async fn storage_is_throttled_with_its_original_observation_time() {
    let directory = tempdir().unwrap();
    let paths = storage_paths(directory.path());
    fs::create_dir(&paths.static_files).unwrap();
    fs::create_dir(&paths.shared_data).unwrap();
    fs::write(&paths.database, [1, 2]).unwrap();
    let mut collector = ResourceCollector::new(paths.clone());
    let first = collector.sample().await.unwrap().storage.unwrap();
    fs::write(&paths.database, [1, 2, 3, 4]).unwrap();
    let cached = collector.sample().await.unwrap().storage.unwrap();
    assert_eq!(cached.captured_at, first.captured_at);
    assert_eq!(cached.database.unwrap().logical_bytes, 2);
    collector.last_storage_attempt = Instant::now().checked_sub(STORAGE_INTERVAL);
    let refreshed = collector.sample().await.unwrap().storage.unwrap();
    assert!(refreshed.captured_at >= first.captured_at);
    assert_eq!(refreshed.database.unwrap().logical_bytes, 4);
}

#[cfg(target_os = "linux")]
#[test]
fn symlinks_do_not_include_unowned_bytes() {
    let directory = tempdir().unwrap();
    let outside = tempdir().unwrap();
    fs::write(outside.path().join("secret"), [1; 20]).unwrap();
    symlink(outside.path(), directory.path().join("link")).unwrap();
    let usage = directory_usage(directory.path(), budget(10, 8)).unwrap();
    assert_eq!(usage.observed.logical_bytes, 0);
    assert_eq!(usage.total_usage(), None);
    symlink(outside.path().join("secret"), directory.path().join("file-link")).unwrap();
    assert_eq!(file_usage(&directory.path().join("file-link")), None);
}

#[cfg(target_os = "linux")]
#[test]
fn linux_capacity_and_allocated_bytes_are_actual_readings() {
    let directory = tempdir().unwrap();
    let capacity = filesystem_usage(directory.path()).unwrap();
    assert!(capacity.total_bytes > 0);
    assert!(capacity.available_bytes <= capacity.free_bytes);
    assert!(capacity.free_bytes <= capacity.total_bytes);
    assert_eq!(filesystem_usage(&directory.path().join("missing")), None);
    let path = directory.path().join("file");
    fs::write(&path, [0; 4096]).unwrap();
    let usage = file_usage(&path).unwrap();
    assert_eq!(usage.logical_bytes, 4096);
    assert!(usage.allocated_bytes.is_some());
    let process = collect_process();
    assert!(process.cpu_seconds.is_some());
    assert!(process.memory.unwrap().resident_bytes.is_some());
    assert!(process.open_fds.unwrap().total().is_some());
}
