//! Exact fixture inventory authentication with a bounded number of native workers.

use super::{
    checked_relative, hash_file, relative_path, walk_files, FixtureManifest,
    MAX_AUTHENTICATION_WORKERS, SPEC_RELEASE,
};
use reth_embedded_beacon_primitives::root_from_hex;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    thread,
};

pub(super) fn verify_inventory(
    root: &Path,
    manifest: &FixtureManifest,
    workers: usize,
    progress: bool,
) -> Result<Vec<PathBuf>, String> {
    if !(1..=MAX_AUTHENTICATION_WORKERS).contains(&workers) {
        return Err(format!(
            "fixture authentication workers must be between 1 and {MAX_AUTHENTICATION_WORKERS}"
        ));
    }
    if manifest.spec_release != SPEC_RELEASE {
        return Err(format!(
            "fixture release mismatch: expected {SPEC_RELEASE}, received {}",
            manifest.spec_release
        ));
    }
    if manifest.files.is_empty() || manifest.required_suites.is_empty() {
        return Err("manifest inventory and required suite list must be nonempty".into());
    }
    if progress {
        eprintln!(
            "fixture inventory: checking {} published files with {workers} authentication workers",
            manifest.files.len()
        );
    }
    let actual = walk_files(root)?;
    let paths =
        actual.iter().map(|path| relative_path(root, path)).collect::<Result<BTreeSet<_>, _>>()?;
    if paths != manifest.files.keys().cloned().collect() {
        return Err("fixture files differ from manifest inventory".into());
    }
    verify_members(root, manifest, workers, progress)?;
    if progress {
        eprintln!("fixture integrity: all {} files verified", manifest.files.len());
    }
    Ok(actual)
}

fn verify_members(
    root: &Path,
    manifest: &FixtureManifest,
    workers: usize,
    progress: bool,
) -> Result<(), String> {
    // Entries borrow the authenticated manifest; no file bytes or duplicate hash map are retained.
    let entries = manifest.files.iter().collect::<Vec<_>>();
    let next = AtomicUsize::new(0);
    let successful = AtomicUsize::new(0);
    let earliest = Mutex::new(None::<(usize, String)>);
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        let mut infrastructure_error = None;
        for worker in 0..workers {
            let task = || -> Result<(), String> {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some((relative, expected)) = entries.get(index) else { break };
                    match verify_member(root, relative, expected) {
                        Ok(()) => {
                            let verified = successful.fetch_add(1, Ordering::Relaxed) + 1;
                            if progress && verified.is_multiple_of(65536) {
                                eprintln!(
                                    "fixture integrity: {verified}/{} files verified",
                                    entries.len()
                                );
                            }
                        }
                        Err(error) => {
                            let mut first = earliest.lock().map_err(|_| {
                                "fixture authentication error lock poisoned".to_string()
                            })?;
                            if first.as_ref().is_none_or(|(position, _)| index < *position) {
                                *first = Some((index, error));
                            }
                        }
                    }
                }
                Ok(())
            };
            match thread::Builder::new()
                .name(format!("fixture-auth-{worker}"))
                .spawn_scoped(scope, task)
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    infrastructure_error =
                        Some(format!("fixture authentication worker could not start: {error}"));
                    break;
                }
            }
        }
        // Join every started worker even after failure; no background verifier outlives this call.
        for handle in handles {
            let error = match handle.join() {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(_) => Some("fixture authentication worker panicked".into()),
            };
            if infrastructure_error.is_none() {
                infrastructure_error = error;
            }
        }
        if let Some(error) = infrastructure_error {
            return Err(error);
        }
        Ok(())
    })?;
    let first = earliest
        .into_inner()
        .map_err(|_| "fixture authentication error lock poisoned".to_string())?;
    if let Some((_, error)) = first {
        return Err(error);
    }
    if successful.load(Ordering::Relaxed) != entries.len() {
        return Err("fixture authentication did not verify every original member".into());
    }
    Ok(())
}

fn verify_member(root: &Path, relative: &str, expected: &str) -> Result<(), String> {
    let path = checked_relative(root, relative)?;
    let expected = root_from_hex(expected)
        .map_err(|error| format!("invalid checksum for {relative}: {error}"))?;
    // Repeat the symlink check immediately before hashing, in addition to the exact inventory walk.
    let metadata =
        std::fs::symlink_metadata(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(format!("fixture symlink is not allowed: {}", path.display()));
    }
    if !metadata.is_file() {
        return Err(format!("fixture member is not a regular file: {relative}"));
    }
    if hash_file(&path)? != expected {
        return Err(format!("fixture checksum mismatch: {relative}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory;
    use tempfile::TempDir;

    fn corpus() -> TempDir {
        let directory = TempDir::new().unwrap();
        let case = directory.path().join("general/phase0/bls/verify/tests/case");
        reth_fs_util::create_dir_all(&case).unwrap();
        for (name, bytes) in [("a.yaml", vec![1; 131072]), ("z.yaml", vec![2; 3])] {
            reth_fs_util::write(case.join(name), bytes).unwrap();
        }
        // More members than workers, each spanning multiple streamed hash reads.
        for index in 0u8..64 {
            reth_fs_util::write(
                case.join(format!("member-{index:02}.yaml")),
                vec![index; 65537 + usize::from(index) * 257],
            )
            .unwrap();
        }
        directory
    }

    #[test]
    fn serial_and_parallel_workers_verify_the_same_original_inventory() {
        let directory = corpus();
        let manifest = inventory(directory.path()).unwrap();
        let serial = verify_inventory(directory.path(), &manifest, 1, false).unwrap();
        for workers in [4, 32] {
            assert_eq!(
                serial,
                verify_inventory(directory.path(), &manifest, workers, false).unwrap()
            );
        }
    }

    #[test]
    fn concurrent_corruption_reports_the_earliest_manifest_member() {
        let directory = corpus();
        let manifest = inventory(directory.path()).unwrap();
        for relative in manifest.files.keys() {
            reth_fs_util::write(directory.path().join(relative), b"changed").unwrap();
        }
        let expected = "fixture checksum mismatch: general/phase0/bls/verify/tests/case/a.yaml";
        for workers in [1, 4, 32, 32] {
            assert_eq!(
                verify_inventory(directory.path(), &manifest, workers, false).unwrap_err(),
                expected
            );
        }
    }

    #[test]
    fn exact_inventory_and_worker_bounds_remain_fatal() {
        let directory = corpus();
        let manifest = inventory(directory.path()).unwrap();
        for workers in [0, 33] {
            assert_eq!(
                verify_inventory(directory.path(), &manifest, workers, false).unwrap_err(),
                "fixture authentication workers must be between 1 and 32"
            );
        }
        reth_fs_util::write(directory.path().join("unexpected.yaml"), b"extra").unwrap();
        assert_eq!(
            verify_inventory(directory.path(), &manifest, 32, false).unwrap_err(),
            "fixture files differ from manifest inventory"
        );
    }

    #[test]
    fn malformed_digest_and_traversal_are_authentication_failures() {
        let directory = corpus();
        let mut manifest = inventory(directory.path()).unwrap();
        let relative = manifest.files.keys().next().unwrap().clone();
        manifest.files.insert(relative.clone(), "invalid".into());
        assert_eq!(
            verify_inventory(directory.path(), &manifest, 32, false).unwrap_err(),
            verify_member(directory.path(), &relative, "invalid").unwrap_err()
        );
        assert_eq!(
            verify_member(directory.path(), "../escape.yaml", &"00".repeat(32)).unwrap_err(),
            "invalid relative fixture path: ../escape.yaml"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_member_is_rejected_before_any_case_can_run() {
        let directory = corpus();
        let manifest = inventory(directory.path()).unwrap();
        let first = manifest.files.keys().next().unwrap();
        let path = directory.path().join(first);
        let outside = directory.path().join("outside");
        reth_fs_util::write(&outside, reth_fs_util::read(&path).unwrap()).unwrap();
        reth_fs_util::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&outside, &path).unwrap();
        assert_eq!(
            verify_inventory(directory.path(), &manifest, 32, false).unwrap_err(),
            format!("fixture symlink is not allowed: {}", path.display())
        );
    }
}
