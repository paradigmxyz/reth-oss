//! Small synthetic scheduling controls and real tiny SSZ adapters; no full corpus run.

use super::*;
use crate::{empty_report, inventory, run, MAX_CASE_WORKERS};
use std::{
    sync::{atomic::AtomicUsize, Condvar, Mutex},
    time::Duration,
};
use tempfile::TempDir;

fn cases(total: usize) -> Vec<Case> {
    (0..total)
        .map(|index| {
            let relative = format!("general/phase0/ssz_generic/boolean/invalid/{index:04}");
            Case {
                path: relative.clone().into(),
                relative,
                preset: "general".into(),
                fork: "phase0".into(),
                runner: "ssz_generic".into(),
                handler: "boolean".into(),
                suite: "invalid".into(),
                name: format!("{index:04}"),
                configuration: Default::default(),
            }
        })
        .collect()
}

fn passed(case: &Case) -> CaseResult {
    CaseResult {
        path: case.relative.clone(),
        outcome: Outcome::Passed,
        message: String::new(),
        seconds: 0.0,
        spec_only_bls_disabled: false,
        spec_only_execution_mock: false,
        spec_only_data_availability_mock: false,
        spec_only_asserted_graph: false,
    }
}

fn semantic_report(mut report: Report) -> serde_json::Value {
    for case in &mut report.cases {
        case.seconds = 0.0;
    }
    serde_json::to_value(report).unwrap()
}

fn options(workers: usize) -> RunOptions {
    RunOptions {
        focused: true,
        authentication_workers: workers.min(4),
        case_workers: workers,
        ..RunOptions::default()
    }
}

#[test]
fn real_tiny_authenticated_ssz_cases_have_identical_serial_and_parallel_reports() {
    let root = TempDir::new().unwrap();
    for (name, bytes) in
        [("a-valid-rejection", [2]), ("b-invalid-rejection", [1]), ("c-valid-rejection", [3])]
    {
        let path = root.path().join(format!("general/phase0/ssz_generic/boolean/invalid/{name}"));
        reth_fs_util::create_dir_all(&path).unwrap();
        reth_fs_util::write(
            path.join("serialized.ssz_snappy"),
            snap::raw::Encoder::new().compress_vec(&bytes).unwrap(),
        )
        .unwrap();
    }
    let path = root.path().join("mainnet/gloas/unimplemented_protocol/unknown/tests/unsupported");
    reth_fs_util::create_dir_all(&path).unwrap();
    reth_fs_util::write(path.join("pre.ssz_snappy"), [0]).unwrap();
    let manifest = inventory(root.path()).unwrap();
    let serial = run(root.path(), &manifest, &options(1));
    assert_eq!(serial.cases.len(), 4);
    assert!(serial.cases.iter().any(|case| case.outcome == Outcome::Passed));
    assert!(serial.cases.iter().any(|case| case.outcome == Outcome::Failed));
    assert!(serial.cases.iter().any(|case| case.outcome == Outcome::Unsupported));
    assert!(!serial.passed());
    for workers in [4, MAX_CASE_WORKERS] {
        for _ in 0..3 {
            assert_eq!(
                semantic_report(serial.clone()),
                semantic_report(run(root.path(), &manifest, &options(workers)))
            );
        }
    }
}

#[test]
fn corrupted_original_member_prevents_any_parallel_case_execution() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("general/phase0/ssz_generic/boolean/invalid/two");
    reth_fs_util::create_dir_all(&path).unwrap();
    reth_fs_util::write(
        path.join("serialized.ssz_snappy"),
        snap::raw::Encoder::new().compress_vec(&[2]).unwrap(),
    )
    .unwrap();
    let manifest = inventory(root.path()).unwrap();
    reth_fs_util::write(path.join("serialized.ssz_snappy"), b"corrupt").unwrap();
    for workers in [4, MAX_CASE_WORKERS] {
        let report = run(root.path(), &manifest, &options(workers));
        assert!(report.cases.is_empty());
        assert_eq!(report.errors.len(), 1);
        assert!(report.errors[0].contains("fixture checksum mismatch"));
    }
}

#[test]
fn dynamic_queue_advances_behind_a_slow_case_and_restores_canonical_order() {
    let inputs = cases(64);
    for workers in [4, MAX_CASE_WORKERS] {
        let second_completed = (Mutex::new(false), Condvar::new());
        let completions = Mutex::new(Vec::new());
        let mut report = empty_report(false);
        execute_with(&mut report, &inputs, &options(workers), |case| {
            if case.name == "0000" {
                let (lock, changed) = &second_completed;
                let done = lock.lock().unwrap();
                let (done, _) = changed
                    .wait_timeout_while(done, Duration::from_secs(2), |done| !*done)
                    .unwrap();
                assert!(
                    *done,
                    "the next independent case must execute while the first case is still active"
                );
            } else if case.name == "0001" {
                completions.lock().unwrap().push(case.relative.clone());
                let (lock, changed) = &second_completed;
                *lock.lock().unwrap() = true;
                changed.notify_all();
                return passed(case);
            }
            completions.lock().unwrap().push(case.relative.clone());
            passed(case)
        });
        assert!(report.passed(), "{report:?}");
        assert_eq!(
            report.cases.iter().map(|case| &case.path).collect::<Vec<_>>(),
            inputs.iter().map(|case| &case.relative).collect::<Vec<_>>()
        );
        assert_ne!(completions.lock().unwrap().first().unwrap(), &inputs[0].relative);
    }
}

#[test]
fn active_case_contexts_are_bounded_and_every_original_runs_exactly_once() {
    let inputs = cases(64);
    let mut semantic = None;
    assert_eq!(RunOptions::default().case_workers, 1);
    for worker_options in [RunOptions::default(), options(4), options(MAX_CASE_WORKERS)] {
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let visits = (0..inputs.len()).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>();
        let mut report = empty_report(false);
        execute_with(&mut report, &inputs, &worker_options, |case| {
            let index = case.name.parse::<usize>().unwrap();
            visits[index].fetch_add(1, Ordering::SeqCst);
            let live = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(live, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(2));
            active.fetch_sub(1, Ordering::SeqCst);
            passed(case)
        });
        assert!(report.passed(), "{report:?}");
        if worker_options.case_workers == 1 {
            assert_eq!(peak.load(Ordering::SeqCst), 1);
        } else {
            assert!(peak.load(Ordering::SeqCst) > 1);
        }
        assert!(peak.load(Ordering::SeqCst) <= worker_options.case_workers);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(visits.iter().all(|count| count.load(Ordering::SeqCst) == 1));
        assert_eq!(
            report.cases.iter().map(|case| &case.path).collect::<Vec<_>>(),
            inputs.iter().map(|case| &case.relative).collect::<Vec<_>>()
        );
        let actual = semantic_report(report);
        if let Some(expected) = &semantic {
            assert_eq!(&actual, expected);
        }
        semantic = Some(actual);
    }
}

#[test]
fn handler_panic_is_an_explicit_original_failure_with_no_missing_later_cases() {
    let inputs = cases(64);
    let mut semantic = None;
    for workers in [1, 4, MAX_CASE_WORKERS] {
        let visits = (0..inputs.len()).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>();
        let mut report = empty_report(false);
        execute_with(&mut report, &inputs, &options(workers), |case| {
            visits[case.name.parse::<usize>().unwrap()].fetch_add(1, Ordering::SeqCst);
            assert!(case.name != "0007", "intentional fixture panic");
            passed(case)
        });
        assert_eq!(report.cases.len(), inputs.len());
        assert!(!report.passed());
        assert_eq!(report.cases[7].path, inputs[7].relative);
        assert_eq!(report.cases[7].outcome, Outcome::Failed);
        assert!(report.cases[7].message.contains("intentional fixture panic"));
        assert_eq!(report.cases.iter().filter(|case| case.outcome == Outcome::Failed).count(), 1);
        assert!(visits.iter().all(|count| count.load(Ordering::SeqCst) == 1));
        let actual = semantic_report(report);
        if let Some(expected) = &semantic {
            assert_eq!(&actual, expected);
        }
        semantic = Some(actual);
    }
}

#[test]
fn bounded_options_duplicates_and_wrong_returned_identity_fail_closed() {
    let mut inputs = cases(2);
    let visits = AtomicUsize::new(0);
    for workers in [0, MAX_CASE_WORKERS + 1] {
        let mut report = empty_report(false);
        execute_with(&mut report, &inputs, &options(workers), |case| {
            visits.fetch_add(1, Ordering::SeqCst);
            passed(case)
        });
        assert!(report.cases.is_empty());
        assert_eq!(
            report.errors,
            vec![format!("fixture case workers must be between 1 and {MAX_CASE_WORKERS}")]
        );
    }
    assert_eq!(visits.load(Ordering::SeqCst), 0);
    inputs[1].relative = inputs[0].relative.clone();
    for workers in [4, MAX_CASE_WORKERS] {
        let mut report = empty_report(false);
        execute_with(&mut report, &inputs, &options(workers), |case| {
            visits.fetch_add(1, Ordering::SeqCst);
            passed(case)
        });
        assert!(report.cases.is_empty());
        assert_eq!(visits.load(Ordering::SeqCst), 0);
        assert!(report.errors[0].contains("duplicate canonical"));
        let originals = cases(64);
        let mut report = empty_report(false);
        execute_with(&mut report, &originals, &options(workers), |case| {
            let mut result = passed(case);
            result.path = "substituted/case".into();
            result
        });
        assert!(!report.passed());
        assert_eq!(report.cases.len(), originals.len());
        assert!(report
            .cases
            .iter()
            .all(|case| case.outcome == Outcome::Failed &&
                case.message.contains("identity mismatch")));
        assert_eq!(
            report.cases.iter().map(|case| &case.path).collect::<Vec<_>>(),
            originals.iter().map(|case| &case.relative).collect::<Vec<_>>()
        );
    }
}

#[test]
fn duplicate_or_out_of_inventory_completion_never_overwrites_a_success() {
    let inputs = cases(2);
    let mut collector = Collector::new(2, 0);
    collector.record(0, passed(&inputs[0]));
    collector.record(0, failed_result(&inputs[0], "duplicate".into(), 0.0));
    collector.record(2, passed(&inputs[1]));
    assert_eq!(collector.completed, 1);
    assert_eq!(collector.results[0].as_ref().unwrap().outcome, Outcome::Passed);
    assert!(collector.results[1].is_none());
    assert_eq!(collector.errors.len(), 2);
}
