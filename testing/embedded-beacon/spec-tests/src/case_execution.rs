//! Bounded whole-case scheduling after complete source authentication.
//!
//! Handlers keep their own state, stores and ordered events. Only immutable case paths and
//! options are shared. The explicit worker count is bounded by `MAX_CASE_WORKERS`: up to 32
//! independent live case/store contexts, with a completion channel bounded by the selected count.
//! The default remains one worker. Completion messages never retain fixture state or bytes, and
//! the collector fills original canonical indices regardless of completion order.

use super::{execute_case_result, Case, CaseResult, Outcome, Report, RunOptions, MAX_CASE_WORKERS};
use std::{
    any::Any,
    collections::BTreeSet,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::sync_channel,
    },
    thread,
    time::Instant,
};

pub(super) fn execute_cases(report: &mut Report, cases: Vec<Case>, options: &RunOptions) {
    execute_with(report, &cases, options, |case| execute_case_result(case, options.max_case_bytes));
}

fn execute_with<F>(report: &mut Report, cases: &[Case], options: &RunOptions, execute: F)
where
    F: Fn(&Case) -> CaseResult + Sync,
{
    if !(1..=MAX_CASE_WORKERS).contains(&options.case_workers) {
        report
            .errors
            .push(format!("fixture case workers must be between 1 and {MAX_CASE_WORKERS}"));
        return;
    }
    if cases.is_empty() {
        report.errors.push("no fixture cases provided for execution".into());
        return;
    }
    if cases.iter().map(|case| case.relative.as_str()).collect::<BTreeSet<_>>().len() != cases.len()
    {
        report.errors.push("duplicate canonical fixture case provided for execution".into());
        return;
    }
    let workers = options.case_workers.min(cases.len());
    if options.progress_every != 0 {
        eprintln!(
            "fixture execution: {} independent cases with {workers} case workers",
            cases.len()
        );
    }
    let mut collector = Collector::new(cases.len(), options.progress_every);
    if workers == 1 {
        for (index, case) in cases.iter().enumerate() {
            collector.record(index, guarded_result(case, &execute));
        }
    } else {
        let next = AtomicUsize::new(0);
        thread::scope(|scope| {
            // Completion messages contain only outcomes, never a live state/store or fixture bytes.
            let (sender, receiver) = sync_channel(workers);
            let mut handles = Vec::with_capacity(workers);
            for worker in 0..workers {
                let sender = sender.clone();
                let next = &next;
                let execute = &execute;
                let task = move || -> Result<(), String> {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(case) = cases.get(index) else { break };
                        let result = guarded_result(case, execute);
                        sender.send((index, result)).map_err(|_| {
                            "fixture execution completion receiver closed".to_string()
                        })?;
                    }
                    Ok(())
                };
                match thread::Builder::new()
                    .name(format!("fixture-case-{worker}"))
                    // Native KZG temporaries require a bounded eight MiB worker stack.
                    .stack_size(8 * 1024 * 1024)
                    .spawn_scoped(scope, task)
                {
                    Ok(handle) => handles.push(handle),
                    Err(error) => {
                        report
                            .errors
                            .push(format!("fixture execution worker could not start: {error}"));
                        break;
                    }
                }
            }
            drop(sender);
            for (index, result) in receiver {
                collector.record(index, result);
            }
            // Join every started worker even after a panic/error; none outlives this report.
            for handle in handles {
                match handle.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => report.errors.push(error),
                    Err(_) => report
                        .errors
                        .push("fixture execution worker panicked outside its case guard".into()),
                }
            }
        });
    }
    if collector.completed != cases.len() || !collector.errors.is_empty() {
        report
            .errors
            .push("fixture execution did not return each original case exactly once".into());
    }
    report.errors.extend(collector.errors);
    report.cases.extend(collector.results.into_iter().zip(cases).map(|(result, case)| {
        result.unwrap_or_else(|| {
            failed_result(case, "fixture worker did not return this original case".into(), 0.0)
        })
    }));
}

fn guarded_result<F>(case: &Case, execute: &F) -> CaseResult
where
    F: Fn(&Case) -> CaseResult,
{
    let started = Instant::now();
    match catch_unwind(AssertUnwindSafe(|| execute(case))) {
        Ok(result) if result.path == case.relative => result,
        Ok(result) => failed_result(
            case,
            format!("fixture result identity mismatch: received {}", result.path),
            started.elapsed().as_secs_f64(),
        ),
        Err(payload) => failed_result(
            case,
            format!(
                "fixture handler panicked; fixture assumption metadata unavailable: {}",
                panic_message(payload.as_ref())
            ),
            started.elapsed().as_secs_f64(),
        ),
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic payload")
}

fn failed_result(case: &Case, message: String, seconds: f64) -> CaseResult {
    CaseResult {
        path: case.relative.clone(),
        outcome: Outcome::Failed,
        message,
        seconds,
        spec_only_bls_disabled: false,
        spec_only_execution_mock: false,
        spec_only_data_availability_mock: false,
        spec_only_asserted_graph: false,
    }
}

struct Collector {
    results: Vec<Option<CaseResult>>,
    completed: usize,
    failures: usize,
    progress_every: usize,
    errors: Vec<String>,
}

impl Collector {
    fn new(total: usize, progress_every: usize) -> Self {
        Self {
            results: vec![None; total],
            completed: 0,
            failures: 0,
            progress_every,
            errors: Vec::new(),
        }
    }

    fn record(&mut self, index: usize, result: CaseResult) {
        let Some(slot) = self.results.get_mut(index) else {
            self.errors.push("fixture completion index exceeds original inventory".into());
            return;
        };
        if slot.is_some() {
            self.errors.push("fixture completion duplicates an original case index".into());
            return;
        }
        if result.outcome != Outcome::Passed {
            self.failures += 1;
            if self.progress_every != 0 {
                eprintln!(
                    "fixture outcome: {}: {:?}: {}",
                    result.path, result.outcome, result.message
                );
            }
        }
        *slot = Some(result);
        self.completed += 1;
        if self.progress_every != 0 && self.completed.is_multiple_of(self.progress_every) {
            eprintln!(
                "fixture progress: {}/{} cases; {} passed; {} failed or unsupported",
                self.completed,
                self.results.len(),
                self.completed - self.failures,
                self.failures
            );
        }
    }
}

#[cfg(test)]
mod tests;
