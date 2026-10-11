//! Abstractions for groups of tests.

use crate::{
    case::Case,
    result::{assert_tests_pass, CaseResult},
};
use rayon::ThreadPoolBuilder;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// A collection of tests.
pub trait Suite {
    /// The type of test cases in this suite.
    type Case: Case;

    /// The path to the test suite directory.
    fn suite_path(&self) -> &Path;

    /// Run every fixture, including files directly inside the suite directory.
    fn run(&self) {
        self.run_only(".");
    }

    /// Recursively load and run every JSON fixture in the provided sub-folder.
    fn run_only(&self, name: &str) {
        let suite_path = self.suite_path().join(name);
        assert!(suite_path.is_dir(), "Test suite directory does not exist: {suite_path:?}");
        let paths = find_all_files_with_extension(&suite_path, "json");
        assert!(!paths.is_empty(), "Test suite contains no JSON fixtures: {suite_path:?}");

        // A fixture can itself execute cases in parallel. Loading files sequentially prevents
        // nested work-stealing from retaining multiple large decoded fixture files on the stack.
        let workers = std::env::var("RETH_EF_TEST_THREADS")
            .ok()
            .map(|value| {
                value.parse::<usize>().expect("RETH_EF_TEST_THREADS must be a positive integer")
            })
            .unwrap_or(2);
        assert!((1..=8).contains(&workers), "execution fixture workers must be between 1 and 8");
        let pool = ThreadPoolBuilder::new()
            .num_threads(workers)
            .stack_size(16 * 1024 * 1024)
            .build()
            .expect("build bounded fixture worker pool");
        let total_files = paths.len();
        let results = pool.install(|| {
            paths
                .into_iter()
                .enumerate()
                .map(|(index, path)| {
                    let result = match Self::Case::load(&path) {
                        Ok(case) => CaseResult::new(&path, case.description(), case.run()),
                        Err(error) => CaseResult::new(&path, "fixture loading".into(), Err(error)),
                    };
                    if let Err(error) = &result.result {
                        if !matches!(error, crate::Error::Skipped) {
                            eprintln!("[!] {}: {error}", path.display());
                        }
                    }
                    if (index + 1) % 100 == 0 || index + 1 == total_files {
                        println!(
                            "Execution fixture progress: {}/{total_files} files executed",
                            index + 1
                        );
                    }
                    result
                })
                .collect::<Vec<_>>()
        });
        assert_tests_pass(name, &suite_path, &results);
    }
}

/// Recursively find regular files with the exact extension, preserving traversal failures.
fn find_all_files_with_extension(path: &Path, extension: &str) -> Vec<PathBuf> {
    let mut paths = WalkDir::new(path)
        .into_iter()
        .map(|entry| {
            entry.unwrap_or_else(|error| panic!("Failed to traverse fixture directory: {error}"))
        })
        .filter(|entry| {
            entry.file_type().is_file() &&
                entry.path().extension().is_some_and(|ext| ext == extension)
        })
        .map(|entry| entry.into_path())
        .collect::<Vec<_>>();
    paths.sort_unstable();
    paths
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static EXECUTED: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug)]
    struct Fixture;

    impl Case for Fixture {
        fn load(path: &Path) -> Result<Self, Error> {
            let contents = std::fs::read_to_string(path)
                .map_err(|error| Error::Io { path: path.into(), error })?;
            serde_json::from_str::<serde_json::Value>(&contents)
                .map_err(|error| Error::CouldNotDeserialize { path: path.into(), error })?;
            Ok(Self)
        }

        fn run(self) -> Result<(), Error> {
            EXECUTED.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct Fixtures(PathBuf);

    impl Suite for Fixtures {
        type Case = Fixture;
        fn suite_path(&self) -> &Path {
            &self.0
        }
    }

    #[test]
    fn root_and_nested_files_are_executed_without_loading_metadata() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("nested")).unwrap();
        std::fs::write(directory.path().join("root.json"), "{}").unwrap();
        std::fs::write(directory.path().join("nested/case.json"), "{}").unwrap();
        std::fs::write(directory.path().join("ignored.json.bak"), "invalid").unwrap();
        std::fs::create_dir(directory.path().join("directory.json")).unwrap();
        let before = EXECUTED.load(Ordering::SeqCst);
        Fixtures(directory.path().into()).run();
        assert_eq!(EXECUTED.load(Ordering::SeqCst) - before, 2);
    }

    #[test]
    #[should_panic(expected = "Test suite directory does not exist")]
    fn missing_fixtures_fail() {
        let directory = tempfile::tempdir().unwrap();
        Fixtures(directory.path().join("missing")).run();
    }

    #[test]
    #[should_panic(expected = "Test suite contains no JSON fixtures")]
    fn empty_fixtures_fail() {
        let directory = tempfile::tempdir().unwrap();
        Fixtures(directory.path().into()).run();
    }

    #[test]
    #[should_panic(expected = "Some tests failed")]
    fn malformed_fixtures_fail() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("malformed.json"), "{").unwrap();
        Fixtures(directory.path().into()).run();
    }
}
