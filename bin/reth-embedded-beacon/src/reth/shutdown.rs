//! Bounded component joins preserve the original launcher failure and finish both cleanup paths.

use std::{future::Future, time::Duration};

/// Join the outstanding consensus and API owners after their cancellation signals were sent.
/// Both joins finish, even if one fails; an original runtime error keeps its complete context.
pub(super) async fn finish<C, A>(
    primary: eyre::Result<()>,
    consensus: Option<C>,
    api: Option<A>,
    deadline: Duration,
) -> eyre::Result<()>
where
    C: Future<Output = eyre::Result<()>>,
    A: Future<Output = eyre::Result<()>>,
{
    let (consensus, api) = tokio::join!(
        wait_for("consensus", consensus, deadline),
        wait_for("beacon API", api, deadline),
    );
    let mut result = primary;
    for (component, cleanup) in [("consensus", consensus), ("beacon API", api)] {
        if let Err(error) = cleanup {
            if result.is_err() {
                tracing::warn!(target: "beacon::node", component, error = %error,
                    "[beacon] Additional launcher shutdown failure");
            } else {
                result = Err(error);
            }
        }
    }
    result
}

async fn wait_for<F>(
    component: &'static str,
    task: Option<F>,
    deadline: Duration,
) -> eyre::Result<()>
where
    F: Future<Output = eyre::Result<()>>,
{
    let Some(task) = task else { return Ok(()) };
    tokio::time::timeout(deadline, task)
        .await
        .map_err(|_| eyre::eyre!("{component} shutdown timed out"))?
        .map_err(|error| error.wrap_err(format!("{component} shutdown failed")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::{pending, ready, Ready},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    #[tokio::test]
    async fn primary_failure_survives_and_both_cleanup_paths_are_joined() {
        let completed = Arc::new(AtomicUsize::new(0));
        let consensus = completed.clone();
        let api = completed.clone();
        let result = finish(
            Err(eyre::eyre!("original execution acknowledgement failed")),
            Some(async move {
                consensus.fetch_add(1, Ordering::SeqCst);
                Err(eyre::eyre!("consensus cleanup failed"))
            }),
            Some(async move {
                api.fetch_add(1, Ordering::SeqCst);
                Err(eyre::eyre!("API cleanup failed"))
            }),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(completed.load(Ordering::SeqCst), 2);
        assert_eq!(result.unwrap_err().to_string(), "original execution acknowledgement failed");
    }

    #[tokio::test]
    async fn clean_runtime_still_reports_cleanup_failure_after_joining_api() {
        let completed = Arc::new(AtomicUsize::new(0));
        let api = completed.clone();
        let result = finish(
            Ok(()),
            Some(ready(Err(eyre::eyre!("consensus teardown failed")))),
            Some(async move {
                api.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert_eq!(result.unwrap_err().to_string(), "consensus shutdown failed");
    }

    #[tokio::test]
    async fn disabled_or_already_finished_components_require_no_join() {
        finish(
            Ok(()),
            None::<Ready<eyre::Result<()>>>,
            None::<Ready<eyre::Result<()>>>,
            Duration::ZERO,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stalled_component_deadline_is_bounded_and_identifies_component() {
        let result = finish(
            Ok(()),
            Some(pending::<eyre::Result<()>>()),
            Some(ready(Ok(()))),
            Duration::ZERO,
        )
        .await;
        assert_eq!(result.unwrap_err().to_string(), "consensus shutdown timed out");
    }
}
