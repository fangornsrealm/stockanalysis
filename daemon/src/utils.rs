use eyre::Result as EyreResult;
use futures::FutureExt;
use std::future::Future;
use tokio::{spawn, task::JoinHandle};
use tracing::error;

#[macro_export]
macro_rules! require {
    ($condition:expr, $err:expr) => {
        if !$condition {
            return Err($err);
        }
    };
}

/// Spawn a task and abort process if it results in error.
/// Tasks must result in [`EyreResult<()>`]
pub fn spawn_or_abort<F>(future: F) -> JoinHandle<()>
where
    F: Future<Output = EyreResult<()>> + Send + 'static,
{
    spawn(future.map(|result| {
        if let Err(error) = result {
            // Log error
            error!(?error, "Error in task");
            // Abort process
            std::process::abort();
        }
    }))
}
