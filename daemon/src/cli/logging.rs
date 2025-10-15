#![warn(clippy::all, clippy::pedantic, clippy::cargo, clippy::nursery)]

use super::tokio_console;
use eyre::Result as EyreResult;
use std::{process::id as pid, thread::available_parallelism};
use structopt::StructOpt;
use tracing;
use tracing_subscriber::{filter, fmt, layer::SubscriberExt, Layer};
use users::{get_current_gid, get_current_uid};

#[derive(Debug, PartialEq, StructOpt)]
pub struct Options {
    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: usize,

    /// Apply an env_filter compatible log filter
    #[structopt(long, env, default_value)]
    log_filter: String,

    #[structopt(flatten)]
    pub tokio_console: tokio_console::Options,
}

impl Options {
    #[allow(clippy::borrow_as_ptr)] // ptr::addr_of! does not work here.
    pub fn init(&self) -> EyreResult<()> {
        // Log filtering is a combination of `--log-filter` and `--verbose` arguments.
        //let targets = log_filter.with_targets(verbosity);

        let journald_layer = tracing_journald::layer()?;
        let stdout_layer = fmt::Layer::default()
            .with_writer(std::io::stdout)
            .with_ansi(false)
            .with_filter(filter::LevelFilter::INFO);
        // Support server for tokio-console
        let console_layer = tokio_console::layer(&self.tokio_console);

        // Route events to both tokio-console and stdout
        let subscriber = tracing_subscriber::Registry::default()
            .with(journald_layer)
            .with(console_layer)
            .with(stdout_layer);

        tracing::subscriber::set_global_default(subscriber)?;

        // Log version information
        tracing::info!(
            host = env!("TARGET"),
            pid = pid(),
            uid = get_current_uid(),
            gid = get_current_gid(),
            cores = available_parallelism()?,
            main = &crate::main as *const _ as usize,
            commit = &env!("COMMIT_SHA")[..8],
            "{name} {version}",
            name = env!("CARGO_CRATE_NAME"),
            version = env!("CARGO_PKG_VERSION"),
        );

        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse_args() {
        let cmd = "arg0 -v --log-filter foo -vvv";
        let options = Options::from_iter_safe(cmd.split(' ')).unwrap();
        assert_eq!(
            options,
            Options {
                verbose: 4,
                log_filter: "foo".to_owned(),
                tokio_console: tokio_console::Options {
                    tokio_console: false,
                },
            }
        );
    }
}
