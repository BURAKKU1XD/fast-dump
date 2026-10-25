use std::time::{Duration, Instant};

use anyhow::Result;

use crate::cli::Cli;
use crate::network::is_retryable_error;
use crate::progress::RetrySnapshot;
use crate::runtime::RunControl;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetryPolicy {
    pub(crate) retries: u32,
    pub(crate) delay: Duration,
}

impl RetryPolicy {
    pub(crate) fn from_cli(cli: &Cli, gui: bool) -> Self {
        Self::from_options(cli.retries, cli.retry_delay_secs, gui)
    }

    pub(crate) fn from_options(retries: Option<u32>, retry_delay_secs: u32, gui: bool) -> Self {
        Self {
            retries: retries.unwrap_or(if gui { 10 } else { 0 }),
            delay: Duration::from_secs(u64::from(retry_delay_secs)),
        }
    }
}

pub(crate) fn run(
    policy: RetryPolicy,
    control: &RunControl,
    mut attempt: impl FnMut() -> Result<()>,
) -> Result<()> {
    let mut retries = 0;
    loop {
        control.check()?;
        // Возврат из попытки означает, что сокеты, reader и producer уже освобождены.
        let error = match attempt() {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        control.check()?;
        if retries == policy.retries || !is_retryable_error(&error) {
            return Err(error);
        }
        retries += 1;
        let message = format!("{error:#}");
        control.log(
            log::Level::Warn,
            &format!(
                "Network error: {message}. Retry {retries}/{} in {} s; Ctrl+C stops retries.",
                policy.retries,
                policy.delay.as_secs()
            ),
        );
        if let Some(hub) = control.progress() {
            hub.set_retry(Some(RetrySnapshot {
                number: retries,
                limit: policy.retries,
                deadline: Instant::now() + policy.delay,
                error: message,
            }));
        }
        let wait_result = control.wait_for_retry(policy.delay);
        if let Some(hub) = control.progress() {
            hub.set_retry(None);
        }
        wait_result?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::network_io;
    use crate::progress::ProgressHub;
    use crate::runtime::Cancelled;
    use std::io;
    use std::sync::{Arc, mpsc};

    fn disconnected() -> anyhow::Error {
        network_io(io::Error::from(io::ErrorKind::ConnectionReset))
    }

    fn immediate(retries: u32) -> RetryPolicy {
        RetryPolicy {
            retries,
            delay: Duration::ZERO,
        }
    }

    #[test]
    fn defaults_and_explicit_overrides() {
        assert_eq!(
            RetryPolicy::from_options(None, 10, false),
            RetryPolicy {
                retries: 0,
                delay: Duration::from_secs(10)
            }
        );
        assert_eq!(RetryPolicy::from_options(None, 10, true).retries, 10);
        for gui in [false, true] {
            assert_eq!(RetryPolicy::from_options(Some(0), 7, gui).retries, 0);
            assert_eq!(
                RetryPolicy::from_options(Some(3), 7, gui),
                RetryPolicy {
                    retries: 3,
                    delay: Duration::from_secs(7)
                }
            );
        }
    }

    #[test]
    fn budget_counts_extra_attempts_and_preserves_last_error() {
        for retries in [0, 1, 10] {
            let mut attempts = 0;
            let error = run(immediate(retries), &RunControl::new(None), || {
                attempts += 1;
                Err(disconnected().context(format!("attempt {attempts}")))
            })
            .unwrap_err();
            assert_eq!(attempts, retries + 1);
            assert_eq!(error.to_string(), format!("attempt {attempts}"));
        }
    }

    #[test]
    fn success_stops_retrying() {
        let mut attempts = 0;
        run(immediate(10), &RunControl::new(None), || {
            attempts += 1;
            if attempts < 3 {
                Err(disconnected())
            } else {
                Ok(())
            }
        })
        .unwrap();
        assert_eq!(attempts, 3);
    }

    #[test]
    fn local_io_and_protocol_errors_are_not_retried() {
        for error in [
            anyhow::anyhow!("receiver rejected dump"),
            io::Error::from(io::ErrorKind::UnexpectedEof).into(),
            io::Error::from(io::ErrorKind::BrokenPipe).into(),
            Cancelled.into(),
        ] {
            let mut error = Some(error);
            let mut attempts = 0;
            assert!(
                run(immediate(10), &RunControl::new(None), || {
                    attempts += 1;
                    Err(error.take().expect("must not repeat permanent failure"))
                })
                .is_err()
            );
            assert_eq!(attempts, 1);
        }
    }

    #[test]
    fn cancellation_interrupts_wait_without_starting_another_attempt() {
        let hub = Arc::new(ProgressHub::new());
        let control = RunControl::new(Some(hub.clone()));
        let worker_control = control.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut attempts = 0;
            let result = run(
                RetryPolicy {
                    retries: 10,
                    delay: Duration::from_secs(10),
                },
                &worker_control,
                || {
                    attempts += 1;
                    Err(disconnected())
                },
            );
            done_tx.send((attempts, result)).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        while hub.retry().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        let was_waiting = hub.retry().is_some();
        control.cancel();
        let (attempts, result) = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        worker.join().unwrap();
        assert!(was_waiting);
        assert_eq!(attempts, 1);
        assert!(result.unwrap_err().is::<Cancelled>());
        assert!(hub.retry().is_none());
    }

    #[test]
    fn completed_attempt_wins_a_concurrent_cancel() {
        let control = RunControl::new(None);
        assert!(
            run(immediate(10), &control, || {
                control.cancel();
                Ok(())
            })
            .is_ok()
        );
    }
}
