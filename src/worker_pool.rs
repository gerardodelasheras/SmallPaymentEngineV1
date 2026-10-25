use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};
use tracing::{error, trace, warn};

use crate::account::account::Account;
use crate::client::client_id::ClientId;
use crate::processing_error::ProcessingError;
use crate::transaction::transaction::Transaction;
use crate::transaction::transaction_processor::TransactionProcessor;

/// Bounded per-worker queue capacity.
const QUEUE_CAPACITY: usize = 256;

/// Upper bound on worker tasks, keeping in-flight work bounded.
const MAX_WORKERS: usize = 16;

enum Command {
    Process(Transaction),
    TakeAccounts(oneshot::Sender<HashMap<ClientId, Account>>),
}

/// A fixed pool of workers, each owning a disjoint set of clients.
pub(crate) struct WorkerPool {
    senders: Vec<mpsc::Sender<Command>>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl WorkerPool {
    /// Creates a pool sized to the available parallelism, clamped for bounded
    /// in-flight work.
    pub(crate) fn new() -> Self {
        let workers = std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1)
            .clamp(1, MAX_WORKERS);
        Self::with_worker_count(workers)
    }

    pub(crate) fn with_worker_count(workers: usize) -> Self {
        assert!(workers > 0, "at least one worker is required");
        let mut senders = Vec::with_capacity(workers);
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
            senders.push(sender);
            handles.push(tokio::spawn(run_worker(receiver)));
        }
        Self { senders, handles }
    }

    pub(crate) fn worker_count(&self) -> usize {
        self.senders.len()
    }

    pub(crate) fn capacities(&self) -> Vec<usize> {
        self.senders.iter().map(mpsc::Sender::capacity).collect()
    }

    /// Blocks the calling non-async thread when the worker queue is full.
    pub(crate) fn dispatch_to(
        &self,
        worker: usize,
        transaction: Transaction,
    ) -> Result<(), ProcessingError> {
        self.senders[worker]
            .blocking_send(Command::Process(transaction))
            .map_err(|_| ProcessingError::WorkerUnavailable("worker channel closed".to_string()))
    }

    /// Takes every worker's accounts, then closes the senders and joins the
    /// workers. Cleanup runs even when an earlier step fails.
    pub(crate) async fn take_accounts(
        mut self,
    ) -> Result<HashMap<ClientId, Account>, ProcessingError> {
        let mut merged = HashMap::new();
        let mut first_error = None;

        for sender in &self.senders {
            let (reply, receiver) = oneshot::channel();
            match sender.send(Command::TakeAccounts(reply)).await {
                Ok(()) => match receiver.await {
                    Ok(accounts) => merged.extend(accounts),
                    Err(_) => {
                        first_error.get_or_insert_with(|| {
                            ProcessingError::WorkerUnavailable(
                                "take-accounts reply dropped".to_string(),
                            )
                        });
                    }
                },
                Err(_) => {
                    first_error.get_or_insert_with(|| {
                        ProcessingError::WorkerUnavailable(
                            "take-accounts request failed".to_string(),
                        )
                    });
                }
            }
        }

        self.senders.clear();

        for handle in self.handles.drain(..) {
            if handle.await.is_err() {
                first_error.get_or_insert_with(|| {
                    ProcessingError::WorkerUnavailable("worker task panicked".to_string())
                });
            }
        }

        first_error.map_or(Ok(merged), Err)
    }
}

async fn run_worker(mut receiver: mpsc::Receiver<Command>) {
    let mut processor = TransactionProcessor::new();
    while let Some(command) = receiver.recv().await {
        match command {
            Command::Process(transaction) => {
                if let Err(problem) = processor.process(&transaction) {
                    match problem {
                        error @ ProcessingError::DisputeExceedsAvailable { .. } => {
                            warn!(
                                client = transaction.client,
                                transaction_id = transaction.transaction_id,
                                error = %error,
                                "client {} dispute exceeds available; account locked",
                                transaction.client
                            );
                        }
                        problem if problem.is_recoverable() => {
                            trace!(error = %problem, "skipped transaction row");
                        }
                        problem => {
                            error!(error = %problem, "worker infrastructure error");
                        }
                    }
                }
            }
            Command::TakeAccounts(reply) => {
                let _ = reply.send(processor.take_accounts());
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amount::Amount;
    use crate::transaction::transaction_type::TransactionType;

    fn deposit(client: ClientId, id: u32, value: &str) -> Transaction {
        Transaction {
            transaction_type: TransactionType::Deposit,
            client,
            transaction_id: id,
            amount: Some(Amount::parse(value).expect("valid amount")),
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
    }

    #[test]
    fn pool_reports_worker_count_and_capacity() {
        let runtime = runtime();
        let pool = {
            let _guard = runtime.enter();
            WorkerPool::with_worker_count(3)
        };

        assert_eq!(pool.worker_count(), 3);
        assert_eq!(pool.capacities(), vec![QUEUE_CAPACITY; 3]);

        let accounts = runtime.block_on(pool.take_accounts()).expect("pool drains");
        assert!(accounts.is_empty());
    }

    #[test]
    fn dispatch_and_drain_return_processed_accounts() {
        let runtime = runtime();
        let pool = {
            let _guard = runtime.enter();
            WorkerPool::with_worker_count(1)
        };

        pool.dispatch_to(0, deposit(1, 1, "10.0"))
            .expect("dispatch succeeds");

        let accounts = runtime.block_on(pool.take_accounts()).expect("pool drains");
        assert_eq!(accounts[&1].available.to_string(), "10.0");
    }

    #[test]
    fn a_rejected_row_does_not_stop_the_worker() {
        let runtime = runtime();
        let pool = {
            let _guard = runtime.enter();
            WorkerPool::with_worker_count(1)
        };

        pool.dispatch_to(0, deposit(1, 1, "5.0"))
            .expect("dispatch succeeds");
        pool.dispatch_to(
            0,
            Transaction {
                transaction_type: TransactionType::Withdrawal,
                client: 1,
                transaction_id: 2,
                amount: Some(Amount::parse("100.0").expect("valid amount")),
            },
        )
        .expect("dispatch succeeds");
        pool.dispatch_to(0, deposit(1, 3, "7.0"))
            .expect("dispatch succeeds");

        let accounts = runtime.block_on(pool.take_accounts()).expect("pool drains");
        assert_eq!(accounts[&1].available.to_string(), "12.0");
    }

    #[test]
    fn a_closed_channel_is_reported_on_dispatch_and_drain() {
        let (sender, receiver) = mpsc::channel::<Command>(1);
        drop(receiver);
        let pool = WorkerPool {
            senders: vec![sender],
            handles: Vec::new(),
        };

        assert!(matches!(
            pool.dispatch_to(0, deposit(1, 1, "1.0")),
            Err(ProcessingError::WorkerUnavailable(_))
        ));

        let result = runtime().block_on(pool.take_accounts());
        assert!(matches!(result, Err(ProcessingError::WorkerUnavailable(_))));
    }

    #[test]
    fn a_full_queue_blocks_dispatch_until_capacity_is_available() {
        let (sender, mut receiver) = mpsc::channel::<Command>(1);
        sender
            .try_send(Command::Process(deposit(1, 1, "1.0")))
            .expect("queue has capacity");
        let pool = WorkerPool {
            senders: vec![sender],
            handles: Vec::new(),
        };

        let (started, started_receiver) = std::sync::mpsc::channel();
        let dispatch = std::thread::spawn(move || {
            started.send(()).expect("signal start");
            pool.dispatch_to(0, deposit(1, 2, "1.0"))
        });

        started_receiver.recv().expect("dispatch thread started");
        assert!(
            !dispatch.is_finished(),
            "dispatch should block while the queue is full"
        );

        let _ = receiver.blocking_recv().expect("queued command");
        assert!(dispatch.join().expect("dispatch thread panicked").is_ok());
    }

    #[tokio::test]
    async fn take_accounts_reports_a_panicking_worker() {
        let (sender, mut receiver) = mpsc::channel::<Command>(1);
        let handle = tokio::spawn(async move {
            if let Some(Command::TakeAccounts(reply)) = receiver.recv().await {
                let _ = reply.send(HashMap::new());
            }
            panic!("worker panicked after replying");
        });
        let pool = WorkerPool {
            senders: vec![sender],
            handles: vec![handle],
        };

        let result = pool.take_accounts().await;

        assert!(matches!(result, Err(ProcessingError::WorkerUnavailable(_))));
    }
}
