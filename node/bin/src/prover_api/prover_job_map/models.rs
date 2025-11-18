use std::fmt::Debug;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub struct JobStatus {
    pub added_at: Instant,
    pub assigned_at: Option<Instant>,
    pub current_attempt: usize, // 0 = never assigned, 1+ = assigned N times
}

#[derive(Clone, Copy, Debug)]
pub struct CompletionStatistics {
    /// Number of attempts it took to finally prove. For normal execution, should be `1`
    pub attempts_took: usize,
    /// Time since last assignment
    pub prove_time: Duration,
    /// Total time spent in Prover Job Map
    #[allow(dead_code)]
    pub total_time: Duration,
}

pub enum QueueStatistics {
    Empty,
    NonEmpty(NonEmptyQueueStatistics),
}

pub struct NonEmptyQueueStatistics {
    pub min_batch_added_at: Instant,
    pub min_batch_current_attempt: usize,
    pub min_batch_number: u64,
    pub max_batch_number: u64,
    pub jobs_count: usize,
}

impl Debug for QueueStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueStatistics::Empty => write!(f, "Empty queue"),
            QueueStatistics::NonEmpty(stats) => write!(
                f,
                "Queue has {} jobs, range: {} - {}, oldest job added {:?} ago and has {} attempts",
                stats.jobs_count,
                stats.min_batch_number,
                stats.max_batch_number,
                stats.min_batch_added_at.elapsed(),
                stats.min_batch_current_attempt
            ),
        }
    }
}

impl JobStatus {
    pub fn new_pending() -> Self {
        Self {
            added_at: Instant::now(),
            assigned_at: None,
            current_attempt: 0,
        }
    }

    /// Assign (or reassign) this job to a prover.
    pub fn assign(&mut self, assigned_at: Instant) {
        self.assigned_at = Some(assigned_at);
        self.current_attempt += 1;
    }

    pub fn completion_statistics(&self) -> CompletionStatistics {
        CompletionStatistics {
            attempts_took: self.current_attempt,
            prove_time: self
                .assigned_at
                .map_or_else(Duration::default, |assigned_at| assigned_at.elapsed()),
            total_time: self.added_at.elapsed(),
        }
    }
}
