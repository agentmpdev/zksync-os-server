use super::models::{JobStatus, NonEmptyQueueStatistics, QueueStatistics};
use crate::prover_api::fri_job_manager::{FriJob, JobState};
use crate::prover_api::metrics::{PROVER_METRICS, ProverStage, ProverType};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{Mutex, Notify};
use zksync_os_l1_sender::batcher_model::{BatchMetadata, ProverInput, SignedBatchEnvelope};
use zksync_os_types::{ExecutionVersion, ProvingVersion};

#[derive(Debug)]
struct JobEntry {
    batch_envelope: SignedBatchEnvelope<ProverInput>,
    proving_version: ProvingVersion,
    status: JobStatus,
}

/// Concurrent map of prover jobs
/// Keys are batch numbers stored in a BTreeMap for ordered iteration.
/// Features:
///  * add_job - adds a new job
///     * blocks if adding this job would exceed max_assigned_batch_range until space is available
///     * O(log n)
///  * pick_job - picks the first job that is either pending or assigned and older than min_age
///     * currently, it iterates over all jobs and picks the first one that meets the criteria
///     * O(n)
///  * complete_job - marks a job as complete by removing it from the map
///     * O(log n)
///
/// Note that the current implementation uses async Mutex which is locked on each operation.
/// This may be problematic with a large number of in-progress jobs and a lot of polling -
/// but should be OK for hundreds of jobs/provers.
#[derive(Debug)]
pub struct ProverJobMap {
    // == state ==
    jobs: Mutex<BTreeMap<u64, JobEntry>>,
    // Notification for waiting when batch range limit is hit
    space_available: Notify,

    // == config ==
    // assigns to another prover if it takes longer than this
    assignment_timeout: Duration,
    // maximum allowed range between min and max batch numbers
    max_assigned_batch_range: usize,
}

impl ProverJobMap {
    pub fn new(assignment_timeout: Duration, max_assigned_batch_range: usize) -> Self {
        Self {
            jobs: Mutex::new(BTreeMap::new()),
            space_available: Notify::new(),
            assignment_timeout,
            max_assigned_batch_range,
        }
    }

    /// Adds a pending job to the map.
    /// Awaits if adding this job would exceed max_assigned_batch_range until space is available.
    pub async fn add_job(
        &self,
        batch_envelope: SignedBatchEnvelope<ProverInput>,
    )  {
        let batch_number = batch_envelope.batch_number();
        let mut jobs = self.jobs.lock().await;

        // Wait until there's space available (await if batch range limit would be exceeded)
        while self.is_queue_full(&jobs) {
            let queue_statistics = Self::queue_statistics(&jobs);

            tracing::info!(
                batch_number,
                ?queue_statistics,
                max_assigned_batch_range = self.max_assigned_batch_range,
                "Waiting for space in job map"
            );
            // Drop lock before awaiting notification
            drop(jobs);
            self.space_available.notified().await;
            // Re-acquire lock after notification
            jobs = self.jobs.lock().await;
        }

        let forward_run_execution_version =
            ExecutionVersion::try_from(batch_envelope.batch.execution_version)
                .expect("Must be valid execution as set by the server");
        let proving_version =
            ProvingVersion::from_forward_run_execution_version(forward_run_execution_version);

        let entry = JobEntry {
            batch_envelope,
            proving_version,
            status: JobStatus::new_pending(),
        };

        jobs.insert(batch_number, entry);

        tracing::info!(
            batch_number,
            queue_statistics = ?Self::queue_statistics(&jobs),
            "Job added"
        );
    }

    /// Picks the first job (lowest batch number) that is either:
    /// - Pending and older than min_age (fake provers use non-empty min_age)
    /// - Assigned and timed out
    ///
    /// Returns None if no eligible job is found.
    pub async fn pick_job(
        &self,
        min_age: Duration,
        prover_id: &'static str,
    ) -> Option<(FriJob, ProverInput)> {
        let now = Instant::now();
        let mut jobs = self.jobs.lock().await;

        // Find an eligible job
        let eligible_batch_number = jobs.iter().find_map(|(batch_number, entry)| {
            match entry.status.assigned_at {
                // Pending: check if job meets minimum age requirement
                None if now.duration_since(entry.status.added_at) >= min_age => Some(*batch_number),
                // Assigned: check if job has timed out
                Some(assigned_at) if now.duration_since(assigned_at) > self.assignment_timeout => {
                    Some(*batch_number)
                }
                _ => None,
            }
        })?;

        let queue_statistics = Self::queue_statistics(&jobs);
        // Update status and extract data
        let entry = jobs.get_mut(&eligible_batch_number).unwrap();
        entry.status.assign(now);

        let forward_run_execution_version =
            ExecutionVersion::try_from(entry.batch_envelope.batch.execution_version)
                .expect("Must be valid execution as set by the server");
        let proving_execution_version =
            ProvingVersion::from_forward_run_execution_version(forward_run_execution_version);

        let fri_job = FriJob {
            batch_number: eligible_batch_number,
            vk_hash: proving_execution_version.vk_hash().to_string(),
        };

        tracing::info!(
            batch_number = eligible_batch_number,
            prover_id,
            vk_hash = fri_job.vk_hash,
            current_attempt = entry.status.current_attempt,
            time_since_added = ?now.duration_since(entry.status.added_at),
            ?queue_statistics,
            "Job assigned"
        );

        Some((fri_job, entry.batch_envelope.data.clone()))
    }

    /// If a job is present for a given batch_number, returns the corresponding BatchMetadata
    pub async fn get_job(&self, batch_number: u64) -> Option<BatchMetadata> {
        let jobs = self.jobs.lock().await;
        jobs.get(&batch_number)
            .map(|entry| entry.batch_envelope.batch.clone())
    }

    /// If a job is present for given batch_number, returns (vk, prover_input)
    pub async fn get_batch_data(&self, batch_number: u64) -> Option<(&'static str, ProverInput)> {
        let jobs = self.jobs.lock().await;
        jobs.get(&batch_number).map(|entry| {
            (
                entry
                    .batch_envelope
                    .batch
                    .verification_key_hash()
                    .expect("VK hash must exist"),
                entry.batch_envelope.data.clone(),
            )
        })
    }

    /// Marks a job as complete by removing it from the map.
    /// Notifies inbound jobs waiting in add_job() that space may be available.
    /// Records metrics and logs timing info. Returns the batch envelope if the job existed.
    pub async fn complete_job(
        &self,
        batch_number: u64,
        prover_type: ProverType,
        prover_id: &'static str,
    ) -> Option<SignedBatchEnvelope<ProverInput>> {
        let mut jobs = self.jobs.lock().await;
        let entry = jobs.remove(&batch_number)?;

        let completion_stats = entry.status.completion_statistics();
        let batch_envelope = entry.batch_envelope;

        drop(jobs);
        self.space_available.notify_one();

        // Record Prometheus metrics
        PROVER_METRICS.prove_time[&(ProverStage::Fri, prover_type, prover_id)]
            .observe(completion_stats.prove_time);
        if batch_envelope.batch.tx_count > 0 {
            PROVER_METRICS.prove_time_per_tx[&(ProverStage::Fri, prover_type, prover_id)]
                .observe(completion_stats.prove_time / batch_envelope.batch.tx_count as u32);
        }
        PROVER_METRICS.proved_after_attempts[&(ProverStage::Fri, prover_type)]
            .observe(completion_stats.attempts_took as f64);

        tracing::info!(
            batch_number,
            ?completion_stats,
            ?prover_type,
            prover_id,
            "FRI Prover job completed and removed from map"
        );

        Some(batch_envelope)
    }

    /// Check if the queue is full (range between oldest and newest batch >= max_assigned_batch_range)
    fn is_queue_full(&self, jobs: &BTreeMap<u64, JobEntry>) -> bool {
        if let (Some(&min), Some(&max)) = (jobs.keys().next(), jobs.keys().next_back()) {
            max - min >= self.max_assigned_batch_range as u64
        } else {
            false
        }
    }

    fn queue_statistics(jobs: &BTreeMap<u64, JobEntry>) -> QueueStatistics {
        let min_batch = jobs.values().next();
        match min_batch {
            Some(min_batch) => QueueStatistics::NonEmpty(NonEmptyQueueStatistics {
                min_batch_added_at: min_batch.status.added_at,
                min_batch_current_attempt: min_batch.status.current_attempt,
                min_batch_number: min_batch.batch_envelope.batch_number(),
                max_batch_number: *jobs.keys().next_back().unwrap(),
                jobs_count: jobs.len(),
            }),
            None => QueueStatistics::Empty,
        }
    }

    pub async fn status(&self) -> Vec<JobState> {
        let jobs = self.jobs.lock().await;
        jobs.iter()
            .map(|(batch_number, entry)| JobState {
                fri_job: FriJob {
                    batch_number: *batch_number,
                    vk_hash: entry
                        .batch_envelope
                        .batch
                        .verification_key_hash()
                        .expect("VK hash must exist")
                        .to_string(),
                },
                assigned_seconds_ago: entry
                    .status
                    .assigned_at
                    .map(|assigned_at| assigned_at.elapsed().as_secs()),
                current_attempt: entry.status.current_attempt,
                added_seconds_ago: entry.status.added_at.elapsed().as_secs(),
            })
            .collect() // Already sorted by BTreeMap ordering
    }
}
