use std::collections::HashMap;

use alloy::primitives::ChainId;
use alloy::rpc::types::{Log, Topic, ValueOrArray};
use alloy::sol_types::SolEvent;
use alloy::{primitives::Address, providers::DynProvider};
use zksync_os_contract_interface::IMessageRoot::{AppendedChainRoot, NewInteropRoot};
use zksync_os_contract_interface::{Bridgehub, InteropRoot};
use zksync_os_mempool::subpools::interop_roots::InteropRootsSubpool;
use zksync_os_types::{IndexedInteropRoot, InteropRootsLogIndex};

use crate::watcher::{L1Watcher, L1WatcherError};
use crate::{L1WatcherConfig, ProcessRawEvents};

pub struct InteropWatcher {
    contract_address: Address,
    chain_id: ChainId,
    starting_interop_event_index: InteropRootsLogIndex,
    interop_roots_subpool: InteropRootsSubpool,
}

impl InteropWatcher {
    pub async fn create_watcher(
        bridgehub: Bridgehub<DynProvider>,
        config: L1WatcherConfig,
        starting_interop_event_index: InteropRootsLogIndex,
        interop_roots_subpool: InteropRootsSubpool,
    ) -> anyhow::Result<L1Watcher> {
        let contract_address = bridgehub.message_root_address().await?;

        tracing::info!(
            contract_address = ?contract_address,
            starting_interop_event_index = ?starting_interop_event_index,
            "initializing interop watcher"
        );

        let this = Self {
            contract_address,
            chain_id: bridgehub.l2_chain_id,
            starting_interop_event_index,
            interop_roots_subpool,
        };

        let l1_watcher = L1Watcher::new(
            bridgehub.provider().clone(),
            this.starting_interop_event_index.block_number,
            config.max_blocks_to_process,
            config.poll_interval,
            Box::new(this),
        );

        Ok(l1_watcher)
    }
}

#[async_trait::async_trait]
impl ProcessRawEvents for InteropWatcher {
    fn name(&self) -> &'static str {
        "interop_root"
    }

    fn event_signatures(&self) -> Topic {
        Topic::from(NewInteropRoot::SIGNATURE_HASH).extend(AppendedChainRoot::SIGNATURE_HASH)
    }

    fn contract_addresses(&self) -> ValueOrArray<Address> {
        self.contract_address.into()
    }

    fn filter_events(&self, logs: Vec<Log>) -> Vec<Log> {
        // we want to accept only the latest event for each log id
        let mut indexes = HashMap::new();

        for log in logs {
            if log.topic0() == Some(&NewInteropRoot::SIGNATURE_HASH) {
                let sol_event = NewInteropRoot::decode_log(&log.inner)
                    .expect("failed to decode log")
                    .data;
                indexes.insert(sol_event.blockNumber, log);
            } else if log.topic0() == Some(&AppendedChainRoot::SIGNATURE_HASH) {
                let sol_event = AppendedChainRoot::decode_log(&log.inner)
                    .expect("failed to decode log")
                    .data;
                indexes.insert(sol_event.batchNumber, log);
            } else {
                panic!("unexpected log type");
            }
        }

        indexes.into_values().collect()
    }

    async fn process_raw_event(&mut self, log: Log) -> Result<(), L1WatcherError> {
        let event_log_index = InteropRootsLogIndex {
            block_number: log.block_number.unwrap(),
            index_in_block: log.log_index.unwrap(),
        };
        let interop_root = if log.topic0() == Some(&NewInteropRoot::SIGNATURE_HASH) {
            let event = NewInteropRoot::decode_log(&log.inner)?.data;
            if event_log_index < self.starting_interop_event_index {
                tracing::debug!(
                    log_id = ?event.logId,
                    starting_interop_event_index = ?self.starting_interop_event_index,
                    "skipping interop root event before starting index",
                );
                return Ok(());
            }
            InteropRoot {
                chainId: event.chainId,
                blockOrBatchNumber: event.blockNumber,
                sides: event.sides.clone(),
            }
        } else if log.topic0() == Some(&AppendedChainRoot::SIGNATURE_HASH) {
            let event = AppendedChainRoot::decode_log(&log.inner)?.data;
            if event_log_index < self.starting_interop_event_index {
                tracing::debug!(
                    starting_interop_event_index = ?self.starting_interop_event_index,
                    "skipping interop root event before starting index",
                );
                return Ok(());
            }
            InteropRoot {
                chainId: event.chainId,
                blockOrBatchNumber: event.batchNumber,
                sides: vec![event.chainRoot],
            }
        } else {
            panic!("unexpected log type");
        };
        if interop_root.chainId == self.chain_id {
            tracing::info!("skipping own root import");
            return Ok(());
        }

        tracing::info!(
            "discovered interop root: chain_id={}, block_number={}, root={:?}",
            interop_root.chainId,
            interop_root.blockOrBatchNumber,
            interop_root.sides,
        );

        self.interop_roots_subpool
            .add_root(IndexedInteropRoot {
                log_index: event_log_index,
                root: interop_root,
            })
            .await;
        Ok(())
    }
}
