use std::marker::PhantomData;

use crate::transaction::{system::envelope::SystemTransactionEnvelope, tx::SystemTransaction};
use alloy::primitives::{Address, Bytes, address};
use alloy::sol_types::SolCall;
use alloy_rlp::{BufMut, Decodable, Encodable};
use serde::{Deserialize, Serialize};
use zksync_os_contract_interface::{IMessageRoot::addInteropRootCall, InteropRoot};
//use zksync_os_contract_interface::IMessageRoot::addInteropRootsInBatchCall;

pub mod envelope;
pub mod tx;

pub const BOOTLOADER_FORMAL_ADDRESS: Address =
    address!("0x0000000000000000000000000000000000008001");
pub const L2_INTEROP_ROOT_STORAGE_ZKSYNC_OS_ADDRESS: Address =
    address!("0x0000000000000000000000000000000000010008");

const DEFAULT_GAS_LIMIT: u64 = 72_000_000;

pub type InteropRootsEnvelope = SystemTransactionEnvelope<InteropRootsTxType>;

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq, Default, PartialOrd)]
pub struct InteropRootsLogIndex {
    pub block_number: u64,
    pub log_index: u64,
}

impl InteropRootsLogIndex {
    pub fn increment_log_index(&mut self) {
        self.log_index += 1;
    }
}

impl Encodable for InteropRootsLogIndex {
    fn encode(&self, out: &mut dyn BufMut) {
        vec![self.block_number, self.log_index].encode(out);
    }

    fn length(&self) -> usize {
        vec![self.block_number, self.log_index].length()
    }
}

impl Decodable for InteropRootsLogIndex {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let vec: Vec<u64> = Vec::decode(buf)?;
        let array: [u64; 2] = vec
            .try_into()
            .map_err(|_| alloy::rlp::Error::Custom("expected array of length 2"))?;
        Ok(Self {
            block_number: array[0],
            log_index: array[1],
        })
    }
}

impl InteropRootsEnvelope {
    pub fn from_interop_root(
        interop_root: InteropRoot,
        last_event_log_index: InteropRootsLogIndex,
    ) -> Self {
        let calldata = addInteropRootCall {
            chainId: interop_root.chainId,
            blockOrBatchNumber: interop_root.blockOrBatchNumber,
            sides: interop_root.sides,
        }
        .abi_encode();

        let transaction = SystemTransaction {
            // todo: set some real value maybe?
            gas_limit: DEFAULT_GAS_LIMIT,
            to: L2_INTEROP_ROOT_STORAGE_ZKSYNC_OS_ADDRESS,
            input: Bytes::from(calldata),
            marker: PhantomData,
        };

        Self {
            hash: transaction.calculate_hash(),
            event_log_index: last_event_log_index,
            inner: transaction,
        }
    }

    pub fn interop_roots_count(&self) -> u64 {
        1
    }

    // pub fn from_interop_roots(interop_roots: Vec<InteropRoot>) -> Self {
    //     let calldata = addInteropRootsInBatchCall {
    //         interopRootsInput: interop_roots,
    //     }
    //     .abi_encode();

    //     let transaction = SystemTransaction {
    //         gas_limit: 0,
    //         to: L2_INTEROP_ROOT_STORAGE_ZKSYNC_OS_ADDRESS,
    //         input: Bytes::from(calldata),
    //         marker: PhantomData,
    //     };

    //     Self {
    //         hash: transaction.calculate_hash(),
    //         inner: transaction,
    //     }
    // }

    // pub fn interop_roots_count(&self) -> u64 {
    //     let interop_roots = addInteropRootsInBatchCall::abi_decode(&self.inner.input)
    //         .expect("Failed to decode interop roots calldata")
    //         .interopRootsInput;
    //     interop_roots.len() as u64
    // }
}

pub trait SystemTxType: Clone + Send + Sync + std::fmt::Debug + 'static {
    const TX_TYPE: u8;
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct InteropRootsTxType;

impl SystemTxType for InteropRootsTxType {
    const TX_TYPE: u8 = 0x7d;
}
