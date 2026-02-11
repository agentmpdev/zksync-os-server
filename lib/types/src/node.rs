use serde::{Deserialize, Serialize};

/// A node's role in the network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    MainNode,
    ExternalNode,
}

impl NodeRole {
    pub fn is_main(&self) -> bool {
        self == &NodeRole::MainNode
    }

    pub fn is_external(&self) -> bool {
        self == &NodeRole::ExternalNode
    }
}
