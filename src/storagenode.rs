//! A storage node: a content-addressed local store plus a message handler.
//!
//! Durability model: the local store is durable. A crash takes the node offline
//! (it stops sending and receiving) but does not lose bytes that were already
//! stored. Recovery brings the node back with its store intact. Volatile state
//! is only what is in flight on the network, which the simulator drops.

use crate::hash::{sha256, Hash};
use crate::message::{Message, NodeId};
use std::collections::BTreeMap;

/// A single storage node identified by index.
pub struct StorageNode {
    /// Node index within the cluster.
    pub idx: u32,
    store: BTreeMap<Hash, Vec<u8>>,
}

impl StorageNode {
    /// Create an empty storage node.
    pub fn new(idx: u32) -> Self {
        StorageNode {
            idx,
            store: BTreeMap::new(),
        }
    }

    /// Whether this node holds a chunk.
    pub fn has(&self, id: &Hash) -> bool {
        self.store.contains_key(id)
    }

    /// Number of distinct chunks held.
    pub fn count(&self) -> usize {
        self.store.len()
    }

    /// All held chunk ids in sorted order.
    pub fn chunk_ids(&self) -> Vec<Hash> {
        self.store.keys().copied().collect()
    }

    /// Store a chunk only if its bytes match its id. Rejecting a mismatch keeps
    /// corrupt bytes from ever entering the store.
    fn put(&mut self, id: Hash, data: Vec<u8>) {
        if sha256(&data) == id {
            self.store.insert(id, data);
        }
    }

    /// Handle one message and return the messages to send in response, each as
    /// (from, to, message) with `from` set to this node.
    pub fn handle(&mut self, from: NodeId, msg: Message) -> Vec<(NodeId, NodeId, Message)> {
        let me = NodeId::Storage(self.idx);
        match msg {
            Message::StoreChunk { id, data } => {
                self.put(id, data);
                if self.has(&id) {
                    vec![(me, from, Message::StoreAck { id })]
                } else {
                    Vec::new()
                }
            }
            Message::FetchChunk { id } => {
                let data = self.store.get(&id).cloned();
                vec![(me, from, Message::ChunkData { id, data })]
            }
            Message::Replicate { id, data } => {
                self.put(id, data);
                Vec::new()
            }
            Message::ReplicateOrder { id, dest } => {
                if let Some(data) = self.store.get(&id).cloned() {
                    vec![(me, NodeId::Storage(dest), Message::Replicate { id, data })]
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_and_fetch() {
        let mut n = StorageNode::new(0);
        let data = vec![1u8, 2, 3];
        let id = sha256(&data);
        let out = n.handle(NodeId::Client, Message::StoreChunk { id, data: data.clone() });
        assert_eq!(out, vec![(NodeId::Storage(0), NodeId::Client, Message::StoreAck { id })]);
        assert!(n.has(&id));

        let out = n.handle(NodeId::Client, Message::FetchChunk { id });
        assert_eq!(
            out,
            vec![(
                NodeId::Storage(0),
                NodeId::Client,
                Message::ChunkData { id, data: Some(data) }
            )]
        );
    }

    #[test]
    fn fetch_missing_returns_none() {
        let mut n = StorageNode::new(1);
        let id = sha256(b"absent");
        let out = n.handle(NodeId::Client, Message::FetchChunk { id });
        assert_eq!(
            out,
            vec![(
                NodeId::Storage(1),
                NodeId::Client,
                Message::ChunkData { id, data: None }
            )]
        );
    }

    #[test]
    fn corrupt_store_is_rejected() {
        let mut n = StorageNode::new(0);
        let id = sha256(b"real");
        let out = n.handle(
            NodeId::Client,
            Message::StoreChunk {
                id,
                data: b"tampered".to_vec(),
            },
        );
        assert!(out.is_empty());
        assert!(!n.has(&id));
    }

    #[test]
    fn replicate_order_ships_data() {
        let mut n = StorageNode::new(0);
        let data = vec![5u8; 10];
        let id = sha256(&data);
        n.handle(NodeId::Client, Message::StoreChunk { id, data: data.clone() });
        let out = n.handle(NodeId::Meta(0), Message::ReplicateOrder { id, dest: 3 });
        assert_eq!(
            out,
            vec![(NodeId::Storage(0), NodeId::Storage(3), Message::Replicate { id, data })]
        );
    }
}
