use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::error::Result;
use crate::raft_rpc::raftrpc::{Entry, HardState, Snapshot, SnapshotMetadata};
use std::cmp;
use std::sync::Arc;

#[derive(Default)]
pub struct RaftState {
    pub hard_state: HardState,
    // TODO: ConfState?
}

// TODO: refactor error types here
pub trait StorageTest {
    fn initial_state(&self) -> Result<RaftState>;
    fn entries(&self, low: u64, high: u64) -> Result<Vec<Entry>>;
    fn term(&self, idx: u64) -> Result<u64>;
    fn first_index(&self) -> Result<u64>;
    fn last_index(&self) -> Result<u64>;
    fn snapshot(&self, request_index: u64, to: u64) -> Result<Snapshot>;
}

pub trait Storage {}

/*
    The core of the in-memory storage
    all the design is taken from tikv/raft-rs,
    except here we add some file persistence mechanics
    to further enhance the design, until a suitable KV DB
    is used
*/
#[derive(Default)]
pub struct MemStorageInner {
    raft_state: RaftState,
    entries: Vec<Entry>,
    snapshot_metadata: SnapshotMetadata,
    snapshot_unavailable: bool,
    log_unavailable: bool,
    // TODO: getentriescontext?
}

impl MemStorageInner {
    pub fn set_hardstate(&mut self, hs: HardState) {
        self.raft_state.hard_state = hs;
    }

    pub fn hard_state(&self) -> &HardState {
        &self.raft_state.hard_state
    }

    fn first_index(&self) -> u64 {
        match self.entries.first() {
            Some(e) => e.commit_index,
            None => self.snapshot_metadata.index + 1,
        }
    }

    fn last_index(&self) -> u64 {
        match self.entries.last() {
            Some(e) => e.commit_index,
            None => self.snapshot_metadata.index,
        }
    }

    fn has_entry_at(&self, index: u64) -> bool {
        !self.entries.is_empty() && index >= self.first_index() && index <= self.last_index()
    }

    pub fn commit_to_index(&mut self, index: u64) -> Result<()> {
        assert!(
            self.has_entry_at(index),
            "storage is commiting to {}, but the entry does not exist",
            index
        );
        let offset = (index - self.entries[0].commit_index) as usize;
        self.raft_state.hard_state.commit_index = index;
        self.raft_state.hard_state.term = self.entries[offset].term;
        Ok(())
    }

    // TODO: fn to set conf state

    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<()> {
        // Apply snapshot internally by clearing entries and picking
        // the most recent data, comparatively to the snapshot
        let meta = snapshot.metadata;
        let mut index = 0;

        if let Some(meta) = meta {
            index = meta.index;
        } else {
            // return since the snapshot doesn't have index
            // no use in applying an unreliable snapshot?
            return Ok(());
        }

        if self.first_index() > index {
            // Error out, outdated index
        }

        //TODO: unwrap may be problematic here
        self.snapshot_metadata = meta.unwrap().clone();
        self.raft_state.hard_state.term =
            cmp::max(self.raft_state.hard_state.term, self.snapshot_metadata.term);
        self.raft_state.hard_state.commit_index = index;
        self.entries.clear();

        // TODO: update conf state
        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        // Currently assume that all entries which have indexes less than the
        // HardState commit index have been applied, so the current snapshot
        // is using the latest commit index

        let mut snapshot = Snapshot::default();

        let mut meta = SnapshotMetadata::default();
        meta.index = self.raft_state.hard_state.commit_index;
        meta.term = match meta.index.cmp(&self.snapshot_metadata.index) {
            cmp::Ordering::Equal => self.snapshot_metadata.term,
            cmp::Ordering::Greater => {
                let offset = self.entries[0].commit_index;
                self.entries[(meta.index - offset) as usize].term
            }
            cmp::Ordering::Less => {
                // Panic/error out
                panic!(
                    "Commit index {} is less than the snapshot metadata index {}",
                    meta.index, self.snapshot_metadata.index
                );
            }
        };

        snapshot.metadata = Some(meta);
        // TODO: Set conf state here

        snapshot
    }

    pub fn compact(&mut self, index: u64) -> Result<()> {
        // Compacts entries by removing all prior entries to the compact

        if index <= self.first_index() {
            return Ok(());
        }

        if index > self.last_index() + 1 {
            panic!(
                "compact index: {} is greater than the last index (plus one): {}",
                index,
                self.last_index()
            );
        }

        if let Some(entry) = self.entries.first() {
            let offset = index - entry.commit_index;
            self.entries.drain(..offset as usize);
        }
        Ok(())
    }

    pub fn append(&mut self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        if self.first_index() > entries[0].commit_index {
            panic!();
        }

        if self.last_index() + 1 < entries[0].commit_index {
            panic!();
        }

        let offset = entries[0].commit_index - self.first_index();
        self.entries.drain(offset as usize..);
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    pub fn set_snapshot_unavailable(&mut self) {
        self.snapshot_unavailable = true;
    }

    pub fn set_log_unavailable(&mut self, state: bool) {
        self.log_unavailable = state;
    }
}

#[derive(Clone, Default)]
pub struct MemStorage {
    // try using tokio RwLock
    inner: Arc<RwLock<MemStorageInner>>,
}

impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage { ..Default::default() }
    }

    pub fn rl(&self) -> RwLockReadGuard<'_, MemStorageInner> {
        self.inner.read().unwrap()
    }

    pub fn wl(&self) -> RwLockWriteGuard<'_, MemStorageInner> {
        self.inner.write().unwrap()
    }
}
