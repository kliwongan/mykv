// Temporary memory storage used for raft election algorithm testing only
use crate::storage::Storage;

pub struct MemStorage {}

impl Storage for MemStorage {}
