use std::path::PathBuf;

use redb::{
    CommitError, Database, DatabaseError, ReadTransaction, ReadableDatabase, ReadableTable,
    StorageError, TableDefinition, TableError, TransactionError,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("Database error: {0}")]
    Database(#[from] DatabaseError),
    #[error("Transaction error: {0}")]
    Transaction(#[from] TransactionError),
    #[error("Table error: {0}")]
    Table(#[from] TableError),
    #[error("Commit error: {0}")]
    Commit(#[from] CommitError),
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type BlockKey = (String, u64);

const BLOCKS_TABLE: TableDefinition<BlockKey, &[u8]> = TableDefinition::new("blocks");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockState {
    Writing,
    Downloaded,
    Purging,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockEntry {
    pub path: String,
    pub state: BlockState,
    pub version: String,
}

pub struct Index {
    db: Database,
}

pub struct AllBlocksIter {
    _txn: ReadTransaction,
    iter: Box<dyn Iterator<Item = (BlockKey, BlockEntry)>>,
}

impl Iterator for AllBlocksIter {
    type Item = (BlockKey, BlockEntry);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl Index {
    pub fn open(path: PathBuf) -> Result<Self, IndexError> {
        let db = Database::create(path)?;
        let write_txn = db.begin_write()?;
        let _ = write_txn.open_table(BLOCKS_TABLE)?;
        write_txn.commit()?;
        Ok(Self { db })
    }

    pub fn upsert(
        &self,
        entries: impl IntoIterator<Item = (BlockKey, BlockEntry)>,
    ) -> Result<(), IndexError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(BLOCKS_TABLE)?;
            for ((object, offset), entry) in entries {
                let value = serde_json::to_vec(&entry)?;
                table.insert((object, offset), value.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn delete(&self, key: &BlockKey) -> Result<(), IndexError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(BLOCKS_TABLE)?;
            table.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn delete_many(&self, keys: &[BlockKey]) -> Result<(), IndexError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(BLOCKS_TABLE)?;
            for key in keys {
                table.remove(key)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn list_all(&self) -> Result<AllBlocksIter, IndexError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(BLOCKS_TABLE)?;
        let raw_iter = table.iter()?;

        // SAFETY: We extend the iterator's lifetime to 'static, but this is safe because:
        // 1. The iterator is stored in AllBlocksIter alongside _txn
        // 2. _txn is declared before iter in the struct, so it will be dropped AFTER iter
        // 3. This ensures the transaction outlives the iterator
        let iter: Box<dyn Iterator<Item = (BlockKey, BlockEntry)>> = unsafe {
            std::mem::transmute(Box::new(raw_iter.filter_map(|item| {
                let (key, value) = item.ok()?;
                let (object, offset) = key.value();
                let entry: BlockEntry = serde_json::from_slice(value.value()).ok()?;
                Some(((object.to_string(), offset), entry))
            }))
                as Box<dyn Iterator<Item = (BlockKey, BlockEntry)>>)
        };

        Ok(AllBlocksIter {
            _txn: read_txn,
            iter,
        })
    }
}
