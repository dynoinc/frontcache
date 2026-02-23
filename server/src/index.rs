use std::path::PathBuf;

use redb::{
    CommitError, Database, DatabaseError, Durability, ReadableDatabase, ReadableTable,
    SetDurabilityError, StorageError, TableDefinition, TableError, TransactionError,
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
    #[error("Set durability error: {0}")]
    SetDurability(#[from] SetDurabilityError),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type BlockKey = (String, u64);

const BLOCKS_TABLE: TableDefinition<BlockKey, &[u8]> = TableDefinition::new("blocks");
const LAST_ACCESSED_TABLE: TableDefinition<BlockKey, u64> = TableDefinition::new("last_accessed");

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

pub struct BlockRecord {
    pub entry: BlockEntry,
    pub last_accessed: Option<u64>,
}

impl Index {
    pub fn open(path: PathBuf) -> Result<Self, IndexError> {
        let db = Database::create(path)?;
        let write_txn = db.begin_write()?;
        let _ = write_txn.open_table(BLOCKS_TABLE)?;
        let _ = write_txn.open_table(LAST_ACCESSED_TABLE)?;
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
            let mut blocks = write_txn.open_table(BLOCKS_TABLE)?;
            let mut last_accessed = write_txn.open_table(LAST_ACCESSED_TABLE)?;
            blocks.remove(key)?;
            last_accessed.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn delete_many(&self, keys: &[BlockKey]) -> Result<(), IndexError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut blocks = write_txn.open_table(BLOCKS_TABLE)?;
            let mut last_accessed = write_txn.open_table(LAST_ACCESSED_TABLE)?;
            for key in keys {
                blocks.remove(key)?;
                last_accessed.remove(key)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn flush_last_accessed(
        &self,
        entries: impl IntoIterator<Item = (BlockKey, u64)>,
    ) -> Result<(), IndexError> {
        let mut write_txn = self.db.begin_write()?;
        write_txn.set_durability(Durability::None)?;
        {
            let mut table = write_txn.open_table(LAST_ACCESSED_TABLE)?;
            for (key, ts) in entries {
                table.insert(key, ts)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn list_all(&self) -> Result<Vec<(BlockKey, BlockRecord)>, IndexError> {
        let read_txn = self.db.begin_read()?;
        let blocks_table = read_txn.open_table(BLOCKS_TABLE)?;
        let la_table = read_txn.open_table(LAST_ACCESSED_TABLE)?;

        let mut la_iter = la_table.iter()?.peekable();

        let mut records = Vec::new();
        for item in blocks_table.iter()? {
            let (bk, bv) = item?;
            let block_key: BlockKey = (bk.value().0.to_string(), bk.value().1);

            while let Some(Ok((k, _))) = la_iter.peek() {
                if (k.value().0.as_str(), k.value().1) < (block_key.0.as_str(), block_key.1) {
                    la_iter.next();
                } else {
                    break;
                }
            }

            let mut last_accessed = None;
            if let Some(Ok((k, _))) = la_iter.peek()
                && (k.value().0.as_str(), k.value().1) == (block_key.0.as_str(), block_key.1)
            {
                let (_, v) = la_iter.next().unwrap()?;
                last_accessed = Some(v.value());
            }

            let entry: BlockEntry = serde_json::from_slice(bv.value())?;
            records.push((
                block_key,
                BlockRecord {
                    entry,
                    last_accessed,
                },
            ));
        }
        Ok(records)
    }
}
