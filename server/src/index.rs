use std::path::PathBuf;

use redb::{
    CommitError, Database, DatabaseError, Key, ReadTransaction, ReadableDatabase, ReadableTable,
    StorageError, TableDefinition, TableError, TransactionError, TypeName, Value,
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
}

const BLOCKS_TABLE: TableDefinition<BlockKey, BlockEntry> = TableDefinition::new("blocks");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockState {
    Downloading,
    Downloaded,
    Purging,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockEntry {
    pub path: String,
    pub state: BlockState,
}

impl Value for BlockEntry {
    type SelfType<'a> = BlockEntry;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        bincode::deserialize(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        bincode::serialize(value).unwrap()
    }

    fn type_name() -> TypeName {
        TypeName::new("BlockEntry")
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct BlockKey {
    pub object: String,
    pub block_offset: u64,
}

impl Key for BlockKey {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

impl Value for BlockKey {
    type SelfType<'a> = BlockKey;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        bincode::deserialize(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        bincode::serialize(value).unwrap()
    }

    fn type_name() -> TypeName {
        TypeName::new("BlockKey")
    }
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

    pub fn insert(
        &self,
        entries: impl IntoIterator<Item = (BlockKey, BlockEntry)>,
    ) -> Result<(), IndexError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(BLOCKS_TABLE)?;
            for (key, entry) in entries {
                table.insert(&key, &entry)?;
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
                Some((key.value(), value.value()))
            }))
                as Box<dyn Iterator<Item = (BlockKey, BlockEntry)>>)
        };

        Ok(AllBlocksIter {
            _txn: read_txn,
            iter,
        })
    }
}
