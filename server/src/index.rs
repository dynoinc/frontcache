use std::path::PathBuf;

use redb::{
    CommitError, Database, DatabaseError, ReadableDatabase, ReadableTable, StorageError,
    TableDefinition, TableError, TransactionError,
};
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

const BLOCKS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("blocks");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockState {
    Downloading,
    Downloaded,
    Purging,
}

impl BlockState {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "downloading" => Some(BlockState::Downloading),
            "downloaded" => Some(BlockState::Downloaded),
            "purging" => Some(BlockState::Purging),
            _ => None,
        }
    }

    fn as_str(&self) -> &str {
        match self {
            BlockState::Downloading => "downloading",
            BlockState::Downloaded => "downloaded",
            BlockState::Purging => "purging",
        }
    }
}

pub struct BlockEntry {
    pub path: String,
    pub state: BlockState,
}

pub struct Index {
    db: Database,
}

impl Index {
    pub fn open(path: PathBuf) -> Result<Self, IndexError> {
        let db = Database::create(path)?;
        let write_txn = db.begin_write()?;
        let _ = write_txn.open_table(BLOCKS_TABLE)?;
        write_txn.commit()?;
        Ok(Self { db })
    }

    pub fn insert<'a>(
        &self,
        entries: impl IntoIterator<Item = (&'a str, BlockEntry)>,
    ) -> Result<(), IndexError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(BLOCKS_TABLE)?;
            for (key, entry) in entries {
                let value = format!("{}:{}", entry.state.as_str(), entry.path);
                table.insert(key, value.as_str())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<(), IndexError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(BLOCKS_TABLE)?;
            table.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn list_all(&self) -> Result<Vec<(String, BlockEntry)>, IndexError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(BLOCKS_TABLE)?;

        table
            .iter()?
            .filter_map(|item| {
                let (key, value) = item.ok()?;
                let value_str = value.value();
                let parts: Vec<&str> = value_str.splitn(2, ':').collect();
                if parts.len() != 2 {
                    return None;
                }
                let state = BlockState::from_str(parts[0])?;
                Some(Ok((
                    key.value().to_string(),
                    BlockEntry {
                        path: parts[1].to_string(),
                        state,
                    },
                )))
            })
            .collect()
    }
}
