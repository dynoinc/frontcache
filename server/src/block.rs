use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use short_uuid::ShortUuid;

#[derive(Debug, thiserror::Error)]
pub enum BlockError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("alloc failed: {0}")]
    Layout(#[from] std::alloc::LayoutError),
    #[error("persist failed: {0}")]
    Persist(#[from] tempfile::PersistError),
    #[error("spawn failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("{0}")]
    Other(String),
}

const ALIGN: usize = 4096;

struct AlignedBuf {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl AlignedBuf {
    fn new(len: usize) -> Result<Self, BlockError> {
        if len == 0 {
            return Err(BlockError::Other("zero-size AlignedBuf".into()));
        }
        let layout = Layout::from_size_align(len, ALIGN)?;
        let ptr = unsafe { alloc_zeroed(layout) };
        let ptr =
            NonNull::new(ptr).ok_or_else(|| BlockError::Other("aligned alloc failed".into()))?;
        Ok(Self { ptr, layout })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }
}

impl AsRef<[u8]> for AlignedBuf {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

pub struct PendingBlock {
    tmp: tempfile::NamedTempFile,
    path: PathBuf,
    e_tag: String,
    block_size: u64,
    object_size: u64,
}

impl PendingBlock {
    pub async fn prepare(
        cache_dir: &Path,
        data: Bytes,
        e_tag: String,
        object_size: u64,
    ) -> Result<Self, BlockError> {
        let id = ShortUuid::generate().to_string();
        let prefix = &id[..2];
        let filename = format!("{}.blk", id);

        let final_dir = cache_dir.join(prefix);
        tokio::fs::create_dir_all(&final_dir).await?;

        let tmp = tempfile::NamedTempFile::new_in(cache_dir.join("tmp"))?;
        let block_size = data.len() as u64;
        let path = final_dir.join(&filename);

        let tmp =
            tokio::task::spawn_blocking(move || -> Result<tempfile::NamedTempFile, BlockError> {
                let mut file = tmp;
                file.as_file().set_len(block_size)?;
                file.write_all(&data)?;
                file.as_file().sync_all()?;
                Ok(file)
            })
            .await??;

        Ok(Self {
            tmp,
            path,
            e_tag,
            block_size,
            object_size,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn persist(self) -> Result<Block, BlockError> {
        self.tmp.persist(&self.path)?;
        Ok(Block::new(
            self.path,
            self.e_tag,
            self.block_size,
            self.object_size,
            Block::now(),
        ))
    }
}

pub struct Block {
    path: PathBuf,
    e_tag: String,
    block_size: u64,
    object_size: u64,
    last_accessed: AtomicU64,
}

impl Block {
    pub fn new(
        path: PathBuf,
        e_tag: String,
        block_size: u64,
        object_size: u64,
        last_accessed: u64,
    ) -> Self {
        Self {
            path,
            e_tag,
            block_size,
            object_size,
            last_accessed: AtomicU64::new(last_accessed),
        }
    }

    pub fn open_reader(&self) -> Result<BlockReader, BlockError> {
        let file = open_direct(&self.path)?;
        Ok(BlockReader {
            file: Arc::new(file),
        })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn e_tag(&self) -> &str {
        &self.e_tag
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn object_size(&self) -> u64 {
        self.object_size
    }

    pub fn record_access(&self) {
        self.last_accessed.store(Self::now(), Ordering::Relaxed);
    }

    pub fn last_accessed(&self) -> u64 {
        self.last_accessed.load(Ordering::Relaxed)
    }

    pub fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

pub struct BlockReader {
    file: Arc<File>,
}

impl BlockReader {
    pub async fn read_chunk(&self, offset: u64, len: usize) -> Result<Bytes, BlockError> {
        if len == 0 {
            return Ok(Bytes::new());
        }

        let file = self.file.clone();
        let aligned_offset = offset & !(ALIGN as u64 - 1);
        let skip = (offset - aligned_offset) as usize;
        let rounded = (skip + len + ALIGN - 1) & !(ALIGN - 1);

        tokio::task::spawn_blocking(move || {
            let mut buf = AlignedBuf::new(rounded)?;
            let n = file.read_at(buf.as_mut_slice(), aligned_offset)?;
            if n <= skip {
                return Ok(Bytes::new());
            }
            let end = (skip + len).min(n);
            Ok(Bytes::from_owner(buf).slice(skip..end))
        })
        .await?
    }
}

fn open_direct(path: &Path) -> Result<File, BlockError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
        {
            Ok(f) => return Ok(f),
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(OpenOptions::new().read(true).open(path)?)
}
