mod cache;
mod dump;
mod error;
mod formats;
mod iter;
mod mmap;

pub use cache::Cache;
pub use error::Error;
pub use iter::{LeafIter, LeafSlice, RangeIter};

use error::{ErrCtx, Fallible};
use formats::{BlockHeader, BranchHeader, HasMagic, IndexEntry, LeafHeader, StagingHeader, StagingHeaderLifted};
use iter::SearchIter;
use mmap::MmapFile;
use smallvec::SmallVec;
use std::{cell::RefCell, io::Write, mem::size_of_val, ops::RangeBounds, path::PathBuf, slice};

pub struct EventFile {
    file: MmapFile,
    id: u32,
    compression_threshold: usize,
    block_event_limit: u32,
    cache: RefCell<Box<dyn Cache>>,
}

impl EventFile {
    pub fn new(
        id: u32, path: PathBuf, user_version: u32, compression_threshold: usize, block_event_limit: u32,
        cache: Box<dyn Cache>,
    ) -> Fallible<Self> {
        let mut ret = Self {
            file: MmapFile::new(path, user_version)?,
            id,
            compression_threshold,
            block_event_limit,
            cache: RefCell::new(cache),
        };
        if ret.file.staging_len() == 0 {
            // fresh file
            ret.prep_staging(u64::MAX, 0)?;
        }
        Ok(ret)
    }

    fn prep_staging(&mut self, last_block: u64, start_idx: u64) -> Fallible<()> {
        let size = self.staging_event_start() + self.compression_threshold;
        self.file.clear_staging();
        self.file.ensure_staging_len(size)?;
        self.file.staging_put(0, StagingHeader::new(last_block, start_idx, 0, self.block_event_limit))?;
        self.flush()?;
        Ok(())
    }

    fn staging_event_start(&self) -> usize {
        self.staging_idx(u32_to_usize(self.block_event_limit))
    }

    fn staging_idx(&self, idx: usize) -> usize {
        StagingHeader::LEN + idx * 4
    }

    fn staging_header(&self) -> Fallible<StagingHeaderLifted> {
        self.file.staging_at::<StagingHeader>(0).map(|x| x.lift())
    }

    pub fn append(&mut self, event: &[u8]) -> Fallible<()> {
        let header = self.staging_header()?;
        let count = header.count;
        let idx = self.staging_idx(u32_to_usize(count));
        let offset = u32::from_be(*self.file.staging_mut_no_magic::<u32>(idx)?);
        let start = self.staging_event_start() + u32_to_usize(offset);
        self.file.ensure_staging_len(start + event.len())?;
        self.file.staging_write(start, event)?;
        let new_len = offset + event.len() as u32;
        *self.file.staging_mut_no_magic::<u32>(idx + 4)? = new_len.to_be();
        self.file.staging_at_mut::<StagingHeader>(0)?.set_count(count + 1);
        if count + 2 >= header.capacity || u32_to_usize(new_len) >= self.compression_threshold {
            self.compress()?;
        }
        Ok(())
    }

    fn compress(&mut self) -> Fallible<()> {
        let header = self.staging_header()?;

        // compress jump table and event data
        let mut encoder = zstd::Encoder::new(Vec::new(), 21).ctx("creating encoder")?;
        let from = self.staging_idx(0);
        let to = self.staging_idx(u32_to_usize(header.count));
        let jump_table = self.file.staging_bytes(from, to + 4)?;
        encoder.write_all(jump_table).ctx("compressing")?;
        let end = self.staging_event_start() + u32_to_usize(u32::from_be(*self.file.staging_mut_no_magic(to)?));
        let event_data = self.file.staging_bytes(self.staging_event_start(), end)?;
        encoder.write_all(event_data).ctx("compressing")?;
        let compressed = encoder.finish().ctx("compressing")?;
        let length = compressed
            .len()
            .checked_add(LeafHeader::LEN)
            .and_then(|l| u32::try_from(l).ok())
            .ok_or(Error::numeric_overflow("compression result > 4GiB"))?;

        // must be recorded before appending!
        let mut current = self.file.end_offset();

        // write block header, leaf header, and compressed data at level 0
        self.file.stream_append(BlockHeader::new(header.last_block, 0, length))?;
        self.file.stream_append(LeafHeader::new(header.start_idx, header.count))?;
        self.file.stream_append_bytes(&*compressed)?;

        // possibly write new index blocks
        let mut level = 1;
        loop {
            let mut prev_idx = u64::MAX;
            let mut indexes = SmallVec::<[IndexEntry; 16]>::new();
            let mut end_idx = 0;
            for block in SearchIter::new(&self.file, current) {
                let (offset, block) = block?;
                if block.level() >= level {
                    prev_idx = offset;
                    break;
                }
                let (start_idx, end) = if block.level() == 0 {
                    let leaf: &LeafHeader = self.file.stream_at_no_magic(offset + BlockHeader::SIZE)?;
                    (leaf.start_idx(), leaf.start_idx() + u64::from(leaf.count()))
                } else {
                    let offset = offset + BlockHeader::SIZE;
                    let branch: &BranchHeader = self.file.stream_at(offset)?;
                    let offset = offset + BranchHeader::SIZE;
                    let index: &IndexEntry = self.file.stream_at_no_magic(offset)?;
                    (index.start_idx(), branch.end_idx())
                };
                if end_idx == 0 {
                    end_idx = end;
                }
                indexes.push(IndexEntry::new(offset, start_idx));
            }
            if indexes.len() < 16 {
                break;
            }
            indexes.reverse();

            let next_current = self.file.end_offset();
            let length = u32::try_from(BranchHeader::LEN + size_of_val(&*indexes)).ctx("index > 4GiB")?;
            self.file.stream_append(BlockHeader::new(current, level, length))?;
            self.file.stream_append(BranchHeader::new(prev_idx, end_idx))?;
            let index_bytes =
                unsafe { slice::from_raw_parts(&*indexes as *const _ as *const u8, size_of_val(&*indexes)) };
            self.file.stream_append_bytes(index_bytes)?;

            current = next_current;
            level += 1;
        }

        self.prep_staging(current, header.start_idx + u64::from(header.count))?;

        Ok(())
    }

    pub fn flush(&self) -> Fallible<()> {
        self.file.flush()
    }

    pub fn iter(&self, range: impl RangeBounds<u64>) -> Fallible<RangeIter> {
        RangeIter::new(self, self.staging_header()?.last_block, range)
    }
}

macro_rules! embed {
    ($($f:ident: $from:ty => $to:ty;)*) => {
        $(
            #[allow(dead_code)]
            const fn $f(x: $from) -> $to {
                #[allow(dead_code)]
                const ASSERT: () = {
                    if std::mem::size_of::<$from>() > std::mem::size_of::<$to>() {
                        panic!(concat!("this library requires ", stringify!($to), " to be at least as wide as ", stringify!($from)));
                    }
                };
                x as $to
            }
        )*
    };
}

embed! {
    u8_to_u32: u8 => u32;
    u8_to_usize: u8 => usize;
    u32_to_usize: u32 => usize;
    usize_to_u64: usize => u64;
    isize_to_u64: isize => u64;
}

/// TODO:
///
///  - don’t compress index blocks
///  - fixed indexing by message number
///  - change level 0 index to jump table
///  - hand out reference to bytes instead of taking an extractor function
///  - clean up low-level access into tiny internal API
#[allow(dead_code)]
const DONE: bool = false;
