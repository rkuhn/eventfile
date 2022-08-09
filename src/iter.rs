use crate::{
    error::{ErrCtx, Fallible},
    formats::{BlockHeader, BranchHeader, HasMagic, IndexEntry, JumpEntry, LeafHeader, StagingHeader},
    mmap::MmapFile,
    u32_to_usize, Cache, EventFile,
};
use smallvec::SmallVec;
use std::{
    cell::RefCell,
    ops::{Bound, RangeBounds},
    sync::Arc,
};

macro_rules! handle_err {
    ($e:expr, $f:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                #[allow(clippy::no_effect)]
                $f;
                return Some(Err(e));
            }
        }
    };
}

pub struct SearchIter<'a> {
    file: &'a MmapFile,
    offset: u64,
}

impl<'a> SearchIter<'a> {
    pub fn new(file: &'a MmapFile, offset: u64) -> Self {
        Self { file, offset }
    }
}

impl<'a> Iterator for SearchIter<'a> {
    type Item = Fallible<(u64, &'a BlockHeader)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == u64::MAX {
            return None;
        }
        let ret = handle_err!(self.file.stream_at::<BlockHeader>(self.offset), self.offset = u64::MAX);
        let offset = self.offset;
        self.offset = if ret.level() == 0 {
            ret.prev_block()
        } else {
            let branch = handle_err!(
                self.file.stream_at::<BranchHeader>(self.offset + BlockHeader::SIZE),
                self.offset = u64::MAX
            );
            branch.prev_offset()
        };
        Some(Ok((offset, ret)))
    }
}

pub struct RangeIter<'a> {
    file: &'a MmapFile,
    cache: &'a RefCell<Box<dyn Cache>>,
    block_event_limit: u32,
    id: u32,
    done: bool,
    /// next event index to deliver
    start_idx: u64,
    /// last event index to deliver
    end_idx: u64,
    /// stack from which matching top-level branches are popped
    todo: SmallVec<[u64; 16]>,
}

impl<'a> RangeIter<'a> {
    pub fn new(file: &'a EventFile, last_block: u64, range: impl RangeBounds<u64>) -> Fallible<Self> {
        let EventFile { file, cache, id, block_event_limit, .. } = file;

        let (start_idx, end_idx) =
            if range.start_bound() == Bound::Excluded(&u64::MAX) || range.end_bound() == Bound::Excluded(&0) {
                (1, 0)
            } else {
                let start = match range.start_bound() {
                    Bound::Included(i) => *i,
                    Bound::Excluded(e) => *e + 1,
                    Bound::Unbounded => 0,
                };
                let end = match range.end_bound() {
                    Bound::Included(i) => *i,
                    Bound::Excluded(e) => *e - 1,
                    Bound::Unbounded => u64::MAX,
                };
                (start, end)
            };
        if start_idx > end_idx {
            return Ok(Self {
                file,
                cache,
                id: *id,
                block_event_limit: *block_event_limit,
                done: true,
                start_idx,
                end_idx,
                todo: SmallVec::new(),
            });
        }

        let todo = SearchIter::new(file, last_block)
            .filter_map(|x| {
                let (offset, block) = handle_err!(x, ());
                let (start, end) = if block.level() == 0 {
                    let leaf: &LeafHeader = handle_err!(file.stream_after(block), ());
                    let start = leaf.start_idx();
                    (start, start + u64::from(leaf.count()) - 1)
                } else {
                    let branch: &BranchHeader = handle_err!(file.stream_after(block), ());
                    let index: &IndexEntry = handle_err!(file.stream_after(branch), ());
                    (index.start_idx(), branch.end_idx() - 1)
                };
                if start <= end_idx && start_idx <= end {
                    Some(Ok(offset))
                } else {
                    None
                }
            })
            .collect::<Fallible<_>>()?;

        Ok(Self {
            file,
            cache,
            id: *id,
            block_event_limit: *block_event_limit,
            done: false,
            start_idx,
            end_idx,
            todo,
        })
    }

    fn decompress(&self, header: &BlockHeader, prio: bool) -> Fallible<Arc<[u8]>> {
        debug_assert!(header.level() == 0);
        let key = (self.id, self.file.stream_offset(header)?);
        let bytes = self.cache.borrow_mut().get(key);
        if let Some(bytes) = bytes {
            tracing::trace!(?key, "cache hit");
            Ok(bytes)
        } else {
            tracing::trace!(?key, prio, "cache miss");
            let leaf: &LeafHeader = self.file.stream_after(header)?;
            let length = u32_to_usize(header.length()) - LeafHeader::LEN;
            let bytes = self.file.stream_bytes_after(leaf, length)?;
            let bytes = zstd::decode_all(bytes).ctx("decompressing index")?;
            let bytes = Arc::<[u8]>::from(bytes);
            self.cache.borrow_mut().put(key, bytes.clone(), prio);
            Ok(bytes)
        }
    }
}

impl<'a> Iterator for RangeIter<'a> {
    type Item = Fallible<LeafSlice>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if self.todo.is_empty() {
            // rest is in staging area
            self.done = true;
            if self.start_idx > self.end_idx {
                return None;
            }
            let head = handle_err!(self.file.staging_at::<StagingHeader>(0), ());
            let head_start = head.start_idx();
            let head_count = u64::from(head.count());
            let start = self.start_idx - head_start;
            if start >= head_count {
                return None;
            }
            let end = (self.end_idx - head_start).min(head_count - 1);
            let bytes = handle_err!(self.file.staging_bytes(StagingHeader::LEN, self.file.staging_len()), ());
            let base = u32_to_usize(self.block_event_limit) * JumpEntry::LEN;
            return Some(Ok(LeafSlice::new(bytes.into(), start, end, base)));
        }
        let mut offset = *self.todo.last().unwrap();
        loop {
            let block: &BlockHeader = handle_err!(self.file.stream_at(offset), self.done = true);
            if block.level() == 0 {
                if offset == *self.todo.last().unwrap() {
                    self.todo.pop();
                }
                let bytes = handle_err!(self.decompress(block, false), self.done = true);
                let leaf: &LeafHeader = handle_err!(self.file.stream_after(block), self.done = true);
                let base = u32_to_usize(leaf.count() + 1) * JumpEntry::LEN;
                let iter = LeafSlice::new(
                    bytes,
                    self.start_idx - leaf.start_idx(),
                    (self.end_idx - leaf.start_idx()).min(u64::from(leaf.count()) - 1),
                    base,
                );
                self.start_idx = leaf.start_idx() + u64::from(leaf.count());
                return Some(Ok(iter));
            } else {
                let branch: &BranchHeader = handle_err!(self.file.stream_after(block), self.done = true);
                if branch.end_idx() <= self.start_idx || self.start_idx > self.end_idx {
                    self.todo.pop();
                    offset = *self.todo.last()?;
                    continue;
                }
                let count = (u32_to_usize(block.length()) - BranchHeader::LEN) / IndexEntry::LEN;
                let mut e: &IndexEntry = handle_err!(self.file.stream_after(branch), self.done = true);
                for _ in 1..count {
                    let n: &IndexEntry = handle_err!(self.file.stream_after(e), self.done = true);
                    if n.start_idx() > self.start_idx {
                        break;
                    }
                    e = n;
                }
                offset = e.offset();
            }
        }
    }
}

pub struct LeafSlice {
    bytes: Arc<[u8]>,
    start_idx: u32,
    end_idx: u32,
    base: usize,
}

impl LeafSlice {
    pub fn new(bytes: Arc<[u8]>, start_idx: u64, end_idx: u64, base: usize) -> Self {
        Self {
            bytes,
            start_idx: start_idx.try_into().unwrap(),
            end_idx: end_idx.try_into().unwrap(),
            base,
        }
    }

    pub fn iter(&self) -> LeafIter<'_> {
        LeafIter {
            leaf: &self.bytes,
            pos: self.start_idx,
            last: self.end_idx,
            base: self.base,
        }
    }
}

pub struct LeafIter<'a> {
    leaf: &'a Arc<[u8]>,
    pos: u32,
    last: u32,
    base: usize,
}

impl<'a> Iterator for LeafIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos > self.last {
            return None;
        }
        let pos = 4 * u32_to_usize(self.pos);
        let from = u32_to_usize(JumpEntry::from_slice(&self.leaf[pos..pos + 4]).pos());
        let to = u32_to_usize(JumpEntry::from_slice(&self.leaf[pos + 4..pos + 8]).pos());
        self.pos += 1;
        let ret = &self.leaf[self.base + from..self.base + to];
        Some(ret)
    }
}
