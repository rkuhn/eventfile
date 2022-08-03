use crate::{
    cache::Cache,
    error::{ErrCtx, Fallible},
    formats::{BlockHeader, BlockTrailer, StreamHeader},
    io::write_all_vectored,
    isize_to_u64, u32_to_usize, u8_to_u32, u8_to_usize, usize_to_u64, Error, EventFile,
    EventFileIter, EventFrame,
};
use memmap2::Mmap;
use smallvec::SmallVec;
use std::{
    cell::RefCell,
    fs::File,
    io::{Read, Write},
    mem::{replace, size_of},
    num::NonZeroUsize,
    ops::Range,
    path::PathBuf,
    rc::Rc,
    sync::Arc,
};
use zstd::stream::Encoder;

pub struct Stream {
    id: u32,
    config: StreamConfig,
    path: PathBuf,
    main: File,
    uncompressed: EventFile,
    uncompressed_index: Option<Vec<u8>>,
    user_version: u32,
    /// the file byte offset of the first stored block relative to stream start
    offset: u64,
    /// the memory offset of the last block relative to mmap start
    last_header: Option<NonZeroUsize>,
    mmap: Mmap,
}

pub const FANOUT: usize = 16;

pub struct StreamConfig {
    compression_threshold: u64,
    // signature_key: SecretKey,
    summarise_leaf: fn(EventFileIter) -> Vec<u8>,
    summarise_index: fn(NodeType, [&[u8]; FANOUT]) -> Vec<u8>,
    cache: RefCell<Box<dyn Cache>>,
}

impl StreamConfig {
    pub fn new(
        summarise_leaf: fn(EventFileIter) -> Vec<u8>,
        summarise_index: fn(NodeType, [&[u8]; FANOUT]) -> Vec<u8>,
        // signature_key: SecretKey,
        cache: Box<dyn Cache>,
    ) -> Self {
        Self {
            compression_threshold: 100_000,
            summarise_leaf,
            summarise_index,
            // signature_key,
            cache: RefCell::new(cache),
        }
    }

    pub fn compression_threshold(self, threshold: u64) -> Self {
        Self {
            compression_threshold: threshold,
            ..self
        }
    }
}

const HEADER_SIZE: usize = size_of::<StreamHeader>();
const HEADER_LEN: u64 = HEADER_SIZE as u64;

const VERSION_1: u32 = 0xe5f00001;

/// Prefix-forgettable append-only event log
///
/// Data format:
///  - header
///  - blocks
///
/// header format:
///  - stream version: u32
///  - user version: u32
///  - offset: u64
///
/// block format:
///  + block header
///    - total length: u32
///    - level: u32
///    - previous: u64 (offset of level same or higher, -1 if none)
///  + block payload
///    - compressed payload
///    - padding bytes
///  + block trailer
///    - padding length: u8
///    - (3 bytes unused)
///    - total length: u32
///
/// payload format:
///  - level 0:
///      - index length: u32
///      - index bytes
///      - event length: u32
///      - event bytes
///      - repeat event
///  - level >0:
///      - index bytes
///
/// Note that each block of level N>0 is preceded by a block of level N-1.
impl Stream {
    pub fn new(
        id: u32,
        config: StreamConfig,
        path: impl Into<PathBuf>,
        version: u32,
    ) -> Fallible<Self> {
        let path: PathBuf = path.into();
        let _span = tracing::debug_span!("init", path = %path.display(), id).entered();
        let mut main = File::options()
            .append(true)
            .create(true)
            .read(true)
            .open(&*path)
            .ctx(&*path)?;
        let mut offset = 0;
        if main.metadata().ctx(&*path)?.len() < HEADER_LEN {
            tracing::debug!(version, "writing new header");
            main.set_len(0).ctx(&*path)?;
            let header = StreamHeader::new(VERSION_1, version, 0);
            main.write_all(header.as_slice()).ctx(&*path)?;
        } else {
            let mut bytes = [0u8; size_of::<StreamHeader>()];
            main.read_exact(&mut bytes).ctx(&*path)?;
            let header = StreamHeader::from_slice(&bytes);
            tracing::debug!(
                "read header (stream={:08x} user={:08x} offset={})",
                header.stream_version(),
                header.user_version(),
                header.offset()
            );
            if header.stream_version() != VERSION_1 {
                return Err(Error::WrongStreamVersion(header.stream_version()));
            }
            if header.user_version() != version {
                return Err(Error::WrongUserVersion {
                    expected: version,
                    found: header.user_version(),
                });
            }
            offset = header.offset();
        }
        let mmap = unsafe { Mmap::map(&main) }.ctx(&*path)?;
        tracing::trace!("file contains {} bytes", mmap.len());
        let last_header = if mmap.len() <= HEADER_SIZE {
            None
        } else {
            let Range { end, .. } = mmap.as_ptr_range();
            let trailer = unsafe { &*BlockTrailer::from_ptr(end.sub(size_of::<BlockTrailer>())) };
            tracing::trace!("last trailer length={}", trailer.length());
            const MIN_SIZE: u32 = u8_to_u32(BlockHeader::size()) + u8_to_u32(BlockTrailer::size());
            if trailer.length() < MIN_SIZE {
                return Err(Error::DataCorruption {
                    message: "last block too short",
                    found: trailer.length().into(),
                    expected: MIN_SIZE.into(),
                });
            }
            if mmap.len() < HEADER_SIZE + u32_to_usize(trailer.length()) {
                return Err(Error::DataCorruption {
                    message: "last block too long",
                    found: trailer.length().into(),
                    expected: usize_to_u64(mmap.len() - HEADER_SIZE),
                });
            }
            let header =
                unsafe { &*BlockHeader::from_ptr(end.sub(u32_to_usize(trailer.length()))) };
            if header.length() != trailer.length() {
                return Err(Error::DataCorruption {
                    message: "header and trailer length mismatch",
                    found: header.length().into(),
                    expected: trailer.length().into(),
                });
            }
            let lh =
                unsafe { NonZeroUsize::new_unchecked(mmap.len() - u32_to_usize(trailer.length())) };
            tracing::trace!("last_header={}", lh);
            Some(lh)
        };
        let mut uncompressed_file = path
            .file_name()
            .expect("stream filename ends in /..")
            .to_owned();
        uncompressed_file.push(".uncompressed");
        let uncompressed = EventFile::open(
            path.with_file_name(uncompressed_file),
            offset
                .checked_add(usize_to_u64(mmap.len() - HEADER_SIZE))
                .ok_or(Error::NumericOverflow("fitting stream length into u64"))?,
        )?;
        Ok(Self {
            id,
            config,
            path,
            main,
            uncompressed,
            uncompressed_index: None,
            user_version: version,
            offset,
            last_header,
            mmap,
        })
    }

    pub fn version(&self) -> u32 {
        self.user_version
    }

    pub fn append(&mut self, frame: EventFrame<'_>) -> Fallible<()> {
        let _span =
            tracing::trace_span!("append", id = %self.id, last = ?self.last_header).entered();
        self.uncompressed.append(frame)?;
        self.uncompressed_index = None;
        if self.uncompressed.len() > self.config.compression_threshold {
            self.write_level_zero()?;
            let mut level = 0;
            loop {
                let mut branches = [None; FANOUT];
                let mut pos = FANOUT;
                for block in self.search_iter() {
                    if block.level() > level {
                        break;
                    }
                    pos -= 1;
                    branches[pos] = Some(block);
                }
                if pos != 0 {
                    break;
                }
                level += 1;
                let (header, payload, padding, trailer) =
                    self.compose_upper_level(branches.map(|x| x.unwrap()), level)?;
                let padding = &[0u8; 7][..padding as usize];
                write_all_vectored(
                    &mut self.main,
                    [header.as_slice(), &*payload, padding, trailer.as_slice()],
                )
                .ctx(&*self.path)?;
                self.advance_mmap_last_header()?;
            }
        }
        Ok(())
    }

    pub fn iter<F1, F2, T>(
        &mut self,
        selector: F1,
        extractor: F2,
    ) -> Fallible<StreamIter<'_, F1, F2, T>>
    where
        F1: FnMut(NodeType, &[u8]) -> Fallible<SmallVec<[u32; FANOUT]>>,
        F2: FnMut(&[u8]) -> Fallible<T>,
    {
        StreamIter::new(self, selector, extractor)
    }

    fn search_iter(&self) -> SearchIter<'_> {
        self.search_from(self.last_header())
    }

    fn search_from<'a>(&'a self, pos: Option<&'a BlockHeader>) -> SearchIter<'_> {
        tracing::trace!(?pos, "search_iter");
        SearchIter {
            start: unsafe { self.mmap.as_ptr().add(HEADER_SIZE) },
            offset: self.offset,
            pos,
        }
    }

    fn last_header(&self) -> Option<&BlockHeader> {
        self.last_header.map(|head| header_at(&self.mmap, head))
    }

    fn block_offset(&self, header: &BlockHeader) -> u64 {
        let ptr = header.as_ptr();
        let Range { start, end } = self.mmap.as_ptr_range();
        debug_assert!(ptr > start && ptr < end, "foreign block");
        self.offset + isize_to_u64(unsafe { ptr.offset_from(start) }) - HEADER_LEN
    }

    fn prev_block(&self, header: &BlockHeader) -> Option<&BlockHeader> {
        const MIN_BYTES: usize =
            HEADER_SIZE + u8_to_usize(BlockHeader::size()) + u8_to_usize(BlockTrailer::size());
        if header.as_ptr() < unsafe { self.mmap.as_ptr().add(MIN_BYTES) } {
            None
        } else {
            let trailer = unsafe {
                &*BlockTrailer::from_ptr(header.as_ptr().sub(BlockTrailer::size().into()))
            };
            let header = unsafe {
                &*BlockHeader::from_ptr(header.as_ptr().sub(u32_to_usize(trailer.length())))
            };
            Some(header)
        }
    }

    fn advance_mmap_last_header(&mut self) -> Fallible<()> {
        self.mmap = unsafe { Mmap::map(&self.main) }.ctx(&*self.path)?;
        if let Some(lh) = self.last_header.as_mut() {
            *lh = unsafe {
                NonZeroUsize::new_unchecked(
                    lh.get()
                        .checked_add(u32_to_usize(header_at(&self.mmap, *lh).length()))
                        .ok_or(Error::NumericOverflow("advancing last_header"))?,
                )
            };
        } else {
            self.last_header = NonZeroUsize::new(HEADER_SIZE);
        }
        tracing::trace!("last_header={}", self.last_header.unwrap());
        Ok(())
    }

    fn write_level_zero(&mut self) -> Fallible<()> {
        let _span = tracing::trace_span!("write", level=%0u32).entered();

        let index = (self.config.summarise_leaf)(self.uncompressed.iter()?);
        let index_len = u32::try_from(index.len()).expect("index size greater 4GiB!");
        let mut enc = Encoder::new(Vec::new(), 21).ctx("creating zstd encoder")?;
        enc.write_all(&index_len.to_be_bytes())
            .ctx("writing to buffer")?;
        enc.write_all(&*index).ctx("writing to buffer")?;
        let mut count = 0;
        let mut size = 0;
        for frame in &mut self.uncompressed.iter()? {
            count += 1u32;
            let data_len = u32::try_from(frame.data.len()).expect("event size greater 4GiB!");
            size += u64::from(data_len);
            enc.write_all(&data_len.to_be_bytes())
                .ctx("writing to buffer")?;
            enc.write_all(frame.data).ctx("writing to buffer")?;
        }
        tracing::trace!(index=%index_len, count, size, "uncompressed");

        let payload = enc.finish().ctx("finish compression")?;
        let payload_len = u32::try_from(payload.len()).expect("compression result greater 4GiB!");
        let padding = (!payload_len + 1) & 7;
        let length = size_of::<BlockHeader>() as u32
            + payload_len
            + padding
            + size_of::<BlockTrailer>() as u32;
        let previous = self
            .last_header
            .map(|lh| usize_to_u64(lh.get() - HEADER_SIZE) + self.offset)
            .unwrap_or(u64::MAX);
        let header = BlockHeader::new(length, 0, previous);
        let trailer = BlockTrailer::new(padding as u8, length);
        tracing::trace!(payload=%payload_len, padding, length, previous, "compressed");
        let padding = &[0u8; 7][..padding as usize];
        write_all_vectored(
            &mut self.main,
            [header.as_slice(), &*payload, padding, trailer.as_slice()],
        )
        .ctx(&*self.path)?;
        self.advance_mmap_last_header()?;
        self.uncompressed
            .truncate(self.offset + usize_to_u64(self.mmap.len() - HEADER_SIZE))?;
        Ok(())
    }

    fn compose_upper_level(
        &self,
        blocks: [&BlockHeader; FANOUT],
        level: u32,
    ) -> Fallible<(BlockHeader, Vec<u8>, u32, BlockTrailer)> {
        let _span = tracing::trace_span!("write", level).entered();

        let mut decompressed = blocks.map(|d| self.decompress(d, level > 1));
        let mut input = [&[][..]; FANOUT];
        for idx in 0..FANOUT {
            if decompressed[idx].is_err() {
                replace(&mut decompressed[idx], Err(Error::NumericOverflow("dummy")))?;
                unreachable!()
            }
            let decomp = decompressed[idx].as_deref().unwrap();
            if level == 1 {
                let end = u32_to_usize(u32::from_be_bytes(decomp[..4].try_into().unwrap())) + 4;
                input[idx] = &decomp[4..end];
            } else {
                input[idx] = decomp;
            }
        }
        let index = (self.config.summarise_index)(NodeType::from_level(level - 1), input);
        let payload = zstd::encode_all(&*index, 21).ctx("encoding index")?;
        let payload_len = u32::try_from(payload.len()).expect("compression result greater 4GiB!");
        let padding = (!payload_len + 1) & 7;
        let length = size_of::<BlockHeader>() as u32
            + payload_len
            + padding
            + size_of::<BlockTrailer>() as u32;
        let previous = blocks[0].previous();
        let header = BlockHeader::new(length, level, previous);
        let trailer = BlockTrailer::new(padding as u8, length);
        tracing::trace!(payload=%payload_len, padding, length, previous, "compressed");
        Ok((header, payload, padding, trailer))
    }

    fn decompress(&self, header: &BlockHeader, prio: bool) -> Fallible<Arc<[u8]>> {
        let key = (self.id, self.block_offset(header));
        let bytes = self.config.cache.borrow_mut().get(key);
        if let Some(bytes) = bytes {
            tracing::trace!(?key, "cache hit");
            Ok(bytes)
        } else {
            tracing::trace!(?key, prio, "cache miss");
            let bytes = zstd::decode_all(header.data()).ctx("decompressing index")?;
            let bytes = Arc::<[u8]>::from(bytes);
            self.config.cache.borrow_mut().put(key, bytes.clone(), prio);
            Ok(bytes)
        }
    }
}

fn header_at(mmap: &Mmap, pos: NonZeroUsize) -> &BlockHeader {
    unsafe { &*BlockHeader::from_ptr(mmap.as_ptr().add(pos.get())) }
}

pub struct SearchIter<'a> {
    start: *const u8,
    offset: u64,
    pos: Option<&'a BlockHeader>,
}

impl<'a> Iterator for SearchIter<'a> {
    type Item = &'a BlockHeader;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.pos;
        if let Some(pos) = self.pos {
            let prev = pos.previous();
            if prev == u64::MAX || prev < self.offset {
                self.pos = None;
            } else {
                self.pos = Some(unsafe {
                    &*BlockHeader::from_ptr(self.start.add((prev - self.offset) as usize))
                });
            }
        }
        ret
    }
}

struct LeafIter<F: FnMut(&[u8]) -> Fallible<T>, T> {
    extractor: Rc<RefCell<F>>,
    block: Arc<[u8]>,
    pos: usize,
    idx: u32,
    selected: SmallVec<[u32; FANOUT]>,
}

impl<F: FnMut(&[u8]) -> Fallible<T>, T> Iterator for LeafIter<F, T> {
    type Item = Fallible<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.block.len() {
            return None;
        }
        let skip = match self.selected.binary_search(&self.idx) {
            Ok(_) => 0u32,
            Err(idx) => {
                if idx >= self.selected.len() {
                    self.pos = self.block.len();
                    return None;
                }
                self.selected[idx] - self.idx
            }
        };
        for _ in 0..skip {
            let data_len = u32_to_usize(u32::from_be_bytes(
                self.block[self.pos..self.pos + 4].try_into().unwrap(),
            ));
            self.pos += 4 + data_len;
            self.idx += 1;
            if self.pos > self.block.len() {
                return Some(Err(Error::DataCorruption {
                    message: "skipped frame extends beyond end",
                    found: usize_to_u64(self.pos),
                    expected: usize_to_u64(self.block.len()),
                }));
            }
        }
        let data_len = u32_to_usize(u32::from_be_bytes(
            self.block[self.pos..self.pos + 4].try_into().unwrap(),
        ));
        let next = self.pos + 4 + data_len;
        if next > self.block.len() {
            self.pos = next;
            return Some(Err(Error::DataCorruption {
                message: "frame extends beyond end",
                found: usize_to_u64(next),
                expected: usize_to_u64(self.block.len()),
            }));
        }
        let data = &self.block[self.pos + 4..next];
        self.pos = next;
        self.idx += 1;
        Some((self.extractor.borrow_mut())(data))
    }
}

struct UncompressedIter<F> {
    extractor: Rc<RefCell<F>>,
    selected: SmallVec<[u32; 16]>,
    iter: EventFileIter,
    idx: u32,
}

impl<F: FnMut(&[u8]) -> Fallible<T>, T> Iterator for UncompressedIter<F> {
    type Item = Fallible<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self
            .selected
            .last()
            .copied()
            .into_iter()
            .all(|x| x < self.idx)
        {
            return None;
        }
        let next = self.selected[self.selected.binary_search(&self.idx).unwrap_or_else(|x| x)];
        while self.idx < next {
            (&mut self.iter).next();
            self.idx += 1;
        }
        let data = (&mut self.iter).next()?;
        self.idx += 1;
        Some((self.extractor.borrow_mut())(data.data))
    }
}

enum StreamIterState<'a, F1, F2, T>
where
    F2: FnMut(&[u8]) -> Fallible<T>,
{
    NotStarted,
    Branch(Box<StreamIter<'a, F1, F2, T>>),
    Leaf(LeafIter<F2, T>),
    Uncompressed(UncompressedIter<F2>),
    Failed,
}

pub struct StreamIter<'a, F1, F2, T>
where
    F2: FnMut(&[u8]) -> Fallible<T>,
{
    stream: &'a Stream,
    selector: Rc<RefCell<F1>>,
    extractor: Rc<RefCell<F2>>,
    todo: SmallVec<[&'a BlockHeader; 16]>,
    state: StreamIterState<'a, F1, F2, T>,
    uncompressed: Option<(Vec<u8>, EventFileIter)>,
}

impl<'a, F1, F2, T> StreamIter<'a, F1, F2, T>
where
    F2: FnMut(&[u8]) -> Fallible<T>,
{
    pub fn new(stream: &'a mut Stream, selector: F1, extractor: F2) -> Fallible<Self> {
        let _span = tracing::trace_span!("StreamIter::new", id = stream.id).entered();
        let uncompressed = stream.uncompressed.iter()?;
        let index = if let Some(index) = &stream.uncompressed_index {
            index.clone()
        } else {
            let index = (stream.config.summarise_leaf)(uncompressed.clone());
            stream.uncompressed_index = Some(index.clone());
            index
        };
        Ok(Self {
            stream,
            selector: Rc::new(RefCell::new(selector)),
            extractor: Rc::new(RefCell::new(extractor)),
            todo: stream.search_iter().collect(),
            state: StreamIterState::NotStarted,
            uncompressed: Some((index, uncompressed)),
        })
    }
}

macro_rules! handle_err {
    ($e:expr, $f:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                $f;
                return Some(Err(e));
            }
        }
    };
}

impl<'a, F1, F2, T> Iterator for StreamIter<'a, F1, F2, T>
where
    F1: FnMut(NodeType, &[u8]) -> Fallible<SmallVec<[u32; FANOUT]>>,
    F2: Fn(&[u8]) -> Fallible<T>,
{
    type Item = Fallible<T>;

    fn next(&mut self) -> Option<Self::Item> {
        use StreamIterState::Failed;

        let _span = tracing::trace_span!("StreamIter::next", id = self.stream.id).entered();
        loop {
            match self.state {
                StreamIterState::NotStarted => {
                    let block = match self.todo.pop() {
                        Some(b) => b,
                        None => {
                            if let Some((index, iter)) = self.uncompressed.take() {
                                let mut selected = handle_err!(
                                    (self.selector.borrow_mut())(NodeType::Leaf, &*index),
                                    self.state = Failed
                                );
                                selected.sort_unstable();
                                if selected.is_empty() {
                                    tracing::trace!("selecting none from uncompressed");
                                } else {
                                    tracing::trace!(
                                        num = selected.len(),
                                        min = selected[0],
                                        max = selected[selected.len() - 1],
                                        "selecting from uncompressed"
                                    );
                                }
                                self.state = StreamIterState::Uncompressed(UncompressedIter {
                                    extractor: self.extractor.clone(),
                                    selected,
                                    iter,
                                    idx: 0,
                                });
                                continue;
                            } else {
                                return None;
                            }
                        }
                    };
                    let node_type = NodeType::from_level(block.level());
                    match node_type {
                        NodeType::Branch => {
                            let decomp = handle_err!(
                                self.stream.decompress(block, true),
                                self.state = Failed
                            );
                            let mut selected = handle_err!(
                                (self.selector.borrow_mut())(node_type, decomp.as_ref()),
                                self.state = Failed
                            );
                            selected.sort_unstable();
                            if selected.is_empty() {
                                tracing::trace!(
                                    level = block.level(),
                                    "selecting none from branch"
                                );
                            } else {
                                tracing::trace!(
                                    level = block.level(),
                                    num = selected.len(),
                                    min = selected[0],
                                    max = selected[selected.len() - 1],
                                    "selecting from branch"
                                );
                            }
                            let prev = self.stream.prev_block(block)?;
                            let todo = (0..FANOUT as u32)
                                .rev()
                                // this ensures that at max FANOUT blocks are visited, i.e. only same level
                                .zip(self.stream.search_from(Some(prev)))
                                .filter_map(|(idx, block)| {
                                    selected.binary_search(&idx).ok().map(|_| block)
                                })
                                .collect();
                            self.state = StreamIterState::Branch(Box::new(Self {
                                stream: self.stream,
                                selector: self.selector.clone(),
                                extractor: self.extractor.clone(),
                                todo,
                                state: StreamIterState::NotStarted,
                                uncompressed: None,
                            }))
                        }
                        NodeType::Leaf => {
                            let decomp = handle_err!(
                                self.stream.decompress(block, false),
                                self.state = Failed
                            );
                            let index_len =
                                u32_to_usize(u32::from_be_bytes(decomp[..4].try_into().unwrap()));
                            let index_bytes = &decomp[4..index_len + 4];
                            let mut selected = handle_err!(
                                (self.selector.borrow_mut())(node_type, index_bytes),
                                self.state = Failed
                            );
                            selected.sort_unstable();
                            if selected.is_empty() {
                                tracing::trace!(
                                    level = block.level(),
                                    "selecting none from branch"
                                );
                            } else {
                                tracing::trace!(
                                    level = block.level(),
                                    num = selected.len(),
                                    min = selected[0],
                                    max = selected[selected.len() - 1],
                                    "selecting from branch"
                                );
                            }
                            self.state = StreamIterState::Leaf(LeafIter {
                                extractor: self.extractor.clone(),
                                block: decomp,
                                pos: index_len + 4,
                                idx: 0,
                                selected,
                            })
                        }
                    }
                }
                StreamIterState::Branch(ref mut b) => match b.next() {
                    Some(res) => return Some(res),
                    None => self.state = StreamIterState::NotStarted,
                },
                StreamIterState::Leaf(ref mut l) => match l.next() {
                    Some(res) => return Some(res),
                    None => self.state = StreamIterState::NotStarted,
                },
                StreamIterState::Uncompressed(ref mut u) => match u.next() {
                    Some(res) => return Some(res),
                    None => self.state = StreamIterState::NotStarted,
                },
                StreamIterState::Failed => return None,
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum NodeType {
    Branch,
    Leaf,
}

impl NodeType {
    fn from_level(level: u32) -> Self {
        if level == 0 {
            NodeType::Leaf
        } else {
            NodeType::Branch
        }
    }
}
