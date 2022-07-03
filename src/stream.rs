use crate::{
    error::{ErrCtx, Result},
    formats::{BlockHeader, BlockTrailer, StreamHeader},
    io::write_all_vectored,
    Error, EventFile, EventFileIter, EventFrame,
};
use memmap2::{Mmap, MmapOptions};
use smallvec::SmallVec;
use std::{
    fs::File,
    io::{Read, Write},
    mem::{replace, size_of},
    ops::Range,
    path::PathBuf,
    slice::from_raw_parts,
};
use zstd::stream::Encoder;

pub struct Stream {
    config: StreamConfig,
    path: PathBuf,
    main: File,
    uncompressed: EventFile,
    version: u32,
    offset: u64,
    last_header: Option<usize>,
    mmap: Mmap,
}

const FANOUT: usize = 16;

pub struct StreamConfig {
    compression_threshold: u64,
    signature_key: [u8; ed25519_dalek::SECRET_KEY_LENGTH],
    summarise_leaf: fn(EventFileIter) -> Vec<u8>,
    summarise_index: fn([&[u8]; FANOUT]) -> Vec<u8>,
}

impl StreamConfig {
    pub fn new(
        summarise_leaf: fn(EventFileIter) -> Vec<u8>,
        summarise_index: fn([&[u8]; FANOUT]) -> Vec<u8>,
        signature_key: [u8; ed25519_dalek::SECRET_KEY_LENGTH],
    ) -> Self {
        Self {
            compression_threshold: 100_000,
            summarise_leaf,
            summarise_index,
            signature_key,
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
///  - header (see HEADER_LEN)
///  - blocks
///
/// block format:
///  - total length: u32
///  - level: u32
///  - previous: u64 (offset of level same or higher, 0 if none)
///  - compressed payload
///  - padding bytes
///  - padding: u8
///  - (3 bytes unused)
///  - total length: u32
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
    pub fn new(config: StreamConfig, path: impl Into<PathBuf>, version: u32) -> Result<Self> {
        let mut path: PathBuf = path.into();
        path.set_extension("main");
        let mut main = File::options()
            .append(true)
            .create(true)
            .read(true)
            .open(&*path)
            .ctx(&*path)?;
        let mut offset = 0;
        if main.metadata().ctx(&*path)?.len() < HEADER_LEN {
            main.set_len(0).ctx(&*path)?;
            let header = StreamHeader::new(VERSION_1, version, 0);
            main.write_all(header.as_slice()).ctx(&*path)?;
        } else {
            let mut bytes = [0u8; size_of::<StreamHeader>()];
            main.read_exact(&mut bytes).ctx(&*path)?;
            let header = StreamHeader::from_slice(&bytes);
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
        let mmap = unsafe { MmapOptions::new().offset(HEADER_LEN).map(&main) }.ctx(&*path)?;
        let last_header = if mmap.is_empty() {
            None
        } else {
            let Range { start, end } = mmap.as_ptr_range();
            let trailer = unsafe { &*BlockTrailer::from_ptr(end.sub(size_of::<BlockTrailer>())) };
            Some(unsafe { end.offset_from(start) } as usize - trailer.length() as usize)
        };
        let uncompressed = EventFile::open(
            path.with_extension("uncompressed"),
            offset
                .checked_add(u64::try_from(mmap.len()).ctx("fitting file length into u64")?)
                .ok_or(Error::NumericOverflow("fitting stream length into u64"))?,
        )?;
        Ok(Self {
            config,
            path,
            main,
            uncompressed,
            version,
            offset,
            last_header,
            mmap,
        })
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn append(&mut self, frame: EventFrame<'_>) -> Result<()> {
        self.uncompressed.append(frame)?;
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
                self.mmap = unsafe { MmapOptions::new().offset(HEADER_LEN).map(&self.main) }
                    .ctx(&*self.path)?;
                let advance = self.last_header().unwrap().len();
                *self.last_header.as_mut().unwrap() += advance;
            }
        }
        Ok(())
    }

    pub fn iter<F1, F2, T>(&self, mut selector: F1, extractor: F2) -> StreamIter<'_, F1, F2, T>
    where
        F1: FnMut(Node, &[u8]) -> SmallVec<[u16; FANOUT]>,
        F2: Fn(&[u8]) -> T,
    {
        let last_header = self.last_header();
        let indices = last_header
            .map(|h| selector(Node::from_level(h.level()), h.data()))
            .unwrap_or_default();
        StreamIter {
            selector,
            extractor,
            pos: last_header.into_iter().collect(),
            indices,
        }
    }

    fn search_iter(&self) -> SearchIter<'_> {
        SearchIter {
            start: self.mmap.as_ptr(),
            offset: self.offset,
            pos: self.last_header(),
        }
    }

    fn last_header(&self) -> Option<&BlockHeader> {
        self.last_header
            .map(|head| unsafe { &*BlockHeader::from_ptr(self.mmap.as_ptr().add(head)) })
    }

    fn header_offset(&self, header: &BlockHeader) -> u64 {
        unsafe { header.as_ptr().offset_from(self.mmap.as_ptr()) }
            .try_into()
            .unwrap()
    }

    fn last_header_offset(&self) -> Option<u64> {
        self.last_header().map(|h| self.header_offset(h))
    }

    fn write_level_zero(&mut self) -> Result<()> {
        let index = (self.config.summarise_leaf)(self.uncompressed.iter()?);
        let mut enc = Encoder::new(Vec::new(), 21).ctx("creating zstd encoder")?;
        enc.write_all(&index.len().to_be_bytes())
            .ctx("writing to buffer")?;
        enc.write_all(&*index).ctx("writing to buffer")?;
        for frame in &mut self.uncompressed.iter()? {
            enc.write_all(&frame.data.len().to_be_bytes())
                .ctx("writing to buffer")?;
            enc.write_all(frame.data).ctx("writing to buffer")?;
        }
        let payload = enc.finish().ctx("finish compression")?;
        let payload_len = payload.len() as u32;
        let padding = (!payload_len + 1) & 7;
        let length = size_of::<BlockHeader>() as u32
            + payload_len
            + padding
            + size_of::<BlockTrailer>() as u32;
        let previous = self.last_header_offset().unwrap_or_default();
        let header = BlockHeader::new(length, 0, previous);
        let trailer = BlockTrailer::new(padding as u8, length);
        let padding = &[0u8; 7][..padding as usize];
        write_all_vectored(
            &mut self.main,
            [header.as_slice(), &*payload, padding, trailer.as_slice()],
        )
        .ctx(&*self.path)?;
        self.mmap =
            unsafe { MmapOptions::new().offset(HEADER_LEN).map(&self.main) }.ctx(&*self.path)?;
        self.last_header = self
            .last_header()
            .map(|h| self.last_header.unwrap_or_default() + h.len())
            .or(Some(0));
        self.uncompressed
            .truncate(self.offset + self.mmap.len() as u64)?;
        Ok(())
    }

    fn compose_upper_level(
        &self,
        blocks: [&BlockHeader; 16],
        level: u32,
    ) -> Result<(BlockHeader, Vec<u8>, u32, BlockTrailer)> {
        let mut decompressed = blocks.map(|d| zstd::decode_all(d.data()));
        let mut input = [&[][..]; FANOUT];
        for idx in 0..FANOUT {
            if decompressed[idx].is_err() {
                replace(&mut decompressed[idx], Ok(Vec::new())).ctx("decompressing index")?;
                unreachable!()
            }
            if level == 1 {
                input[idx] = decompressed[idx]
                    .as_deref()
                    .map(|bytes| {
                        let end = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize + 4;
                        &bytes[4..end]
                    })
                    .unwrap();
            } else {
                input[idx] = decompressed[idx].as_deref().unwrap();
            }
        }
        let index = (self.config.summarise_index)(input);
        let payload = zstd::encode_all(&*index, 21).ctx("encoding index")?;
        let payload_len = payload.len() as u32;
        let padding = (!payload_len + 1) & 7;
        let length = size_of::<BlockHeader>() as u32
            + payload_len
            + padding
            + size_of::<BlockTrailer>() as u32;
        let previous = blocks[0].previous();
        let header = BlockHeader::new(length, 0, previous);
        let trailer = BlockTrailer::new(padding as u8, length);
        Ok((header, payload, padding, trailer))
    }
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
            if prev == 0 || prev < self.offset {
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

pub struct StreamIter<'a, F1, F2, T>
where
    F2: Fn(&[u8]) -> T,
{
    selector: F1,
    extractor: F2,
    pos: SmallVec<[&'a BlockHeader; FANOUT]>,
    indices: SmallVec<[u16; FANOUT]>,
}

impl<'a, F1, F2, T> Iterator for StreamIter<'a, F1, F2, T>
where
    F1: FnMut(Node, &[u8]) -> SmallVec<[u16; FANOUT]>,
    F2: Fn(&[u8]) -> T,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

pub enum Node {
    Branch,
    Leaf,
}

impl Node {
    pub fn from_level(level: u32) -> Self {
        if level == 0 {
            Node::Leaf
        } else {
            Node::Branch
        }
    }
}
