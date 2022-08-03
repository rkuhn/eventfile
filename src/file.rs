use crate::{
    error::{ErrCtx, Fallible},
    formats::FileHeader,
    io::write_all_vectored,
    usize_to_u64, Error,
};
use memmap2::Mmap;
use std::{
    fs::File,
    io::{Read, Write},
    mem::size_of,
    ops::Range,
    path::PathBuf,
    sync::Arc,
};

#[repr(transparent)]
struct SigLenPad(u16);

impl std::fmt::Debug for SigLenPad {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigLenPad")
            .field("sig", &self.sig_len())
            .field("pad", &self.padding())
            .finish()
    }
}
impl SigLenPad {
    pub fn new(sig_len: usize, padding: usize) -> Self {
        debug_assert!(sig_len < 0x3fff);
        debug_assert!(padding < 4);
        Self(u16::to_be(((sig_len as u16) << 2) | (padding as u16)))
    }
    pub fn sig_len(&self) -> usize {
        (u16::from_be(self.0) >> 2).into()
    }
    pub fn padding(&self) -> usize {
        (u16::from_be(self.0) & 3).into()
    }
}

#[repr(C)]
struct EventFrameIo<T: ?Sized> {
    len: u32,
    sig_len_pad: SigLenPad,
    data: T,
}

impl<T: ?Sized> std::fmt::Debug for EventFrameIo<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventFrameIo")
            .field("at", &(self as *const _))
            .field("len", &self.len())
            .field("sig", &self.sig_len())
            .field("pad", &self.padding())
            .finish()
    }
}
const FRAME_HEADER_LEN: usize = 6;
impl EventFrameIo<()> {
    pub fn header(sig_len: usize, data_len: usize) -> Self {
        let total = FRAME_HEADER_LEN + sig_len + data_len;
        let pad = (!total + 1) & 3;
        let len = total + pad;
        Self {
            len: u32::to_be(len.try_into().expect("4GiB max frame size")),
            sig_len_pad: SigLenPad::new(sig_len, pad),
            data: (),
        }
    }
}
impl<T: ?Sized> EventFrameIo<T> {
    pub fn len(&self) -> usize {
        u32::from_be(self.len) as usize
    }
    pub fn sig_len(&self) -> usize {
        self.sig_len_pad.sig_len()
    }
    pub fn padding(&self) -> usize {
        self.sig_len_pad.padding()
    }
    pub fn slice_len(&self) -> usize {
        self.len() - self.padding() - FRAME_HEADER_LEN
    }
    pub fn header_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const Self as *const u8, FRAME_HEADER_LEN) }
    }
}
impl EventFrameIo<[u8]> {
    pub fn at(ptr: *const u8) -> *const Self {
        let no_data = unsafe { &*(ptr as *const EventFrameIo<()>) };
        tracing::trace!("EventFrame @ {:?}", no_data);
        let data_len = no_data.slice_len();
        let data = unsafe { std::slice::from_raw_parts(ptr, data_len) };
        data as *const [u8] as *const EventFrameIo<[u8]>
    }
    pub fn signature(&self) -> &[u8] {
        &self.data[..self.sig_len()]
    }
    pub fn data(&self) -> &[u8] {
        &self.data[self.sig_len()..]
    }
}

pub struct EventFrame<'a> {
    pub data: &'a [u8],
    pub signature: &'a [u8],
}

impl<'a> EventFrame<'a> {
    pub fn new(data: &'a [u8], signature: &'a [u8]) -> Self {
        Self { data, signature }
    }
}

pub struct EventFile {
    file: File,
    path: PathBuf,
    offset: u64,
    len: u64,
}

/// offset: u64
const FILE_HEADER_LEN: usize = size_of::<u64>();

impl EventFile {
    pub fn open(path: impl Into<PathBuf>, expected_offset: u64) -> Fallible<Self> {
        let path = path.into();
        tracing::debug!(path = %path.display(), "EventFile::open");
        let mut file = File::options()
            .create(true)
            .append(true)
            .read(true)
            .open(&*path)
            .ctx(&*path)?;
        let len = file.metadata().ctx(&*path)?.len();
        if len < (FILE_HEADER_LEN as u64) {
            let mut ret = Self {
                file,
                path,
                offset: expected_offset,
                len: 0,
            };
            ret.write_header()?;
            Ok(ret)
        } else {
            let mut buf = [0u8; FILE_HEADER_LEN];
            file.read_exact(&mut buf).ctx(&*path)?;
            let offset = u64::from_be_bytes(buf);
            if offset != expected_offset {
                return Err(Error::WrongOffset {
                    expected: expected_offset,
                    found: offset,
                });
            }
            Ok(Self {
                file,
                path,
                offset: expected_offset,
                len: len - FILE_HEADER_LEN as u64,
            })
        }
    }

    fn write_header(&mut self) -> Fallible<()> {
        tracing::trace!(path = %self.path.display(), "EventFile::write_header");
        self.file.set_len(0).ctx(&*self.path)?;
        let header = FileHeader::new(self.offset);
        self.file.write_all(header.as_slice()).ctx(&*self.path)?;
        Ok(())
    }

    pub fn append(&mut self, frame: EventFrame<'_>) -> Fallible<()> {
        let header = EventFrameIo::header(frame.signature.len(), frame.data.len());
        let padding = [0u8; 4];
        let bufs = [
            header.header_slice(),
            frame.signature,
            frame.data,
            &padding[..header.padding()],
        ];
        tracing::trace!(sig=%frame.signature.len(), data=%frame.data.len(), padding=header.padding(), "uncompressed");
        self.len += bufs.iter().map(|x| usize_to_u64(x.len())).sum::<u64>();
        write_all_vectored(&mut self.file, bufs).ctx(&*self.path)?;
        Ok(())
    }

    pub fn sync(&mut self) -> Fallible<()> {
        tracing::trace!(path = %self.path.display(), "EventFile::sync");
        self.file.sync_data().ctx(&*self.path)?;
        Ok(())
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> Fallible<EventFileIter> {
        let mmap = Arc::new(unsafe { Mmap::map(&self.file) }.ctx(&*self.path)?);
        let Range {
            start: mut pos,
            end,
        } = mmap.as_ptr_range();
        pos = unsafe { pos.add(FILE_HEADER_LEN) };
        tracing::trace!(pos=?pos, end=?end, path=%self.path.display(), "EventFile::iter");
        Ok(EventFileIter { mmap, pos, end })
    }

    pub fn truncate(&mut self, new_offset: u64) -> Fallible<()> {
        tracing::debug!(offset = new_offset, "truncating");
        self.len = 0;
        self.offset = new_offset;
        self.write_header()?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct EventFileIter {
    #[allow(dead_code)]
    // this keeps the mapping alive while we iterate
    mmap: Arc<Mmap>,
    pos: *const u8,
    end: *const u8,
}
impl<'a> Iterator for &'a mut EventFileIter {
    type Item = EventFrame<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.end {
            None
        } else {
            let frame = unsafe { &*EventFrameIo::at(self.pos) };
            self.pos = unsafe { self.pos.add(frame.len()) };
            Some(EventFrame {
                data: frame.data(),
                signature: frame.signature(),
            })
        }
    }
}
