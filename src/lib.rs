#![allow(unused)]
use memmap2::Mmap;
use std::{
    fs::File,
    io::{IoSlice, Write},
    mem::{align_of, size_of},
    ops::Range,
    path::{Path, PathBuf},
};

#[derive(Debug, thiserror::Error, derive_more::From)]
pub enum Error {
    #[error("{0}: {1}")]
    Io(PathBuf, std::io::Error),
}
impl From<(&Path, std::io::Error)> for Error {
    fn from(pair: (&Path, std::io::Error)) -> Self {
        Error::Io(pair.0.to_owned(), pair.1)
    }
}

trait ErrCtx {
    type Output;
    type Error;
    fn ctx<T>(self, t: T) -> std::result::Result<Self::Output, (T, Self::Error)>;
}
impl<O, E> ErrCtx for std::result::Result<O, E> {
    type Output = O;
    type Error = E;

    fn ctx<T>(self, t: T) -> std::result::Result<Self::Output, (T, Self::Error)> {
        self.map_err(|e| (t, e))
    }
}
type Result<T> = std::result::Result<T, Error>;

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
            .field("len", &self.len())
            .field("sig_len_pad", &self.sig_len_pad)
            .finish()
    }
}
const HEADER_LEN: usize = 6;
impl EventFrameIo<()> {
    pub fn header(sig_len: usize, data_len: usize) -> Self {
        let total = HEADER_LEN + sig_len + data_len;
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
        self.len() - self.padding() - HEADER_LEN
    }
    pub fn data_len(&self) -> usize {
        self.len() - self.sig_len() - self.padding() - HEADER_LEN
    }
    pub fn header_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const Self as *const u8, HEADER_LEN) }
    }
}
impl EventFrameIo<[u8]> {
    pub fn at(ptr: *const u8) -> *const Self {
        let no_data = unsafe { &*(ptr as *const EventFrameIo<()>) };
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
}

impl EventFile {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = File::options()
            .create(true)
            .append(true)
            .read(true)
            .open(&*path)
            .ctx(&*path)?;
        Ok(Self { file, path })
    }
    pub fn append(&mut self, frame: EventFrame<'_>) -> Result<()> {
        let header = EventFrameIo::header(frame.signature.len(), frame.data.len());
        let padding = [0u8; 4];
        let bufs = vec![
            IoSlice::new(header.header_slice()),
            IoSlice::new(frame.signature),
            IoSlice::new(frame.data),
            IoSlice::new(&padding[..header.padding()]),
        ];
        self.file.write_vectored(&*bufs).ctx(&*self.path)?;
        Ok(())
    }
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data().ctx(&*self.path)?;
        Ok(())
    }
    pub fn iter(&self) -> Result<EventFileIter> {
        let mmap = unsafe { Mmap::map(&self.file) }.ctx(&*self.path)?;
        let Range { start: pos, end } = mmap.as_ptr_range();
        Ok(EventFileIter { mmap, pos, end })
    }
}

pub struct EventFileIter {
    mmap: Mmap,
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
