use crate::{
    error::{ErrCtx, Fallible},
    formats::{HasMagic, MmapFileHeader},
    isize_to_u64, usize_to_u64, Error,
};
use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::{metadata, File},
    mem::{align_of, size_of},
    path::PathBuf,
    slice,
};

/// A file that contains:
///  - 4kiB header
///  - bytes named [start_offset..end_offset] (boundaries 8-byte aligned)
///  - unnamed bytes until the file end
pub struct MmapFile {
    path: PathBuf,
    file: File,
    mmap: MmapMut,
    start_offset: u64,
    end_offset: u64,
}

impl MmapFile {
    pub fn new(path: PathBuf, user_version: u32) -> Fallible<Self> {
        let file = File::options().create(true).read(true).write(true).open(&*path).ctx(&*path)?;
        let len = metadata(&path).ctx(&*path)?.len();
        if len < 4096 {
            if len > 0 {
                return Err(Error::data_corruption("non-empty file is too small", len, 4096));
            }
            file.set_len(4096).ctx(&*path)?;
        }
        let mmap = unsafe { MmapOptions::new().map_mut(&file) }.ctx(&*path)?;
        let mut ret = Self { path, file, mmap, start_offset: 0, end_offset: 0 };
        if len < 4096 {
            // we created the file
            ret.put(0, MmapFileHeader::new(1, user_version, 0, 0))?;
            ret.mmap.flush().ctx(&*ret.path)?;
        } else {
            let header = *ret.at::<MmapFileHeader>(0)?;
            if header.stream_version() != 1 {
                return Err(Error::wrong_stream_version(header.stream_version()));
            }
            if header.user_version() != user_version {
                return Err(Error::wrong_user_version(user_version, header.user_version()));
            }
            ret.start_offset = header.start_offset();
            ret.end_offset = header.end_offset();
        }
        Ok(ret)
    }

    pub fn header(&self) -> Fallible<&MmapFileHeader> {
        self.at(0)
    }

    pub fn flush(&self) -> Fallible<()> {
        Ok(self.mmap.flush().ctx("flushing")?)
    }

    pub fn end_offset(&self) -> u64 {
        self.end_offset
    }

    pub fn staging_len(&self) -> usize {
        self.mmap.len() - self.staging_start()
    }

    pub fn staging_start(&self) -> usize {
        4096 + (self.end_offset - self.start_offset) as usize
    }

    fn validate_range<T: HasMagic>(&self, offset: usize) -> Fallible<()> {
        if offset & (align_of::<T>() - 1) != 0 {
            return Err(Error::data_corruption("alignment error", usize_to_u64(offset), 0));
        }
        let end = offset + T::LEN;
        if end > self.mmap.len() {
            return Err(Error::data_corruption(
                "index beyond file end",
                usize_to_u64(end),
                usize_to_u64(self.mmap.len()),
            ));
        }
        Ok(())
    }

    fn at<T: HasMagic>(&self, offset: usize) -> Fallible<&T> {
        self.validate_range::<T>(offset)?;
        if self.mmap[offset..offset + T::MAGIC.len()] != T::MAGIC[..] {
            return Err(Error::data_corruption(
                "magic value not found",
                usize_to_u64(offset),
                u64::from_be_bytes(T::MAGIC.try_into().unwrap_or([0; 8])),
            ));
        }
        Ok(unsafe { &*(self.mmap.as_ptr().add(offset + T::MAGIC.len()) as *const T) })
    }

    fn at_mut<T: HasMagic>(&mut self, offset: usize) -> Fallible<&mut T> {
        self.validate_range::<T>(offset)?;
        if self.mmap[offset..offset + T::MAGIC.len()] != T::MAGIC[..] {
            return Err(Error::data_corruption(
                "magic value not found",
                usize_to_u64(offset),
                u64::from_be_bytes(T::MAGIC.try_into().unwrap_or([0; 8])),
            ));
        }
        Ok(unsafe { &mut *(self.mmap.as_ptr().add(offset + T::MAGIC.len()) as *mut T) })
    }

    fn put<T: HasMagic>(&mut self, offset: usize, value: T) -> Fallible<()> {
        self.validate_range::<T>(offset)?;
        self.mmap[offset..offset + T::MAGIC.len()].copy_from_slice(T::MAGIC);
        unsafe { std::ptr::write(self.mmap.as_ptr().add(offset + T::MAGIC.len()) as *mut T, value) };
        Ok(())
    }

    fn write(&mut self, offset: usize, bytes: &[u8]) -> Fallible<()> {
        let end = offset + bytes.len();
        if end > self.mmap.len() {
            return Err(Error::data_corruption(
                "writing beyond end of file",
                usize_to_u64(end),
                usize_to_u64(self.mmap.len()),
            ));
        }
        unsafe { slice::from_raw_parts_mut(self.mmap.as_mut_ptr().add(offset), bytes.len()) }.copy_from_slice(bytes);
        Ok(())
    }

    pub fn stream_at<T: HasMagic>(&self, offset: u64) -> Fallible<&T> {
        if offset < self.start_offset {
            return Err(Error::data_not_present("index before start offset", offset, self.start_offset));
        }
        let end = offset + T::SIZE;
        if end > self.end_offset {
            return Err(Error::data_not_present(
                "object reaching beyond end offset",
                offset,
                self.end_offset,
            ));
        }
        self.at((offset - self.start_offset + 4096) as usize)
    }

    pub fn stream_offset<T: HasMagic>(&self, at: &T) -> Fallible<u64> {
        let off = unsafe { (at as *const T as *const u8).offset_from(self.mmap.as_ptr()) };
        if off < 4096 {
            return Err(Error::data_corruption("invalid reference", isize_to_u64(off), 4096));
        }
        if off > self.staging_start() as isize {
            return Err(Error::data_corruption(
                "invalid reference",
                isize_to_u64(off),
                usize_to_u64(self.staging_start()),
            ));
        }
        Ok(off as u64 - 4096 + self.start_offset)
    }

    pub fn stream_after<T: HasMagic, U: HasMagic>(&self, at: &T) -> Fallible<&U> {
        let at_offset = unsafe { (at as *const T as *const u8).offset_from(self.mmap.as_ptr()) } as usize;
        // FIXME add range checks
        self.at(at_offset + size_of::<T>())
    }

    pub fn stream_bytes(&self, from: u64, to: u64) -> Fallible<&[u8]> {
        if from < self.start_offset {
            return Err(Error::data_not_present(
                "byte index before start offset",
                from,
                self.start_offset,
            ));
        }
        if to > self.end_offset {
            return Err(Error::data_not_present("byte index beyond stream end", to, self.end_offset));
        }
        if from > to {
            return Err(Error::numeric_overflow("negative range of stream_bytes requested"));
        }
        Ok(unsafe {
            slice::from_raw_parts(
                self.mmap.as_ptr().add(4096 + (from - self.start_offset) as usize),
                (to - from) as usize,
            )
        })
    }

    pub fn stream_bytes_after<T: HasMagic>(&self, at: &T, length: usize) -> Fallible<&[u8]> {
        let start = unsafe { (at as *const T as *const u8).offset_from(self.mmap.as_ptr()) } as u64 - 4096
            + self.start_offset
            + usize_to_u64(size_of::<T>());
        let end = start + usize_to_u64(length);
        self.stream_bytes(start, end)
    }

    /// CAUTION: this clobbers the staging area!
    pub fn stream_append<T: HasMagic>(&mut self, value: T) -> Fallible<()> {
        self.ensure_staging_len(T::LEN)?;
        self.put(self.staging_start(), value)?;
        self.end_offset += T::SIZE;
        let end = self.end_offset;
        self.at_mut::<MmapFileHeader>(0)?.set_end_offset(end);
        Ok(())
    }

    /// CAUTION: this clobbers the staging area!
    pub fn stream_append_bytes(&mut self, bytes: &[u8]) -> Fallible<()> {
        self.ensure_staging_len(bytes.len())?;
        self.write(self.staging_start(), bytes)?;
        self.end_offset += usize_to_u64(bytes.len() + 7) & !7;
        let end = self.end_offset;
        self.at_mut::<MmapFileHeader>(0)?.set_end_offset(end);
        Ok(())
    }

    pub fn clear_staging(&mut self) {
        let start = self.staging_start();
        self.mmap[start..].fill(0);
    }

    pub fn staging_at<T: HasMagic>(&self, offset: usize) -> Fallible<&T> {
        let end = offset + T::LEN;
        if end > self.staging_len() {
            return Err(Error::data_corruption(
                "index beyond staging end",
                usize_to_u64(end),
                usize_to_u64(self.staging_len()),
            ));
        }
        self.at(offset + self.staging_start())
    }

    pub fn staging_at_mut<T: HasMagic>(&mut self, offset: usize) -> Fallible<&mut T> {
        let end = offset + T::LEN;
        if end > self.staging_len() {
            return Err(Error::data_corruption(
                "index beyond staging end",
                usize_to_u64(end),
                usize_to_u64(self.staging_len()),
            ));
        }
        self.at_mut(offset + self.staging_start())
    }

    pub fn staging_bytes(&self, from: usize, to: usize) -> Fallible<&[u8]> {
        if to > self.staging_len() {
            return Err(Error::data_corruption(
                "byte index beyond staging end",
                usize_to_u64(to),
                usize_to_u64(self.staging_len()),
            ));
        }
        if from > to {
            return Err(Error::numeric_overflow("negative range of staging_bytes requested"));
        }
        Ok(unsafe { slice::from_raw_parts(self.mmap.as_ptr().add(self.staging_start() + from), to - from) })
    }

    pub fn ensure_staging_len(&mut self, len: usize) -> Fallible<()> {
        if self.staging_len() >= len {
            return Ok(());
        }
        let file_size = 4096 + self.end_offset - self.start_offset + usize_to_u64(len);
        self.file.set_len(file_size).ctx(&*self.path)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file) }.ctx(&*self.path)?;
        Ok(())
    }

    pub fn staging_put<T: HasMagic>(&mut self, offset: usize, value: T) -> Fallible<()> {
        self.put(self.staging_start() + offset, value)?;
        Ok(())
    }

    pub fn staging_write(&mut self, offset: usize, bytes: &[u8]) -> Fallible<()> {
        self.write(self.staging_start() + offset, bytes)
    }
}
