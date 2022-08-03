use crate::{
    error::Fallible,
    formats::{BlockHeader, BranchHeader, HasMagic},
    mmap::MmapFile,
};

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
        let ret = handle_err!(
            self.file.stream_at::<BlockHeader>(self.offset),
            self.offset = u64::MAX
        );
        let offset = self.offset;
        self.offset = if ret.level() == 0 {
            ret.prev_block()
        } else {
            let branch = handle_err!(
                self.file
                    .stream_at::<BranchHeader>(self.offset + BlockHeader::SIZE),
                self.offset = u64::MAX
            );
            branch.prev_offset()
        };
        Some(Ok((offset, ret)))
    }
}
