use crate::{
    formats::{BlockHeader, BranchHeader, HasMagic, IndexEntry, LeafHeader, StagingHeader},
    u32_to_usize, usize_to_u64, Error, EventFile,
};
use std::io;

macro_rules! err {
    ($e:expr, $w:ident) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                writeln!($w, "error: {}", e)?;
                return Ok(());
            }
        }
    };
}

impl EventFile {
    pub fn dump_text(&self, lines_per_event: usize, mut w: impl io::Write) -> io::Result<()> {
        let file = &self.file;
        let head = err!(file.header(), w).lift();
        writeln!(
            w,
            "header: stream={} user={} start={} end={}",
            head.stream_version, head.user_version, head.start_offset, head.end_offset
        )?;
        let mut offset = head.start_offset;
        while offset < head.end_offset {
            let block = err!(file.stream_at::<BlockHeader>(offset), w);
            writeln!(
                w,
                "block @ {}: prev={} level={} length={}",
                offset,
                block.prev_block(),
                block.level(),
                block.length()
            )?;
            if block.level() == 0 {
                let leaf: &LeafHeader = err!(file.stream_after(block), w);
                writeln!(w, "  leaf: start={} count={}", leaf.start_idx(), leaf.count())?;
                let bytes = u32_to_usize(block.length()) - LeafHeader::LEN;
                let compressed = err!(file.stream_bytes_after(leaf, bytes), w);
                let decomp = err!(zstd::decode_all(compressed), w);
                for i in 0..=leaf.count() {
                    if i & 15 == 0 {
                        if i > 0 {
                            writeln!(w)?;
                        }
                        write!(w, "   ")?;
                    }
                    let off = get_u32_as_usize(&*decomp, i);
                    write!(w, " {}", off)?;
                }
                writeln!(w)?;
                let base = u32_to_usize(leaf.count() + 1) * 4;
                for i in 0..leaf.count() {
                    writeln!(w, "    event {}:", i)?;
                    let from = base + get_u32_as_usize(&*decomp, i);
                    let to = base + get_u32_as_usize(&*decomp, i + 1);
                    let event = err!(
                        decomp.get(from..to).ok_or_else(|| Error::data_corruption(
                            "event past end",
                            usize_to_u64(to),
                            usize_to_u64(decomp.len()),
                        )),
                        w
                    );
                    for line in 0..lines_per_event {
                        hex_dump(event, line, &mut w)?;
                    }
                }
            } else {
                let branch: &BranchHeader = err!(file.stream_after(block), w);
                writeln!(w, "  branch: prev={}, end={}", branch.prev_offset(), branch.end_idx())?;
                let entries = (u32_to_usize(block.length()) - BranchHeader::LEN) / IndexEntry::LEN;
                let mut prev = None;
                for i in 0..entries {
                    let idx: &IndexEntry = match prev {
                        Some(p) => err!(file.stream_after(p), w),
                        None => err!(file.stream_after(branch), w),
                    };
                    writeln!(w, "    {:2}: offset={} start={}", i, idx.offset(), idx.start_idx())?;
                    prev = Some(idx);
                }
            }
            offset += BlockHeader::SIZE + ((u64::from(block.length()) + 7) & !7);
        }
        writeln!(w, "---")?;
        let staging = err!(file.staging_at::<StagingHeader>(0), w);
        writeln!(
            w,
            "staging: last_block={} start={} count={} capacity={}",
            staging.last_block(),
            staging.start_idx(),
            staging.count(),
            staging.capacity()
        )?;
        let idx_bytes_end = StagingHeader::LEN + u32_to_usize(4 * staging.capacity());
        let idx_bytes = err!(file.staging_bytes(StagingHeader::LEN, idx_bytes_end), w);
        for i in 0..staging.capacity() {
            let offset = get_u32_as_usize(idx_bytes, i);
            if offset != 0 {
                writeln!(w, "  {:4}: {}", i, offset)?;
            }
        }
        let event_bytes = err!(file.staging_bytes(idx_bytes_end, file.staging_len()), w);
        for i in 0..staging.count() {
            writeln!(w, "  event {}:", i)?;
            let from = get_u32_as_usize(idx_bytes, i);
            let to = get_u32_as_usize(idx_bytes, i + 1);
            let event = err!(
                event_bytes.get(from..to).ok_or_else(|| Error::data_corruption(
                    "event past end",
                    usize_to_u64(to),
                    usize_to_u64(event_bytes.len()),
                )),
                w
            );
            for line in 0..lines_per_event {
                hex_dump(event, line, &mut w)?;
            }
        }
        Ok(())
    }
}

fn get_u32_as_usize(bytes: &[u8], idx: u32) -> usize {
    let pos = u32_to_usize(4 * idx);
    u32_to_usize(u32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap()))
}

fn hex_dump(bytes: &[u8], line: usize, w: &mut impl io::Write) -> io::Result<()> {
    if bytes.len() <= line * 16 {
        return Ok(());
    }
    write!(w, "  ")?;
    for i in line * 16..(line + 1) * 16 {
        if i & 3 == 0 {
            write!(w, " ")?;
        }
        if let Some(b) = bytes.get(i) {
            write!(w, " {:02x}", b)?;
        } else {
            write!(w, "   ")?;
        }
    }
    write!(w, "    ")?;
    for i in line * 16..(line + 1) * 16 {
        if let Some(b) = bytes.get(i) {
            if (32..127).contains(b) {
                write!(w, "{}", *b as char)?;
            } else {
                write!(w, ".")?;
            }
        }
    }
    writeln!(w)?;
    Ok(())
}
