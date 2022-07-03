use std::{
    fs::File,
    io::{Error, ErrorKind, IoSlice, Write},
};

pub fn write_all_vectored<const N: usize>(
    file: &mut File,
    mut bufs: [&[u8]; N],
) -> std::io::Result<()> {
    let mut bufs = &mut bufs[..];
    // Guarantee that bufs is empty if it contains no data,
    // to avoid calling write_vectored if there is no data to be written.
    advance_slices(&mut bufs, 0);
    while !bufs.is_empty() {
        let mut iov = [IoSlice::new(&[]); N];
        for (idx, buf) in bufs.iter().enumerate() {
            iov[idx] = IoSlice::new(*buf);
        }
        match file.write_vectored(&iov[..bufs.len()]) {
            Ok(0) => {
                return Err(Error::new(
                    ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            Ok(n) => advance_slices(&mut bufs, n),
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn advance_slices<'a>(bufs: &mut &mut [&'a [u8]], n: usize) {
    // Number of buffers to remove.
    let mut remove = 0;
    // Total length of all the to be removed buffers.
    let mut accumulated_len = 0;
    for buf in bufs.iter() {
        if accumulated_len + buf.len() > n {
            break;
        } else {
            accumulated_len += buf.len();
            remove += 1;
        }
    }

    *bufs = &mut std::mem::take(bufs)[remove..];
    if !bufs.is_empty() {
        bufs[0] = &(bufs[0])[n - accumulated_len..];
    }
}
