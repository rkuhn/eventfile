use eventfile::{Error, EventFile, EventFrame};

fn main() -> Result<(), Error> {
    let mut f = EventFile::open("file", 0)?;
    f.append(EventFrame::new(b"12345", b"abcd"))?;
    f.append(EventFrame::new(b"a12345", b"abcd567"))?;
    f.append(EventFrame::new(b".12345", b"abcddfghd"))?;
    for ev in &mut f.iter()? {
        println!("sig: {} data: {}", ev.signature.len(), ev.data.len());
    }
    Ok(())
}
