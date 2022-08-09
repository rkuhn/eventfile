use eventfile::{Error, EventFile, EventFileConfig};
use tempfile::tempdir;

fn main() -> Result<(), Error> {
    let dir = tempdir().map_err(|e| ("tempdir", e))?;
    let mut f = EventFile::new(
        1,
        dir.path().join("file"),
        EventFileConfig::new(0).compression_threshold(20).block_event_limit(20),
    )?;
    f.dump_text(2, std::io::stdout()).unwrap();
    f.append(b"abcd")?;
    println!("***");
    f.dump_text(2, std::io::stdout()).unwrap();
    f.append(b"hello world!")?;
    println!("***");
    f.dump_text(2, std::io::stdout()).unwrap();
    f.append("Crazy Stüff".as_bytes())?;
    println!("***");
    f.dump_text(2, std::io::stdout()).unwrap();
    f.append(b"0123456789")?;
    println!("***");
    f.dump_text(2, std::io::stdout()).unwrap();
    Ok(())
}
