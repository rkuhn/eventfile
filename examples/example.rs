use eventfile::{Error, EventFile};
use tempfile::tempdir;

fn main() -> Result<(), Error> {
    let dir = tempdir().map_err(|e| ("tempdir", e))?;
    let mut f = EventFile::new(1, dir.path().join("file"), 0, 20, 20, Box::new(fbr_cache::FbrCache::new(1000)))?;
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
