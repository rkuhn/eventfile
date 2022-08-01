use rand::{thread_rng, Rng, RngCore};

fn mk_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    thread_rng().fill_bytes(&mut key[..]);
    key
}

const N: u32 = 2000;

fn main() {
    let keys = [mk_key(), mk_key(), mk_key()];
    let mut data = vec![];
    let mut scratch = [0u8; 10];
    for i in 0..2000u32 {
        data.extend_from_slice(keys[thread_rng().gen_range(0..keys.len())].as_slice());
        data.extend_from_slice(&i.to_be_bytes()[..]);
        let len = thread_rng().gen_range(0..scratch.len());
        let bytes = &mut scratch[..len];
        thread_rng().fill_bytes(bytes);
        data.extend_from_slice(bytes);
    }
    println!(
        "total={} per-frame-random={:.3}",
        data.len(),
        (data.len() as f64 / N as f64) - 36.0
    );
    let compressed = zstd::encode_all(&*data, 21).unwrap();
    println!(
        "compressed={} per-frame={:.3}",
        compressed.len(),
        compressed.len() as f64 / N as f64
    );
}

// Conclusion
//
// Using 256bit log names is not a problem, zstd recognises repetition very well.
