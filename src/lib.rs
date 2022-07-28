mod cache;
mod error;
mod file;
mod formats;
mod io;
mod stream;

pub use cache::Cache;
pub use error::Error;
pub use file::{EventFile, EventFileIter, EventFrame};
pub use stream::{NodeType, Stream, StreamConfig, FANOUT};

macro_rules! embed {
    ($($f:ident: $from:ty => $to:ty;)*) => {
        $(
            const fn $f(x: $from) -> $to {
                #[allow(dead_code)]
                const ASSERT: () = {
                    if std::mem::size_of::<$from>() > std::mem::size_of::<$to>() {
                        panic!(concat!("this library requires ", stringify!($to), " to be at least as wide as ", stringify!($from)));
                    }
                };
                x as $to
            }
        )*
    };
}

embed! {
    u8_to_u32: u8 => u32;
    u8_to_usize: u8 => usize;
    u32_to_usize: u32 => usize;
    usize_to_u64: usize => u64;
    isize_to_u64: isize => u64;
}
