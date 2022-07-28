#![allow(unused)]

use crate::u32_to_usize;
use core::{
    mem::{align_of, size_of},
    slice::from_raw_parts,
};

macro_rules! decl {
    ($(struct $name:ident { $($(#[$a:meta])*$field:ident: $tpe:ty,)+ } = ($size:literal, $align:literal);)+) => {
        $(
            #[repr(C)]
            pub struct $name {
                $($(#[$a])* $field: $tpe,)+
            }
            impl $name {
                pub fn new($($field:$tpe,)+) -> Self {
                    Self { $($field: $field.to_be(),)+ }
                }
                $(
                    $(#[$a])*
                    pub fn $field(&self) -> $tpe {
                        <$tpe>::from_be(self.$field)
                    }
                )+
                pub fn as_slice(&self) -> &[u8] {
                    unsafe { from_raw_parts(self as *const _ as *const u8, size_of::<Self>()) }
                }
                pub fn as_ptr(&self) -> *const u8 {
                    self as *const _ as *const u8
                }
                pub fn from_slice(bytes: &[u8]) -> &Self {
                    debug_assert!(bytes.len() >= size_of::<Self>());
                    unsafe { &*(bytes as *const _ as *const Self) }
                }
                pub fn from_ptr(ptr: *const u8) -> *const Self {
                    ptr as *const Self
                }
                pub const fn size() -> u8 {
                    let s = size_of::<Self>();
                    debug_assert!(s < 256);
                    s as u8
                }
            }
            impl ::std::fmt::Debug for $name {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    f.debug_struct(stringify!($name))
                    $(.field(stringify!($field), &self.$field()))+
                    .finish()
                }
            }
        )+

        #[test]
        fn assert_size() {
            $(
                assert_eq!(size_of::<$name>(), $size);
                assert_eq!(align_of::<$name>(), $align);
            )+
        }
    };
}

decl! {

    // compressed tree file

    struct StreamHeader {
        /// version magic value
        stream_version: u32,
        /// user-defined version for stream payload data format
        user_version: u32,
        /// offset of the first stored block relative to stream start
        offset: u64,
    } = (16, 8);

    struct BlockHeader {
        /// total length of this block including header and trailer
        length: u32,
        /// hierarchy level of this block
        level: u32,
        /// stream offset of the previous block of same or higher level
        previous: u64,
    } = (16, 8);

    struct BlockTrailer {
        /// number of padding bytes appended to the compressed payload
        padding: u8,
        /// total length of this block including header and trailer
        length: u32,
    } = (8, 4);

    // uncompressed events file

    struct FileHeader {
        offset: u64,
    } = (8, 8);
}

impl BlockHeader {
    pub fn data(&self) -> &[u8] {
        let trailer = unsafe {
            &*BlockTrailer::from_ptr(
                self.as_ptr()
                    .add(u32_to_usize(self.length()) - size_of::<BlockTrailer>()),
            )
        };
        let len = u32_to_usize(self.length())
            - size_of::<Self>()
            - usize::from(trailer.padding())
            - size_of::<BlockTrailer>();
        let data = unsafe { self.as_ptr().add(size_of::<Self>()) };
        unsafe { from_raw_parts(data, len) }
    }
}

#[test]
fn align() {
    // assert that alignment is at most eight bytes
    assert_eq!(align_of::<StreamHeader>() & !15, 0);
    assert_eq!(align_of::<BlockHeader>() & !15, 0);
    assert_eq!(align_of::<BlockTrailer>() & !15, 0);
    // assert that size is multiple of eight bytes
    assert_eq!(size_of::<StreamHeader>() & 7, 0);
    assert_eq!(size_of::<BlockHeader>() & 7, 0);
    assert_eq!(size_of::<BlockTrailer>() & 7, 0);

    // needed in reading files
    assert!(size_of::<BlockTrailer>() < size_of::<StreamHeader>());
}
