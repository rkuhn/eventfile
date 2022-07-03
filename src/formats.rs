#![allow(unused)]

use core::{
    mem::{align_of, size_of},
    slice::from_raw_parts,
};

macro_rules! decl {
    ($(struct $name:ident { $($field:ident: $tpe:ty,)+ } = ($size:literal, $align:literal);)+) => {
        $(
            #[repr(C)]
            pub struct $name {
                $($field: $tpe,)+
            }
            impl $name {
                pub fn new($($field:$tpe,)+) -> Self {
                    Self { $($field: $field.to_be(),)+ }
                }
                $(
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
    struct StreamHeader {
        stream_version: u32,
        user_version: u32,
        offset: u64,
    } = (16, 8);

    struct BlockHeader {
        length: u32,
        level: u32,
        previous: u64,
    } = (16, 8);

    struct BlockTrailer {
        padding: u8,
        length: u32,
    } = (8, 4);

    struct FileHeader {
        offset: u64,
    } = (8, 8);
}

impl BlockHeader {
    pub fn len(&self) -> usize {
        self.length() as usize
    }
    pub fn data(&self) -> &[u8] {
        let trailer = unsafe {
            &*BlockTrailer::from_ptr(self.as_ptr().add(self.len() - size_of::<BlockTrailer>()))
        };
        let len = self.len()
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
}
