#![allow(unused)]

use crate::u32_to_usize;
use core::{
    mem::{align_of, size_of},
    slice::from_raw_parts,
};

pub trait HasMagic: Sized {
    const MAGIC: &'static [u8];
    const SIZE: u64;
    const LEN: usize;
}

macro_rules! decl {
    ($(
        struct $name:ident / $lifted:ident {
            $($(#[$a:meta])*$field:ident$(/$set:ident)?: $tpe:ty,)+
        } = ($size:literal, $align:literal$(, $magic:literal)?);
    )+) => {
        $(
            #[repr(C)]
            #[derive(Clone, Copy)]
            pub struct $name {
                $($(#[$a])* $field: $tpe,)+
            }
            #[cfg(not(feature = "native"))]
            impl $name {
                pub fn new($($field:$tpe,)+) -> Self {
                    Self { $($field: $field.to_be(),)+ }
                }
                $(
                    $(#[$a])*
                    pub fn $field(&self) -> $tpe {
                        <$tpe>::from_be(self.$field)
                    }
                    $(
                        pub fn $set(&mut self, value: $tpe) {
                            self.$field = value.to_be();
                        }
                    )?
                )+
            }
            #[cfg(feature = "native")]
            impl $name {
                pub fn new($($field:$tpe,)+) -> Self {
                    Self { $($field,)+ }
                }
                $(
                    $(#[$a])*
                    pub fn $field(&self) -> $tpe {
                        self.$field
                    }
                    $(
                        pub fn $set(&mut self, value: $tpe) {
                            self.$field = value;
                        }
                    )?
                )+
            }
            impl $name {
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
            impl ::std::fmt::Debug for $name {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    f.debug_struct(stringify!($name))
                    $(.field(stringify!($field), &self.$field()))+
                    .finish()
                }
            }
            $(
                impl $crate::formats::HasMagic for $name {
                    const MAGIC: &'static [u8] = {
                        if $magic.len() & 7 != 0 { panic!("MAGIC length must be multiple of eight bytes") }
                        $magic
                    };
                    const LEN: usize = {
                        let size = size_of::<$name>() + $magic.len();
                        if size > u8::MAX as usize { panic!("HasMagic size must fit into u8") }
                        size
                    };
                    const SIZE: u64 = crate::usize_to_u64(Self::LEN);
                }
            )?
            #[derive(Clone, Copy)]
            pub struct $lifted {
                $($(#[$a])* pub $field: $tpe,)+
            }
            impl $name {
                pub fn lift(&self) -> $lifted {
                    $lifted {
                        $($field: self.$field(),)+
                    }
                }
            }
            impl From<$lifted> for $name {
                fn from(l: $lifted) -> Self {
                    Self::new($(l.$field),+)
                }
            }
        )+

        #[test]
        fn assert_size() {
            $(
                assert_eq!(size_of::<$name>(), $size);
                assert_eq!(align_of::<$name>(), $align);
                assert!($align <= 8);
            )+
        }
    };
}

decl! {

    struct MmapFileHeader / MmapFileHeaderLifted {
        /// version magic value
        stream_version: u32,
        /// user-defined version for stream payload data format
        user_version: u32,
        /// offset of the first stored byte relative to stream start
        start_offset: u64,
        /// offset of the first byte beyond the stored stream
        end_offset / set_end_offset: u64,
    } = (24, 8, b"Events01");

    struct BlockHeader / BlockHeaderLifted {
        /// stream offset of immediately preceding block (-1 for None)
        prev_block: u64,
        /// level or this block
        level: u32,
        /// length of this block’s payload excluding padding
        length: u32,
    } = (16, 8, b"BlockSta");

    struct LeafHeader / LeafHeaderLifted {
        /// index of first event in this block
        start_idx: u64,
        /// number of events in this block
        count: u32,
    } = (16, 8, b"LeafHead");

    struct BranchHeader / BranchHeaderLifted {
        /// offset of the previous index block of level same or higher (-1 for None)
        prev_offset: u64,
        /// exclusive upper bound on event indices in this block
        end_idx: u64,
    } = (16, 8, b"BranchHd");

    struct IndexEntry / IndexEntryLifted {
        offset: u64,
        start_idx: u64,
    } = (16, 8, b"");

    struct JumpEntry / JumpEntryLifted {
        pos: u32,
    } = (4, 4, b"");

    struct StagingHeader / StagingHeaderLifted {
        /// stream offset of the preceding compressed block’s header
        last_block: u64,
        /// index number of the first stored event
        start_idx: u64,
        /// number of events stored
        count / set_count: u32,
        /// number of event index slots allocated
        capacity: u32,
    } = (24, 8, b"Staging!");

}

#[test]
fn align() {
    assert_eq!(MmapFileHeader::SIZE, 32);
}
