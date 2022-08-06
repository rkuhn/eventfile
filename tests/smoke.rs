use cbor_data::{index_str, Cbor, CborBuilder, Encoder};
use eventfile::{Cache, Error, EventFileIter, EventFrame, NodeType, Stream, StreamConfig, FANOUT};
use rand::{thread_rng, Rng};
use smallvec::SmallVec;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    ops::{Bound, RangeBounds, RangeInclusive},
};
use tempfile::{tempdir, TempDir};
use tracing_subscriber::fmt::format::FmtSpan;

fn overlaps<T, U, V, L>(left: &L, right: RangeInclusive<U>) -> bool
where
    T: AsRef<V>,
    U: AsRef<V>,
    V: ?Sized + Ord,
    L: RangeBounds<T>,
{
    let min = match left.start_bound() {
        std::ops::Bound::Included(left) => Bound::Included(left.as_ref().max(right.start().as_ref())),
        std::ops::Bound::Excluded(left) => {
            if left.as_ref() >= right.start().as_ref() {
                Bound::Excluded(left.as_ref())
            } else {
                Bound::Included(right.start().as_ref())
            }
        }
        std::ops::Bound::Unbounded => Bound::Included(right.start().as_ref()),
    };
    let max = match left.end_bound() {
        Bound::Included(left) => Bound::Included(left.as_ref().min(right.end().as_ref())),
        Bound::Excluded(left) => {
            if left.as_ref() <= right.end().as_ref() {
                Bound::Excluded(left.as_ref())
            } else {
                Bound::Included(right.end().as_ref())
            }
        }
        Bound::Unbounded => Bound::Included(right.end().as_ref()),
    };
    match (min, max) {
        (Bound::Included(l), Bound::Included(r)) => l <= r,
        (Bound::Included(l), Bound::Excluded(r)) => l < r,
        (Bound::Excluded(l), Bound::Included(r)) => l < r,
        (Bound::Excluded(l), Bound::Excluded(r)) => l < r,
        _ => unreachable!(),
    }
}

fn overlaps2<T, L>(left: &L, right: RangeInclusive<T>) -> bool
where
    T: Ord,
    L: RangeBounds<T>,
{
    let min = match left.start_bound() {
        std::ops::Bound::Included(left) => Bound::Included(left.max(right.start())),
        std::ops::Bound::Excluded(left) => {
            if left >= right.start() {
                Bound::Excluded(left)
            } else {
                Bound::Included(right.start())
            }
        }
        std::ops::Bound::Unbounded => Bound::Included(right.start()),
    };
    let max = match left.end_bound() {
        Bound::Included(left) => Bound::Included(left.min(right.end())),
        Bound::Excluded(left) => {
            if left <= right.end() {
                Bound::Excluded(left)
            } else {
                Bound::Included(right.end())
            }
        }
        Bound::Unbounded => Bound::Included(right.end()),
    };
    match (min, max) {
        (Bound::Included(l), Bound::Included(r)) => l <= r,
        (Bound::Included(l), Bound::Excluded(r)) => l < r,
        (Bound::Excluded(l), Bound::Included(r)) => l < r,
        (Bound::Excluded(l), Bound::Excluded(r)) => l < r,
        _ => unreachable!(),
    }
}

fn setup() -> (TempDir, Box<dyn Cache>) {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();
    (tempdir().unwrap(), Box::new(fbr_cache::FbrCache::new(1000)))
}

fn get_str<'a>(cbor: &'a Cbor, path: &str) -> Cow<'a, str> {
    cbor.index_borrowed(index_str(path)).unwrap().decode().to_str().unwrap()
}

fn get_bytes<'a>(cbor: &'a Cbor, path: &str) -> Cow<'a, [u8]> {
    cbor.index_borrowed(index_str(path)).unwrap().decode().to_bytes().unwrap()
}

fn get_u64(cbor: &Cbor, path: &str) -> u64 {
    match cbor.index_borrowed(index_str(path)).unwrap().decode().as_number().unwrap() {
        cbor_data::value::Number::Int(x) => *x as u64,
        _ => panic!("not a u64"),
    }
}

fn summarise_leaf(mut iter: EventFileIter) -> Vec<u8> {
    CborBuilder::new()
        .encode_array(|b| {
            for frame in &mut iter {
                let cbor = Cbor::checked(frame.data).unwrap();
                b.encode_dict(|b| {
                    b.with_key("tag", |b| b.encode_str(get_str(cbor, "[0]")));
                    b.with_key("first", |b| b.encode_u64(get_bytes(cbor, "[1]")[0].into()));
                });
            }
        })
        .as_slice()
        .into()
}

fn summarise_index(nt: NodeType, index: [&[u8]; FANOUT]) -> Vec<u8> {
    CborBuilder::new()
        .encode_array(|b| match nt {
            NodeType::Branch => {
                for idx in index {
                    let arr = Cbor::checked(idx)
                        .unwrap()
                        .decode()
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|c| {
                            (
                                get_str(c, "min_tag").into_owned(),
                                get_str(c, "max_tag").into_owned(),
                                get_u64(c, "min_fst"),
                                get_u64(c, "max_fst"),
                            )
                        })
                        .collect::<Vec<_>>();
                    let min_tag = arr.iter().map(|x| &x.0).min().unwrap();
                    let max_tag = arr.iter().map(|x| &x.1).max().unwrap();
                    let min_fst = arr.iter().map(|x| x.2).min().unwrap();
                    let max_fst = arr.iter().map(|x| x.3).max().unwrap();
                    b.encode_dict(|b| {
                        b.with_key("min_tag", |b| b.encode_str(min_tag.as_str()));
                        b.with_key("max_tag", |b| b.encode_str(max_tag.as_str()));
                        b.with_key("min_fst", |b| b.encode_u64(min_fst));
                        b.with_key("max_fst", |b| b.encode_u64(max_fst));
                    });
                }
            }
            NodeType::Leaf => {
                for idx in index {
                    let arr = Cbor::checked(idx)
                        .unwrap()
                        .decode()
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|c| (get_str(c, "tag").into_owned(), get_u64(c, "first")))
                        .collect::<Vec<_>>();
                    let min_tag = arr.iter().map(|x| &x.0).min().unwrap();
                    let max_tag = arr.iter().map(|x| &x.0).max().unwrap();
                    let min_fst = arr.iter().map(|x| x.1).min().unwrap();
                    let max_fst = arr.iter().map(|x| x.1).max().unwrap();
                    b.encode_dict(|b| {
                        b.with_key("min_tag", |b| b.encode_str(min_tag.as_str()));
                        b.with_key("max_tag", |b| b.encode_str(max_tag.as_str()));
                        b.with_key("min_fst", |b| b.encode_u64(min_fst));
                        b.with_key("max_fst", |b| b.encode_u64(max_fst));
                    });
                }
            }
        })
        .as_slice()
        .into()
}

fn select<'a>(
    range: impl RangeBounds<&'a str>,
) -> impl FnMut(NodeType, &[u8]) -> Result<SmallVec<[u32; FANOUT]>, Error> {
    move |nt, idx| match nt {
        NodeType::Branch => Ok(Cbor::checked(idx)
            .unwrap()
            .decode()
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .filter_map(|(idx, cbor)| {
                let min = get_str(cbor, "min_tag");
                let max = get_str(cbor, "max_tag");
                if overlaps::<_, _, str, _>(&range, min..=max) {
                    Some(idx as u32)
                } else {
                    None
                }
            })
            .collect()),
        NodeType::Leaf => Ok(Cbor::checked(idx)
            .unwrap()
            .decode()
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .filter_map(|(idx, cbor)| {
                let tag = get_str(cbor, "tag");
                if range.contains(&tag.as_ref()) {
                    Some(idx as u32)
                } else {
                    None
                }
            })
            .collect()),
    }
}

fn select_byte(range: impl RangeBounds<u8>) -> impl FnMut(NodeType, &[u8]) -> Result<SmallVec<[u32; FANOUT]>, Error> {
    move |nt, idx| match nt {
        NodeType::Branch => Ok(Cbor::checked(idx)
            .unwrap()
            .decode()
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .filter_map(|(idx, cbor)| {
                let min = get_u64(cbor, "min_fst") as u8;
                let max = get_u64(cbor, "max_fst") as u8;
                if overlaps2(&range, min..=max) {
                    Some(idx as u32)
                } else {
                    None
                }
            })
            .collect()),
        NodeType::Leaf => Ok(Cbor::checked(idx)
            .unwrap()
            .decode()
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .filter_map(|(idx, cbor)| {
                let fst = get_u64(cbor, "first") as u8;
                if range.contains(&fst) {
                    Some(idx as u32)
                } else {
                    None
                }
            })
            .collect()),
    }
}

fn extractor(bytes: &[u8]) -> Result<String, Error> {
    Ok(get_str(Cbor::checked(bytes).unwrap(), "[0]").into_owned())
}

#[test]
fn smoke() {
    let (dir, cache) = setup();

    let config = StreamConfig::new(summarise_leaf, summarise_index, cache).compression_threshold(1000);
    let mut s = Stream::new(13, config, dir.path().join("1234"), 42).unwrap();

    let mut all = vec![];
    let mut per_byte = BTreeMap::<u8, Vec<String>>::new();
    for i in 0..130u64 {
        let cbor = CborBuilder::new().encode_array(|b| {
            b.encode_str(i.to_string());
            let mut bytes = vec![0u8; thread_rng().gen_range(100..300)];
            thread_rng().fill(&mut *bytes);
            per_byte.entry(bytes[0]).or_default().push(i.to_string());
            b.encode_bytes(bytes);
        });
        s.append(EventFrame::new(cbor.as_slice(), &[])).unwrap();
        all.push(i.to_string());
    }

    let evs = s.iter(select(..), extractor).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();
    assert_eq!(evs, all);

    for i in 0..9 {
        let lower = i.to_string();
        let upper = (i + 1).to_string();
        let evs = s
            .iter(select(lower.as_str()..upper.as_str()), extractor)
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            evs,
            all.iter().filter(|s| s.starts_with(lower.as_str())).cloned().collect::<Vec<_>>()
        );
    }

    for b in 0..=255u8 {
        let evs = s.iter(select_byte(b..=b), extractor).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();
        assert_eq!(evs, per_byte.get(&b).cloned().unwrap_or_default(), "b={}", b);
    }
}
