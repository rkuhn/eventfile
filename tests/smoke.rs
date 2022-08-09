use cbor_data::{index_str, Cbor, CborBuilder, Encoder};
use eventfile::{EventFile, EventFileConfig};
use rand::{thread_rng, Rng};
use std::{borrow::Cow, collections::BTreeMap, mem::take, ops::RangeBounds};
use tempfile::tempdir;

fn get_str<'a>(cbor: &'a Cbor, path: &str) -> Cow<'a, str> {
    cbor.index_borrowed(index_str(path)).unwrap().decode().to_str().unwrap()
}

#[test]
fn smoke() {
    const N: u64 = 130;

    let dir = tempdir().unwrap();
    let mut s = EventFile::new(
        13,
        dir.path().join("1234"),
        EventFileConfig::new(42).compression_threshold(1000),
    )
    .unwrap();

    let mut all = vec![];
    let mut per_byte = BTreeMap::<u8, Vec<String>>::new();
    for i in 0..N {
        let cbor = CborBuilder::new().encode_array(|b| {
            b.encode_str(i.to_string());
            let mut bytes = vec![0u8; thread_rng().gen_range(100..300)];
            thread_rng().fill(&mut *bytes);
            per_byte.entry(bytes[0]).or_default().push(i.to_string());
            b.encode_bytes(bytes);
        });
        s.append(cbor.as_slice()).unwrap();
        all.push(i.to_string());
    }

    for i in 0..N {
        let mut evs = s
            .iter(i..=i)
            .unwrap()
            .flat_map(|it| it.unwrap().iter().map(|v| v.to_owned()).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(evs.len(), 1, " at i={}", i);
        let ev = take(&mut evs[0]);
        let cbor = Cbor::checked(&*ev).unwrap_or_else(|e| panic!("{}\n{:?}", e, ev));
        assert_eq!(get_str(cbor, "[0]"), i.to_string(), " at i={}", i);
    }

    fn all_get(s: &mut EventFile, r: impl RangeBounds<u64>) -> Vec<String> {
        s.iter(r)
            .unwrap()
            .flat_map(|it| {
                it.unwrap()
                    .iter()
                    .map(|b| get_str(Cbor::checked(b).unwrap(), "[0]").into_owned())
                    .collect::<Vec<_>>()
            })
            .collect()
    }
    assert_eq!(all_get(&mut s, ..), all);

    for _ in 0..1000 {
        let start = thread_rng().gen_range(0..=N);
        let end = start + thread_rng().gen_range(0..100);
        let startu = start as usize;
        let endu = (end as usize).min(N as usize - 1);
        assert_eq!(all_get(&mut s, start..=end), &all[startu..=endu], "get {}..={}", start, end);
    }
}
