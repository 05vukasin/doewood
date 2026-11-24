//! The open-data set read as a stream into a compact index by MB.
//!
//! The set is one JSON object, `{"DatumPreseka": "YYYY-MM-DD", "Podaci": {"<MB>": {...}}}`,
//! about 58 MB for some 134,000 companies (2026). It is read with a streaming visitor: no JSON
//! tree is built, and the index keeps each name once and every low-cardinality field
//! (municipality, status, legal form, dates, activity) interned, so it takes a fraction of the
//! body. At most `max_companies` entries are indexed; a larger set is refused.

use std::collections::HashMap;
use std::fmt;

use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use time::OffsetDateTime;

use super::model::{FIELDS, RawRecord};

/// No value for a field.
const NONE: u32 = u32::MAX;

/// The index of one register snapshot.
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// `DatumPreseka`, as given.
    as_of: Option<Box<str>>,
    /// When it was downloaded.
    pub fetched_at: OffsetDateTime,
    entries: HashMap<u32, Entry>,
    pool: Pool,
    /// Entries skipped: an MB key that is not eight digits, or a record without a name.
    pub skipped: usize,
}

#[derive(Debug)]
struct Entry {
    name: Box<str>,
    /// `FIELDS[1..]`, as indexes into the pool.
    fields: [u32; 6],
}

/// Interned strings.
#[derive(Debug, Default)]
struct Pool {
    strings: Vec<Box<str>>,
    index: HashMap<Box<str>, u32>,
}

impl Pool {
    fn intern(&mut self, s: Option<String>) -> u32 {
        let Some(s) = s else { return NONE };
        if let Some(&i) = self.index.get(s.as_str()) {
            return i;
        }
        let i = u32::try_from(self.strings.len()).unwrap_or(NONE);
        let s: Box<str> = s.into();
        self.strings.push(s.clone());
        self.index.insert(s, i);
        i
    }

    fn get(&self, i: u32) -> Option<String> {
        self.strings
            .get(usize::try_from(i).ok()?)
            .map(ToString::to_string)
    }
}

impl Snapshot {
    /// How many companies it holds.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The raw record of `mb` (eight digits), if the snapshot has it.
    pub fn record(&self, mb: &str) -> Option<RawRecord> {
        let e = self.entries.get(&key(mb)?)?;
        Some(self.raw(mb.to_owned(), e))
    }

    /// Every record whose name passes `keep`, with its MB.
    pub fn filter(&self, mut keep: impl FnMut(&str) -> bool) -> Vec<RawRecord> {
        self.entries
            .iter()
            .filter(|(_, e)| keep(&e.name))
            .map(|(mb, e)| self.raw(format!("{mb:08}"), e))
            .collect()
    }

    fn raw(&self, mb: String, e: &Entry) -> RawRecord {
        let f = |i: usize| self.pool.get(e.fields[i]);
        RawRecord {
            mb: Some(mb),
            as_of: self.as_of.as_deref().map(str::to_owned),
            name: Some(e.name.to_string()),
            municipality_code: f(0),
            municipality: f(1),
            status: f(2),
            founded: f(3),
            legal_form: f(4),
            activity: f(5),
        }
    }
}

/// An MB as the index key: exactly eight ASCII digits.
fn key(mb: &str) -> Option<u32> {
    (mb.len() == 8 && mb.bytes().all(|b| b.is_ascii_digit()))
        .then(|| mb.parse().ok())
        .flatten()
}

/// Reads the set. `Err` is why it is unusable (not JSON, not the set's shape, too many
/// companies, no company at all).
pub(crate) fn parse(
    body: &[u8],
    max_companies: usize,
    fetched_at: OffsetDateTime,
) -> Result<Snapshot, String> {
    // A UTF-8 byte order mark is tolerated; any other encoding is not JSON.
    let body = body.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(body);
    let mut snapshot = Snapshot {
        as_of: None,
        fetched_at,
        entries: HashMap::new(),
        pool: Pool::default(),
        skipped: 0,
    };
    let mut de = serde_json::Deserializer::from_slice(body);
    Top {
        snapshot: &mut snapshot,
        max: max_companies,
    }
    .deserialize(&mut de)
    .map_err(|e| e.to_string())?;
    de.end().map_err(|e| e.to_string())?;
    if snapshot.entries.is_empty() {
        return Err("no company in the set".to_owned());
    }
    Ok(snapshot)
}

/// The top-level object.
struct Top<'a> {
    snapshot: &'a mut Snapshot,
    max: usize,
}

impl<'de> DeserializeSeed<'de> for Top<'_> {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for Top<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an object with DatumPreseka and Podaci")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut data = false;
        while let Some(k) = map.next_key::<TopKey>()? {
            match k {
                TopKey::AsOf => {
                    self.snapshot.as_of = map.next_value::<Lenient>()?.0.map(Into::into);
                }
                TopKey::Data => {
                    map.next_value_seed(Companies {
                        snapshot: self.snapshot,
                        max: self.max,
                    })?;
                    data = true;
                }
                TopKey::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if data {
            Ok(())
        } else {
            Err(de::Error::missing_field("Podaci"))
        }
    }
}

enum TopKey {
    AsOf,
    Data,
    Other,
}

impl<'de> de::Deserialize<'de> for TopKey {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_identifier(KeyVisitor(|s| match s {
            "DatumPreseka" => TopKey::AsOf,
            "Podaci" => TopKey::Data,
            _ => TopKey::Other,
        }))
    }
}

/// The `Podaci` object: MB to record.
struct Companies<'a> {
    snapshot: &'a mut Snapshot,
    max: usize,
}

impl<'de> DeserializeSeed<'de> for Companies<'_> {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for Companies<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an object of companies by MB")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let s = self.snapshot;
        while let Some(MbKey(mb)) = map.next_key::<MbKey>()? {
            let fields = map.next_value::<Fields>()?.0;
            let [name, rest @ ..] = fields;
            let name = name.and_then(|n| {
                let t = n.trim();
                (!t.is_empty()).then(|| Box::<str>::from(t))
            });
            let (Some(mb), Some(name)) = (mb, name) else {
                s.skipped += 1;
                continue;
            };
            if !s.entries.contains_key(&mb) && s.entries.len() >= self.max {
                return Err(de::Error::custom(format!(
                    "more than {} companies",
                    self.max
                )));
            }
            let fields = rest.map(|v| s.pool.intern(v));
            s.entries.insert(mb, Entry { name, fields });
        }
        Ok(())
    }
}

/// An MB key: `None` when it is not eight digits.
struct MbKey(Option<u32>);

impl<'de> de::Deserialize<'de> for MbKey {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_identifier(KeyVisitor(|s| MbKey(key(s))))
    }
}

/// The fields of one company, in [`FIELDS`] order; unknown fields are ignored, a value that is
/// not a string or a number is `None`.
struct Fields([Option<String>; 7]);

impl<'de> de::Deserialize<'de> for Fields {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Fields;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a company object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Fields, A::Error> {
                let mut out: [Option<String>; 7] = Default::default();
                while let Some(FieldKey(i)) = map.next_key::<FieldKey>()? {
                    match i {
                        Some(i) => out[i] = map.next_value::<Lenient>()?.0,
                        None => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(Fields(out))
            }
        }
        d.deserialize_map(V)
    }
}

struct FieldKey(Option<usize>);

impl<'de> de::Deserialize<'de> for FieldKey {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_identifier(KeyVisitor(|s| {
            FieldKey(FIELDS.iter().position(|f| *f == s))
        }))
    }
}

/// A key read without allocating (when it has no escapes).
struct KeyVisitor<T>(fn(&str) -> T);

impl<T> Visitor<'_> for KeyVisitor<T> {
    type Value = T;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a string key")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<T, E> {
        Ok((self.0)(v))
    }
}

/// A string or a number as text; anything else (null, a bool, an object, an array) is `None`.
struct Lenient(Option<String>);

impl<'de> de::Deserialize<'de> for Lenient {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Lenient;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a value")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Lenient, E> {
                Ok(Lenient(Some(v.to_owned())))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<Lenient, E> {
                Ok(Lenient(Some(v)))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Lenient, E> {
                Ok(Lenient(Some(v.to_string())))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Lenient, E> {
                Ok(Lenient(Some(v.to_string())))
            }

            fn visit_f64<E: de::Error>(self, _: f64) -> Result<Lenient, E> {
                Ok(Lenient(None))
            }

            fn visit_bool<E: de::Error>(self, _: bool) -> Result<Lenient, E> {
                Ok(Lenient(None))
            }

            fn visit_unit<E: de::Error>(self) -> Result<Lenient, E> {
                Ok(Lenient(None))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Lenient, A::Error> {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(Lenient(None))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Lenient, A::Error> {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(Lenient(None))
            }
        }
        d.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH
    }

    #[test]
    fn reads_the_set_and_interns() {
        let body = r#"{"DatumPreseka":"2026-11-01","Extra":[1,2],"Podaci":{
            "99000013":{"PoslovnoIme":" Пекара Измишљена ДОО ","SifraOpstine":"99001",
                "NazivOpstine":"Измишљено","NazivStatus":"Активан","DatumOsnivanja":"2001-02-03",
                "NazivPravneForme":"Друштво са ограниченом одговорношћу","SifraDelatnosti":"1071",
                "Novo":{"x":1}},
            "99000021":{"PoslovnoIme":"Primer Dva","SifraOpstine":99001,"NazivOpstine":"Измишљено",
                "NazivStatus":null,"DatumOsnivanja":true},
            "1234":{"PoslovnoIme":"Short key"},
            "99000048":{"PoslovnoIme":"   "}
        }}"#;
        let s = parse(body.as_bytes(), 10, at()).unwrap();
        assert_eq!(s.len(), 2);
        assert_eq!(s.skipped, 2);
        let r = s.record("99000013").unwrap();
        assert_eq!(r.name.as_deref(), Some("Пекара Измишљена ДОО"));
        assert_eq!(r.as_of.as_deref(), Some("2026-11-01"));
        assert_eq!(r.activity.as_deref(), Some("1071"));
        let r = s.record("99000021").unwrap();
        assert_eq!(r.municipality_code.as_deref(), Some("99001"));
        assert_eq!(r.status, None);
        assert_eq!(r.founded, None);
        // "99001" and "Измишљено" are stored once.
        assert_eq!(s.pool.strings.len(), 6);
        assert!(s.record("99000048").is_none());
        assert!(s.record("9900001").is_none());
        assert_eq!(s.filter(|n| n.starts_with("Primer")).len(), 1);
    }

    #[test]
    fn refusals() {
        let one = r#"{"PoslovnoIme":"X"}"#;
        let two = format!(r#"{{"Podaci":{{"99000013":{one},"99000021":{one}}}}}"#);
        assert!(parse(two.as_bytes(), 2, at()).is_ok());
        assert!(
            parse(two.as_bytes(), 1, at())
                .unwrap_err()
                .contains("more than 1")
        );
        for bad in [
            &b""[..],
            b"[]",
            b"{}",
            b"{\"Podaci\":[]}",
            b"{\"Podaci\":{}}",
            b"{\"Podaci\":{\"99000013\":{\"PoslovnoIme\":\"X\"}}} trailing",
            b"{\"Podaci\":{\"99000013\":{\"PoslovnoIme\":\"X\"}",
            b"<html>maintenance</html>",
        ] {
            assert!(
                parse(bad, 10, at()).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
        // A byte order mark is fine.
        let bom = [b"\xEF\xBB\xBF".as_slice(), two.as_bytes()].concat();
        assert!(parse(&bom, 2, at()).is_ok());
    }
}
