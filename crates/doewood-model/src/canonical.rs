//! JSON text without the `serde_json::Value` tree (issue #98).
//!
//! [`to_canonical`] writes the text `serde_json::to_value(v)?.to_string()` gives: compact, with
//! every object's keys sorted (`serde_json`'s maps are B-trees) and the last of duplicate keys
//! kept. It writes it straight from `v`'s `Serialize`: a record's tree was about 16 times its
//! text, while this keeps one scratch buffer per nesting level, reused across siblings.
//!
//! Stored records were written as `Value::to_string()`, so records written this way are byte
//! for byte the ones written before, and an unchanged record compares equal as text.

use std::borrow::Cow;

use serde::Serialize;
use serde::ser::{self, Error as _};
use serde_json::value::RawValue;
use serde_json::{Error, Result};

/// `serde_json::to_value(value)?.to_string()`, written without the tree. Fails where
/// `to_value` fails (a map key that is not a string, a number or a bool), and for a
/// [`RawValue`] inside `value`.
pub fn to_canonical<T: ?Sized + Serialize>(value: &T) -> Result<Box<RawValue>> {
    write(value, None)
}

/// [`to_canonical`] with the top-level object's `key` set to the string `text` (added, or
/// replacing the one `value` has): `v[key] = Value::String(text)` before `v.to_string()`.
/// Fails if `value` is not an object.
pub fn to_canonical_with<T: ?Sized + Serialize>(
    value: &T,
    key: &str,
    text: &str,
) -> Result<Box<RawValue>> {
    write(value, Some((key, text)))
}

fn write<T: ?Sized + Serialize>(value: &T, set: Option<(&str, &str)>) -> Result<Box<RawValue>> {
    let mut out = Vec::new();
    let mut pool = Pool::default();
    let mut set_used = set.is_none();
    value.serialize(Ser {
        out: &mut out,
        pool: &mut pool,
        set,
        set_used: &mut set_used,
    })?;
    if !set_used {
        return Err(Error::custom("only an object can have a key set"));
    }
    // Only valid UTF-8 is written: JSON punctuation, numbers and serde_json's string escapes.
    let text = String::from_utf8(out).map_err(Error::custom)?;
    RawValue::from_string(text)
}

/// Largest scratch buffer kept for reuse, in bytes.
const POOL_MAX: usize = 64 * 1024;

/// Scratch buffers and entry lists, reused by the objects of each nesting level.
#[derive(Default)]
struct Pool {
    bufs: Vec<Vec<u8>>,
    entries: Vec<Vec<Entry>>,
}

/// One member of an object being written: its key, and where its value is in the buffer.
struct Entry {
    key: Cow<'static, str>,
    start: usize,
    end: usize,
}

struct Ser<'a> {
    out: &'a mut Vec<u8>,
    pool: &'a mut Pool,
    /// The key to set, on the top-level object only.
    set: Option<(&'a str, &'a str)>,
    set_used: &'a mut bool,
}

impl<'a> Ser<'a> {
    fn scalar<T: ?Sized + Serialize>(self, v: &T) -> Result<()> {
        serde_json::to_writer(&mut *self.out, v)
    }

    fn object(self, variant: Option<&'static str>) -> Obj<'a> {
        if self.set.is_some() {
            *self.set_used = true;
        }
        if let Some(v) = variant {
            self.out.push(b'{');
            key(self.out, v);
        }
        self.out.push(b'{');
        Obj {
            buf: self.pool.bufs.pop().unwrap_or_default(),
            entries: self.pool.entries.pop().unwrap_or_default(),
            start: self.out.len(),
            // The key to set may sort anywhere: that object is always sorted at the end.
            streaming: self.set.is_none(),
            out: self.out,
            pool: self.pool,
            set: self.set,
            key: None,
            variant: variant.is_some(),
        }
    }

    fn array(self, variant: Option<&'static str>) -> Arr<'a> {
        if let Some(v) = variant {
            self.out.push(b'{');
            key(self.out, v);
        }
        self.out.push(b'[');
        Arr {
            out: self.out,
            pool: self.pool,
            first: true,
            variant: variant.is_some(),
        }
    }
}

/// Writes `"key":`.
fn key(out: &mut Vec<u8>, k: &str) {
    // Writing a string to a Vec cannot fail.
    let _ = serde_json::to_writer(&mut *out, k);
    out.push(b':');
}

fn nested<'b>(out: &'b mut Vec<u8>, pool: &'b mut Pool, used: &'b mut bool) -> Ser<'b> {
    Ser {
        out,
        pool,
        set: None,
        set_used: used,
    }
}

impl<'a> ser::Serializer for Ser<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Arr<'a>;
    type SerializeTuple = Arr<'a>;
    type SerializeTupleStruct = Arr<'a>;
    type SerializeTupleVariant = Arr<'a>;
    type SerializeMap = Obj<'a>;
    type SerializeStruct = Obj<'a>;
    type SerializeStructVariant = Obj<'a>;

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.scalar(&v)
    }
    fn serialize_i8(self, v: i8) -> Result<()> {
        self.scalar(&v)
    }
    fn serialize_i16(self, v: i16) -> Result<()> {
        self.scalar(&v)
    }
    fn serialize_i32(self, v: i32) -> Result<()> {
        self.scalar(&v)
    }
    fn serialize_i64(self, v: i64) -> Result<()> {
        self.scalar(&v)
    }
    fn serialize_i128(self, v: i128) -> Result<()> {
        // As `to_value`: a number only when it fits 64 bits.
        if let Ok(v) = u64::try_from(v) {
            self.scalar(&v)
        } else if let Ok(v) = i64::try_from(v) {
            self.scalar(&v)
        } else {
            Err(Error::custom("number out of range"))
        }
    }
    fn serialize_u8(self, v: u8) -> Result<()> {
        self.scalar(&v)
    }
    fn serialize_u16(self, v: u16) -> Result<()> {
        self.scalar(&v)
    }
    fn serialize_u32(self, v: u32) -> Result<()> {
        self.scalar(&v)
    }
    fn serialize_u64(self, v: u64) -> Result<()> {
        self.scalar(&v)
    }
    fn serialize_u128(self, v: u128) -> Result<()> {
        match u64::try_from(v) {
            Ok(v) => self.scalar(&v),
            Err(_) => Err(Error::custom("number out of range")),
        }
    }
    fn serialize_f32(self, v: f32) -> Result<()> {
        // `to_value` widens an f32 to f64 (and a non-finite one to null, as the writer does).
        self.scalar(&f64::from(v))
    }
    fn serialize_f64(self, v: f64) -> Result<()> {
        self.scalar(&v)
    }
    fn serialize_char(self, v: char) -> Result<()> {
        self.scalar(&v)
    }
    fn serialize_str(self, v: &str) -> Result<()> {
        self.scalar(v)
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        // `to_value` makes bytes an array of numbers.
        self.out.push(b'[');
        for (i, b) in v.iter().enumerate() {
            if i > 0 {
                self.out.push(b',');
            }
            serde_json::to_writer(&mut *self.out, b)?;
        }
        self.out.push(b']');
        Ok(())
    }
    fn serialize_none(self) -> Result<()> {
        self.serialize_unit()
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<()> {
        self.out.extend_from_slice(b"null");
        Ok(())
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.scalar(variant)
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()> {
        // A one-key object: nothing to sort.
        self.out.push(b'{');
        key(self.out, variant);
        value.serialize(nested(self.out, self.pool, self.set_used))?;
        self.out.push(b'}');
        Ok(())
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<Arr<'a>> {
        Ok(self.array(None))
    }
    fn serialize_tuple(self, _len: usize) -> Result<Arr<'a>> {
        Ok(self.array(None))
    }
    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Arr<'a>> {
        Ok(self.array(None))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Arr<'a>> {
        Ok(self.array(Some(variant)))
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Obj<'a>> {
        Ok(self.object(None))
    }
    fn serialize_struct(self, name: &'static str, _len: usize) -> Result<Obj<'a>> {
        if name == "$serde_json::private::RawValue" {
            return Err(Error::custom("a RawValue cannot be written sorted"));
        }
        Ok(self.object(None))
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Obj<'a>> {
        Ok(self.object(Some(variant)))
    }
}

/// An array, written straight to the output.
struct Arr<'a> {
    out: &'a mut Vec<u8>,
    pool: &'a mut Pool,
    first: bool,
    /// Wrapped in `{"variant": ...}`.
    variant: bool,
}

impl Arr<'_> {
    fn element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        if !self.first {
            self.out.push(b',');
        }
        self.first = false;
        let mut used = true;
        value.serialize(nested(self.out, self.pool, &mut used))
    }

    fn close(self) {
        self.out.push(b']');
        if self.variant {
            self.out.push(b'}');
        }
    }
}

impl ser::SerializeSeq for Arr<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.element(value)
    }
    fn end(self) -> Result<()> {
        self.close();
        Ok(())
    }
}

impl ser::SerializeTuple for Arr<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.element(value)
    }
    fn end(self) -> Result<()> {
        self.close();
        Ok(())
    }
}

impl ser::SerializeTupleStruct for Arr<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.element(value)
    }
    fn end(self) -> Result<()> {
        self.close();
        Ok(())
    }
}

impl ser::SerializeTupleVariant for Arr<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.element(value)
    }
    fn end(self) -> Result<()> {
        self.close();
        Ok(())
    }
}

/// An object. While its keys come in sorted order (quail's `{"t": ..., "v": ...}` values do)
/// its members are written straight to the output; at the first key out of order they move to
/// a scratch buffer, and are written in key order at the end.
struct Obj<'a> {
    out: &'a mut Vec<u8>,
    pool: &'a mut Pool,
    buf: Vec<u8>,
    /// The members so far, their values in `out` while streaming, else in `buf`.
    entries: Vec<Entry>,
    /// Where the members start in `out` (after the `{`).
    start: usize,
    streaming: bool,
    set: Option<(&'a str, &'a str)>,
    /// The key of the map value to come.
    key: Option<Cow<'static, str>>,
    /// Wrapped in `{"variant": ...}`.
    variant: bool,
}

impl Obj<'_> {
    fn member<T: ?Sized + Serialize>(&mut self, k: Cow<'static, str>, value: &T) -> Result<()> {
        let mut used = true;
        if self.streaming && self.entries.last().is_none_or(|l| l.key < k) {
            if !self.entries.is_empty() {
                self.out.push(b',');
            }
            key(self.out, &k);
            let start = self.out.len();
            value.serialize(nested(self.out, self.pool, &mut used))?;
            self.entries.push(Entry {
                key: k,
                start,
                end: self.out.len(),
            });
            return Ok(());
        }
        self.unstream();
        let start = self.buf.len();
        value.serialize(nested(&mut self.buf, self.pool, &mut used))?;
        self.entries.push(Entry {
            key: k,
            start,
            end: self.buf.len(),
        });
        Ok(())
    }

    /// Moves the members written so far to the scratch buffer.
    fn unstream(&mut self) {
        if !self.streaming {
            return;
        }
        self.streaming = false;
        for e in &mut self.entries {
            let start = self.buf.len();
            self.buf.extend_from_slice(&self.out[e.start..e.end]);
            (e.start, e.end) = (start, self.buf.len());
        }
        self.out.truncate(self.start);
    }

    fn close(mut self) -> Result<()> {
        if !self.streaming {
            self.write_sorted()?;
        }
        self.out.push(b'}');
        if self.variant {
            self.out.push(b'}');
        }
        // Small buffers are kept for the next object; a big one (a record's `hash.cdc`, its
        // text) is freed, so it is not held while the levels above are written.
        if self.buf.capacity() <= POOL_MAX {
            self.buf.clear();
            self.pool.bufs.push(std::mem::take(&mut self.buf));
        }
        self.entries.clear();
        self.pool.entries.push(std::mem::take(&mut self.entries));
        Ok(())
    }

    fn write_sorted(&mut self) -> Result<()> {
        if let Some((k, text)) = self.set {
            self.entries.retain(|e| e.key != k);
            let start = self.buf.len();
            serde_json::to_writer(&mut self.buf, text)?;
            self.entries.push(Entry {
                key: Cow::Owned(k.to_owned()),
                start,
                end: self.buf.len(),
            });
        }
        // Stable: of equal keys, the last one is kept (a map insert replaces).
        self.entries.sort_by(|a, b| a.key.cmp(&b.key));
        // Room for the values and their keys at once, not by doubling.
        let keys: usize = self.entries.iter().map(|e| e.key.len() + 4).sum();
        self.out.reserve(self.buf.len() + keys + 2);
        let mut first = true;
        for (i, e) in self.entries.iter().enumerate() {
            if self.entries.get(i + 1).is_some_and(|n| n.key == e.key) {
                continue;
            }
            if !first {
                self.out.push(b',');
            }
            first = false;
            key(self.out, &e.key);
            self.out.extend_from_slice(&self.buf[e.start..e.end]);
        }
        Ok(())
    }
}

impl ser::SerializeMap for Obj<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<()> {
        self.key = Some(Cow::Owned(key.serialize(KeySer)?));
        Ok(())
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        let key = self
            .key
            .take()
            .ok_or_else(|| Error::custom("a map value without a key"))?;
        self.member(key, value)
    }
    fn end(self) -> Result<()> {
        self.close()
    }
}

impl ser::SerializeStruct for Obj<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.member(Cow::Borrowed(key), value)
    }
    fn end(self) -> Result<()> {
        self.close()
    }
}

impl ser::SerializeStructVariant for Obj<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.member(Cow::Borrowed(key), value)
    }
    fn end(self) -> Result<()> {
        self.close()
    }
}

/// A map key as `to_value` makes it: a string, or a number or bool as text.
struct KeySer;

fn key_error() -> Error {
    Error::custom("key must be a string")
}

#[allow(clippy::unnecessary_wraps)] // the key serializer's return type
fn text<T: ToString>(v: &T) -> Result<String> {
    Ok(v.to_string())
}

fn float_key<T: Serialize>(v: &T, finite: bool) -> Result<String> {
    if finite {
        serde_json::to_string(v)
    } else {
        Err(Error::custom(
            "float key must be finite (got NaN or +/-inf)",
        ))
    }
}

impl ser::Serializer for KeySer {
    type Ok = String;
    type Error = Error;
    type SerializeSeq = ser::Impossible<String, Error>;
    type SerializeTuple = ser::Impossible<String, Error>;
    type SerializeTupleStruct = ser::Impossible<String, Error>;
    type SerializeTupleVariant = ser::Impossible<String, Error>;
    type SerializeMap = ser::Impossible<String, Error>;
    type SerializeStruct = ser::Impossible<String, Error>;
    type SerializeStructVariant = ser::Impossible<String, Error>;

    fn serialize_bool(self, v: bool) -> Result<String> {
        text(&v)
    }
    fn serialize_i8(self, v: i8) -> Result<String> {
        text(&v)
    }
    fn serialize_i16(self, v: i16) -> Result<String> {
        text(&v)
    }
    fn serialize_i32(self, v: i32) -> Result<String> {
        text(&v)
    }
    fn serialize_i64(self, v: i64) -> Result<String> {
        text(&v)
    }
    fn serialize_i128(self, v: i128) -> Result<String> {
        text(&v)
    }
    fn serialize_u8(self, v: u8) -> Result<String> {
        text(&v)
    }
    fn serialize_u16(self, v: u16) -> Result<String> {
        text(&v)
    }
    fn serialize_u32(self, v: u32) -> Result<String> {
        text(&v)
    }
    fn serialize_u64(self, v: u64) -> Result<String> {
        text(&v)
    }
    fn serialize_u128(self, v: u128) -> Result<String> {
        text(&v)
    }
    fn serialize_f32(self, v: f32) -> Result<String> {
        float_key(&v, v.is_finite())
    }
    fn serialize_f64(self, v: f64) -> Result<String> {
        float_key(&v, v.is_finite())
    }
    fn serialize_char(self, v: char) -> Result<String> {
        text(&v)
    }
    fn serialize_str(self, v: &str) -> Result<String> {
        Ok(v.to_owned())
    }
    fn serialize_bytes(self, _v: &[u8]) -> Result<String> {
        Err(key_error())
    }
    fn serialize_none(self) -> Result<String> {
        Err(key_error())
    }
    fn serialize_some<T: ?Sized + Serialize>(self, _value: &T) -> Result<String> {
        Err(key_error())
    }
    fn serialize_unit(self) -> Result<String> {
        Err(key_error())
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<String> {
        Err(key_error())
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<String> {
        Ok(variant.to_owned())
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<String> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<String> {
        Err(key_error())
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(key_error())
    }
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(key_error())
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(key_error())
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(key_error())
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(key_error())
    }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(key_error())
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(key_error())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use serde_json::json;

    use super::*;

    fn old<T: Serialize>(v: &T) -> String {
        serde_json::to_value(v).unwrap().to_string()
    }

    fn same<T: Serialize>(v: &T) {
        assert_eq!(to_canonical(v).unwrap().get(), old(v));
    }

    #[derive(Serialize)]
    #[serde(rename_all = "snake_case")]
    enum E {
        Unit,
        New(u8),
        Tuple(i8, String),
        Struct { z: u8, a: Option<u8> },
    }

    #[derive(Serialize)]
    #[serde(tag = "t", content = "v")]
    enum Tagged {
        Text(String),
        List(Vec<Tagged>),
        F(f32),
    }

    #[derive(Serialize)]
    struct Unit;

    #[derive(Serialize)]
    struct New(u64);

    #[derive(Serialize)]
    struct S {
        zeta: u32,
        alpha: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        skipped: Option<u8>,
        mid: Vec<E>,
        tagged: Tagged,
        unit: Unit,
        new: New,
        floats: (f32, f64, f64, f64),
        big: (i128, u128, i64),
        #[serde(with = "bytes")]
        raw: Vec<u8>,
        ch: char,
        nested: BTreeMap<String, S2>,
        hashed: HashMap<u32, bool>,
        booleans: HashMap<bool, u8>,
        esc: String,
    }

    #[derive(Serialize)]
    struct S2 {
        b: u8,
        #[serde(rename = "a")]
        a_but_later: u8,
    }

    mod bytes {
        pub fn serialize<S: serde::Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
            s.serialize_bytes(v)
        }
    }

    #[test]
    fn writes_what_to_value_writes() {
        let s = S {
            zeta: 7,
            alpha: "a",
            skipped: None,
            mid: vec![
                E::Unit,
                E::New(3),
                E::Tuple(-1, "x".into()),
                E::Struct { z: 1, a: None },
            ],
            tagged: Tagged::List(vec![Tagged::Text("t".into()), Tagged::F(0.1)]),
            unit: Unit,
            new: New(u64::MAX),
            floats: (0.1, 1e300, f64::NAN, -0.0),
            big: (-5, 1 << 63, i64::MIN),
            raw: vec![0, 1, 255],
            ch: '"',
            nested: BTreeMap::from([
                (
                    "k".into(),
                    S2 {
                        b: 1,
                        a_but_later: 2,
                    },
                ),
                (
                    "é\n".into(),
                    S2 {
                        b: 3,
                        a_but_later: 4,
                    },
                ),
            ]),
            hashed: (0..50).map(|i| (i, i % 2 == 0)).collect(),
            booleans: HashMap::from([(true, 1), (false, 0)]),
            esc: "line\n\u{1}\u{2028}\"\\ ünï".into(),
        };
        same(&s);
        same(&json!({"b": [1, {"z": null, "a": 2.5}], "a": "x"}));
        same(&json!([]));
        same(&json!({}));
        same(&"plain");
        same(&3u8);
        same(&E::Struct { z: 2, a: Some(1) });
    }

    #[test]
    fn sorts_members_that_came_sorted_at_first() {
        #[derive(Serialize)]
        struct Late {
            a: S2,
            b: Vec<S2>,
            c: Tagged,
            aa: u8,
            d: u8,
            ab: Option<Box<Late>>,
        }
        let inner = Late {
            a: S2 {
                b: 1,
                a_but_later: 2,
            },
            b: vec![S2 {
                b: 3,
                a_but_later: 4,
            }],
            c: Tagged::Text("x".into()),
            aa: 5,
            d: 6,
            ab: None,
        };
        same(&Late {
            a: S2 {
                b: 7,
                a_but_later: 8,
            },
            b: Vec::new(),
            c: Tagged::List(vec![Tagged::F(1.5)]),
            aa: 9,
            d: 10,
            ab: Some(Box::new(inner)),
        });
    }

    #[test]
    fn keeps_the_last_of_equal_keys() {
        #[derive(Serialize)]
        struct Inner {
            a: u8,
        }
        #[derive(Serialize)]
        struct Dup {
            a: u8,
            #[serde(flatten)]
            inner: Inner,
        }
        same(&Dup {
            a: 1,
            inner: Inner { a: 2 },
        });
    }

    #[test]
    fn sets_a_top_level_key() {
        let v = json!({"format": "zip", "z": {"format": 1}, "a": 1});
        let mut expect = v.clone();
        expect["format"] = json!("xlsx");
        assert_eq!(
            to_canonical_with(&v, "format", "xlsx").unwrap().get(),
            expect.to_string()
        );
        let mut added = json!({"b": 1});
        let got = to_canonical_with(&added, "a", "q\"").unwrap();
        added["a"] = json!("q\"");
        assert_eq!(got.get(), added.to_string());
        assert!(to_canonical_with(&json!([1]), "a", "b").is_err());
    }

    #[test]
    fn fails_where_to_value_fails() {
        let m: BTreeMap<Vec<u8>, u8> = BTreeMap::from([(vec![1], 1)]);
        assert!(serde_json::to_value(&m).is_err());
        assert!(to_canonical(&m).is_err());
        assert!(to_canonical(&i128::MIN).is_err());
        assert!(to_canonical(&u128::MAX).is_err());
    }
}
