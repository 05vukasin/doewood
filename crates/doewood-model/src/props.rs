//! Partial, atomic updates of a node's props (issue #51).
//!
//! Ingest and the enrichment job both write company props. Reading a node, changing it and
//! writing it back whole loses whatever the other wrote in between, so each writer sends only
//! its own changes as a [`PropsPatch`], and the backend applies it to the props as stored, in
//! one transaction with the row locked ([`crate::GraphStore::update_props`]).
//!
//! A path names object keys from the top level: `["website"]`, `["ids", "lei"]`. Missing
//! objects on the way are created, and a value on the way that is not an object is replaced
//! by one.

use serde_json::{Map, Value};

use crate::Error;

/// Most operations in one patch.
pub const PATCH_MAX_OPS: usize = 256;
/// Most keys in one path.
pub const PATCH_MAX_DEPTH: usize = 4;

/// One change to a node's props.
#[derive(Clone, Debug, PartialEq)]
pub enum PropOp {
    /// Sets the value at `path`.
    Set {
        /// Where.
        path: Vec<String>,
        /// The value.
        value: Value,
    },
    /// Sets the value at `path` when it is missing or null.
    SetIfAbsent {
        /// Where.
        path: Vec<String>,
        /// The value.
        value: Value,
    },
    /// Removes the key at `path`, if there.
    Remove {
        /// Where.
        path: Vec<String>,
    },
    /// Appends `value` to the list at `path`, unless the list holds it already or holds `max`
    /// entries. A missing list, or a value that is not a list, starts empty.
    AddToSet {
        /// Where.
        path: Vec<String>,
        /// The entry.
        value: Value,
        /// Most entries.
        max: usize,
    },
    /// Appends `value` to the list at `path` after removing the entries that agree with it on
    /// every field in `same` (the whole value when `same` is empty), then drops the oldest
    /// entries past `max`. A missing list, or a value that is not a list, starts empty.
    PushCapped {
        /// Where.
        path: Vec<String>,
        /// The entry.
        value: Value,
        /// The fields that say two entries are the same.
        same: Vec<String>,
        /// Most entries.
        max: usize,
    },
}

impl PropOp {
    fn path(&self) -> &[String] {
        match self {
            Self::Set { path, .. }
            | Self::SetIfAbsent { path, .. }
            | Self::Remove { path }
            | Self::AddToSet { path, .. }
            | Self::PushCapped { path, .. } => path,
        }
    }
}

/// Changes to a node's props, applied in order.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PropsPatch {
    ops: Vec<PropOp>,
}

fn owned(path: &[&str]) -> Vec<String> {
    path.iter().map(|&k| k.to_owned()).collect()
}

impl PropsPatch {
    /// An empty patch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a [`PropOp::Set`].
    #[must_use]
    pub fn set(mut self, path: &[&str], value: Value) -> Self {
        self.ops.push(PropOp::Set {
            path: owned(path),
            value,
        });
        self
    }

    /// Adds a [`PropOp::SetIfAbsent`].
    #[must_use]
    pub fn set_if_absent(mut self, path: &[&str], value: Value) -> Self {
        self.ops.push(PropOp::SetIfAbsent {
            path: owned(path),
            value,
        });
        self
    }

    /// Adds a [`PropOp::Remove`].
    #[must_use]
    pub fn remove(mut self, path: &[&str]) -> Self {
        self.ops.push(PropOp::Remove { path: owned(path) });
        self
    }

    /// Adds a [`PropOp::AddToSet`].
    #[must_use]
    pub fn add_to_set(mut self, path: &[&str], value: Value, max: usize) -> Self {
        self.ops.push(PropOp::AddToSet {
            path: owned(path),
            value,
            max,
        });
        self
    }

    /// Adds a [`PropOp::PushCapped`].
    #[must_use]
    pub fn push_capped(mut self, path: &[&str], value: Value, same: &[&str], max: usize) -> Self {
        self.ops.push(PropOp::PushCapped {
            path: owned(path),
            value,
            same: owned(same),
            max,
        });
        self
    }

    /// The operations, in order.
    pub fn ops(&self) -> &[PropOp] {
        &self.ops
    }

    /// Whether the patch changes nothing.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Checks the patch before it reaches a backend: at most [`PATCH_MAX_OPS`] operations,
    /// each path 1 to [`PATCH_MAX_DEPTH`] non-empty keys, and list caps of at least 1.
    pub fn validate(&self) -> Result<(), Error> {
        if self.ops.len() > PATCH_MAX_OPS {
            return Err(Error::Invalid(format!(
                "a props patch has at most {PATCH_MAX_OPS} operations"
            )));
        }
        for op in &self.ops {
            let path = op.path();
            if path.is_empty() || path.len() > PATCH_MAX_DEPTH || path.iter().any(String::is_empty)
            {
                return Err(Error::Invalid(format!(
                    "a props path is 1 to {PATCH_MAX_DEPTH} non-empty keys"
                )));
            }
            if let PropOp::AddToSet { max, .. } | PropOp::PushCapped { max, .. } = op
                && *max == 0
            {
                return Err(Error::Invalid(
                    "a props list keeps at least 1 entry".to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// Applies the patch to `props`. Backends call this on the props as stored, with the
    /// node locked.
    pub fn apply(&self, props: &mut Map<String, Value>) {
        for op in &self.ops {
            apply_op(op, props);
        }
    }
}

/// The object that holds the last key of `path`, created on the way.
fn parent<'a>(props: &'a mut Map<String, Value>, path: &[String]) -> &'a mut Map<String, Value> {
    let mut map = props;
    for k in &path[..path.len() - 1] {
        let v = map
            .entry(k.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        if !v.is_object() {
            *v = Value::Object(Map::new());
        }
        let Value::Object(next) = v else {
            unreachable!("made an object above")
        };
        map = next;
    }
    map
}

/// The list at the last key of `path` in `map`, created (or replacing a non-list).
fn list<'a>(map: &'a mut Map<String, Value>, key: &str) -> &'a mut Vec<Value> {
    let v = map
        .entry(key.to_owned())
        .or_insert_with(|| Value::Array(Vec::new()));
    if !v.is_array() {
        *v = Value::Array(Vec::new());
    }
    let Value::Array(l) = v else {
        unreachable!("made a list above")
    };
    l
}

fn apply_op(op: &PropOp, props: &mut Map<String, Value>) {
    let path = op.path();
    let Some(last) = path.last() else {
        return;
    };
    match op {
        PropOp::Set { value, .. } => {
            parent(props, path).insert(last.clone(), value.clone());
        }
        PropOp::SetIfAbsent { value, .. } => {
            let map = parent(props, path);
            if map.get(last).is_none_or(Value::is_null) {
                map.insert(last.clone(), value.clone());
            }
        }
        PropOp::Remove { .. } => {
            let mut map = props;
            for k in &path[..path.len() - 1] {
                match map.get_mut(k) {
                    Some(Value::Object(next)) => map = next,
                    _ => return,
                }
            }
            map.remove(last);
        }
        PropOp::AddToSet { value, max, .. } => {
            let l = list(parent(props, path), last);
            if l.len() < *max && !l.contains(value) {
                l.push(value.clone());
            }
        }
        PropOp::PushCapped {
            value, same, max, ..
        } => {
            let l = list(parent(props, path), last);
            let field = |v: &Value, f: &str| v.get(f).cloned().unwrap_or(Value::Null);
            if same.is_empty() {
                l.retain(|x| x != value);
            } else {
                l.retain(|x| same.iter().any(|f| field(x, f) != field(value, f)));
            }
            l.push(value.clone());
            let extra = l.len().saturating_sub(*max);
            l.drain(..extra);
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn applied(start: Value, patch: &PropsPatch) -> Value {
        let Value::Object(mut m) = start else {
            panic!("an object")
        };
        patch.apply(&mut m);
        Value::Object(m)
    }

    #[test]
    fn set_and_set_if_absent() {
        let p = PropsPatch::new()
            .set(&["a"], json!(1))
            .set(&["n", "x"], json!("y"))
            .set_if_absent(&["name"], json!("new"))
            .set_if_absent(&["form"], json!("doo"))
            .set_if_absent(&["gone"], json!(true));
        let got = applied(
            json!({"a": 0, "n": 5, "name": "old", "form": null, "keep": [1]}),
            &p,
        );
        assert_eq!(
            got,
            json!({"a": 1, "n": {"x": "y"}, "name": "old", "form": "doo", "gone": true, "keep": [1]})
        );
    }

    #[test]
    fn remove() {
        let p = PropsPatch::new()
            .remove(&["a"])
            .remove(&["n", "x"])
            .remove(&["s", "x"])
            .remove(&["missing", "x"]);
        let got = applied(json!({"a": 1, "n": {"x": 1, "y": 2}, "s": "str"}), &p);
        assert_eq!(got, json!({"n": {"y": 2}, "s": "str"}));
    }

    #[test]
    fn add_to_set_is_unique_and_capped() {
        let p = PropsPatch::new()
            .add_to_set(&["l"], json!("a"), 2)
            .add_to_set(&["l"], json!("b"), 2)
            .add_to_set(&["l"], json!("c"), 2)
            .add_to_set(&["ids", "lei"], json!("X"), 5)
            .add_to_set(&["bad"], json!(1), 5);
        let got = applied(json!({"l": ["a"], "bad": "x"}), &p);
        assert_eq!(
            got,
            json!({"l": ["a", "b"], "ids": {"lei": ["X"]}, "bad": [1]})
        );
    }

    #[test]
    fn push_capped_replaces_the_same_and_drops_the_oldest() {
        let p = PropsPatch::new()
            .push_capped(&["m"], json!({"doc": 1, "v": 2}), &["doc"], 3)
            .push_capped(&["m"], json!({"doc": 4, "v": 1}), &["doc"], 3)
            .push_capped(&["w"], json!("x"), &[], 2);
        let got = applied(
            json!({"m": [{"doc": 1, "v": 1}, {"doc": 2}, {"doc": 3}], "w": ["x", "y"]}),
            &p,
        );
        assert_eq!(
            got,
            json!({"m": [{"doc": 3}, {"doc": 1, "v": 2}, {"doc": 4, "v": 1}], "w": ["y", "x"]})
        );
    }

    #[test]
    fn validation() {
        assert!(PropsPatch::new().set(&["a"], json!(1)).validate().is_ok());
        assert!(PropsPatch::new().set(&[], json!(1)).validate().is_err());
        assert!(
            PropsPatch::new()
                .set(&["a", ""], json!(1))
                .validate()
                .is_err()
        );
        assert!(
            PropsPatch::new()
                .set(&["a", "b", "c", "d", "e"], json!(1))
                .validate()
                .is_err()
        );
        assert!(
            PropsPatch::new()
                .add_to_set(&["a"], json!(1), 0)
                .validate()
                .is_err()
        );
        let mut big = PropsPatch::new();
        for _ in 0..=PATCH_MAX_OPS {
            big = big.remove(&["a"]);
        }
        assert!(big.validate().is_err());
    }
}
