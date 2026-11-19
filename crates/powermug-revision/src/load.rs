//! Strict JSON loading (PM4, §10.1).
//!
//! Standard JSON parsers keep the last of two equal keys and read every number as a
//! double. Both behaviours hide errors in a revision, so this loader refuses duplicate
//! keys, integers outside the I-JSON safe range and non-finite numbers.

use std::cell::RefCell;
use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};

use crate::error::LoadError;

/// The largest integer that an IEEE double holds exactly (2^53 − 1).
const I_JSON_MAX: i128 = 9_007_199_254_740_991;

/// Parses a JSON document, refusing duplicate keys and ambiguous numbers.
///
/// # Errors
///
/// Returns a [`LoadError`] describing the first refused construct and its JSON pointer.
pub fn parse_strict_json(text: &str) -> Result<Value, LoadError> {
    let refused = RefCell::new(None);
    let ctx = Ctx { refused: &refused };
    let mut de = serde_json::Deserializer::from_str(text);
    let result = StrictSeed {
        ctx: &ctx,
        path: String::new(),
    }
    .deserialize(&mut de);
    match result {
        Ok(value) => {
            de.end().map_err(|e| LoadError::Syntax(e.to_string()))?;
            Ok(value)
        }
        Err(e) => Err(refused
            .into_inner()
            .unwrap_or_else(|| LoadError::Syntax(e.to_string()))),
    }
}

/// State shared by one parse: the first refusal, which serde cannot carry by type.
struct Ctx<'a> {
    refused: &'a RefCell<Option<LoadError>>,
}

impl Ctx<'_> {
    /// Records the refusal and returns the error that stops the parse.
    fn refuse<E: de::Error>(&self, error: LoadError) -> E {
        let message = error.to_string();
        self.refused.borrow_mut().get_or_insert(error);
        E::custom(message)
    }
}

/// Carries the JSON pointer of the value being read, so errors can name it.
struct StrictSeed<'a, 'c> {
    ctx: &'a Ctx<'c>,
    path: String,
}

impl<'de> DeserializeSeed<'de> for StrictSeed<'_, '_> {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(StrictVisitor {
            ctx: self.ctx,
            path: self.path,
        })
    }
}

struct StrictVisitor<'a, 'c> {
    ctx: &'a Ctx<'c>,
    path: String,
}

fn child_path(parent: &str, token: &str) -> String {
    let escaped = token.replace('~', "~0").replace('/', "~1");
    format!("{parent}/{escaped}")
}

impl<'de> Visitor<'de> for StrictVisitor<'_, '_> {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a JSON value")
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
        Ok(Value::String(v.to_owned()))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
        Ok(Value::String(v))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
        self.check_integer(i128::from(v), &v.to_string())?;
        Ok(Value::Number(Number::from(v)))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
        self.check_integer(i128::from(v), &v.to_string())?;
        Ok(Value::Number(Number::from(v)))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
        match Number::from_f64(v) {
            Some(n) => Ok(Value::Number(n)),
            None => Err(self.ctx.refuse(LoadError::NonFinite { path: self.path })),
        }
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        let mut index = 0usize;
        while let Some(item) = seq.next_element_seed(StrictSeed {
            ctx: self.ctx,
            path: child_path(&self.path, &index.to_string()),
        })? {
            items.push(item);
            index += 1;
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut out = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if out.contains_key(&key) {
                return Err(self.ctx.refuse(LoadError::DuplicateKey {
                    key,
                    path: self.path.clone(),
                }));
            }
            let value = map.next_value_seed(StrictSeed {
                ctx: self.ctx,
                path: child_path(&self.path, &key),
            })?;
            out.insert(key, value);
        }
        Ok(Value::Object(out))
    }
}

impl StrictVisitor<'_, '_> {
    fn check_integer<E: de::Error>(&self, value: i128, text: &str) -> Result<(), E> {
        if value.abs() > I_JSON_MAX {
            return Err(self.ctx.refuse(LoadError::UnsafeInteger {
                value: text.to_owned(),
                path: self.path.clone(),
            }));
        }
        Ok(())
    }
}
