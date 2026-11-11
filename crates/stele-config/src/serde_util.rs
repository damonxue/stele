//! Serde helpers shared by the configuration types.
//!
//! The configuration format accepts both a single value and an array for `rpc`, `address`, and
//! `events`. The helpers below normalize both spellings into a `Vec`.

use serde::{Deserialize, Deserializer};

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    fn into_vec(self) -> Vec<T> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
}

/// Deserializes a single value or an array of values into a `Vec`.
pub(crate) fn deserialize_one_or_many<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(OneOrMany::deserialize(deserializer)?.into_vec())
}

/// Deserializes an optional single value or array into an optional `Vec`.
pub(crate) fn deserialize_optional_one_or_many<'de, D, T>(
    deserializer: D,
) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<OneOrMany<T>>::deserialize(deserializer)?.map(OneOrMany::into_vec))
}
