//! Which IEC 104 object is which point: the fidelity of addresses (§9.4).
//!
//! An object is identified by the common address of its ASDU and its
//! information object address. The table is built from the partition revision.

use std::collections::HashMap;

use thiserror::Error;

/// Two entries of the table name the same object.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("common address {common_address}, object address {address} is mapped twice")]
pub struct DuplicateObject {
    pub common_address: u16,
    pub address: u32,
}

/// The objects of one station, each mapped to a point identifier.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Mapping {
    points: HashMap<(u16, u32), String>,
}

impl Mapping {
    /// Builds the table from `(common address, object address, point id)`.
    ///
    /// # Errors
    ///
    /// Returns [`DuplicateObject`] when an object is named twice.
    pub fn new(
        entries: impl IntoIterator<Item = (u16, u32, String)>,
    ) -> Result<Self, DuplicateObject> {
        let mut points = HashMap::new();
        for (common_address, address, point_id) in entries {
            if points.insert((common_address, address), point_id).is_some() {
                return Err(DuplicateObject {
                    common_address,
                    address,
                });
            }
        }
        Ok(Self { points })
    }

    /// The point identifier of an object, if the object is mapped.
    #[must_use]
    pub fn point_of(&self, common_address: u16, address: u32) -> Option<&str> {
        self.points
            .get(&(common_address, address))
            .map(String::as_str)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.points.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }
}
