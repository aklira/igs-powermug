//! Partition ownership and its epoch (SPECIFICATIONS 9.4, FUN-MGT-009, FUN-MGT-010).
//!
//! Each partition has an epoch that increases at every takeover. A partition commands only
//! while it has a confirmed owner: a partition in cession, in shadow mode, or without an owner
//! refuses commands.

use std::collections::BTreeMap;

/// The owner of a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    /// This node is the confirmed owner, at this epoch.
    Confirmed { epoch: u64 },
    /// The partition is being handed over: no new command.
    Ceding,
    /// Shadow mode: the partition is observed, never commanded.
    Shadow,
    /// No confirmed owner.
    Unowned,
}

/// The ownership of every partition of the cell.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ownership {
    partitions: BTreeMap<String, (Owner, u64)>,
}

impl Ownership {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The owner of a partition, `Unowned` when it is not known.
    #[must_use]
    pub fn owner(&self, partition: &str) -> Owner {
        self.partitions
            .get(partition)
            .map_or(Owner::Unowned, |(owner, _)| *owner)
    }

    /// The last epoch the partition had, even when it is not owned now.
    #[must_use]
    pub fn last_epoch(&self, partition: &str) -> u64 {
        self.partitions
            .get(partition)
            .map_or(0, |(_, epoch)| *epoch)
    }

    /// Takes the partition over: its epoch increases, and this node is the confirmed owner.
    /// Returns the new epoch.
    pub fn takeover(&mut self, partition: &str) -> u64 {
        let epoch = self.last_epoch(partition) + 1;
        self.partitions
            .insert(partition.to_owned(), (Owner::Confirmed { epoch }, epoch));
        epoch
    }

    /// Puts the partition in cession, shadow mode, or no owner. Its epoch is kept.
    pub fn release(&mut self, partition: &str, owner: Owner) {
        let epoch = self.last_epoch(partition);
        self.partitions.insert(partition.to_owned(), (owner, epoch));
    }
}
