//! Credit window of a South stream.
//!
//! The core grants credits; the adapter spends one per event it sends. A
//! grant that would take the window above its capacity is refused, so that a
//! buggy core cannot let an adapter run ahead of the window it declared.

use thiserror::Error;

/// Credits held by the adapter, with the window it may not exceed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreditWindow {
    capacity: u32,
    available: u32,
}

/// The window cannot take the operation.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum CreditError {
    #[error("requested {requested} credits, {available} available")]
    Exhausted { requested: u32, available: u32 },
    #[error("granting {granted} credits would exceed the window of {capacity}")]
    OverGrant { granted: u32, capacity: u32 },
}

impl CreditWindow {
    /// An empty window with the given capacity. Capacity must be positive.
    #[must_use]
    pub const fn new(capacity: u32) -> Self {
        Self {
            capacity,
            available: 0,
        }
    }

    #[must_use]
    pub const fn capacity(&self) -> u32 {
        self.capacity
    }

    #[must_use]
    pub const fn available(&self) -> u32 {
        self.available
    }

    /// Adds credits returned by the core.
    ///
    /// # Errors
    ///
    /// Returns [`CreditError::OverGrant`] when the window would exceed its capacity.
    pub const fn grant(&mut self, credits: u32) -> Result<(), CreditError> {
        match self.available.checked_add(credits) {
            Some(total) if total <= self.capacity => {
                self.available = total;
                Ok(())
            }
            _ => Err(CreditError::OverGrant {
                granted: credits,
                capacity: self.capacity,
            }),
        }
    }

    /// Spends credits for events about to be sent.
    ///
    /// # Errors
    ///
    /// Returns [`CreditError::Exhausted`] when fewer credits are available.
    pub const fn consume(&mut self, credits: u32) -> Result<(), CreditError> {
        if credits > self.available {
            return Err(CreditError::Exhausted {
                requested: credits,
                available: self.available,
            });
        }
        self.available -= credits;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_then_consume() {
        let mut window = CreditWindow::new(10);
        window.grant(4).expect("fits");
        window.consume(3).expect("available");
        assert_eq!(window.available(), 1);
    }

    #[test]
    fn consume_beyond_available_is_refused_and_changes_nothing() {
        let mut window = CreditWindow::new(10);
        window.grant(2).expect("fits");
        assert_eq!(
            window.consume(3),
            Err(CreditError::Exhausted {
                requested: 3,
                available: 2
            })
        );
        assert_eq!(window.available(), 2);
    }

    #[test]
    fn grant_above_capacity_is_refused() {
        let mut window = CreditWindow::new(10);
        window.grant(8).expect("fits");
        assert_eq!(
            window.grant(3),
            Err(CreditError::OverGrant {
                granted: 3,
                capacity: 10
            })
        );
        assert_eq!(window.available(), 8);
        window.grant(2).expect("exactly the capacity");
        assert_eq!(window.available(), 10);
    }

    #[test]
    fn huge_grant_does_not_overflow() {
        let mut window = CreditWindow::new(u32::MAX);
        window.grant(u32::MAX).expect("fits");
        assert!(window.grant(1).is_err());
    }
}
