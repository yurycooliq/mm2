//! Bounded short-horizon spot guard for new public-paper entries, never a price oracle.
use mm2_core::Outcome;
use std::collections::VecDeque;

#[derive(Default)]
pub(crate) struct ReferenceGuard {
    samples: VecDeque<(u64, u64, u64)>, // exchange time, local receive time, price
}
impl ReferenceGuard {
    pub fn evidence(&self) -> Option<(u64, u64, u64, u64)> {
        let &(latest, _, price) = self.samples.back()?;
        let target = latest.checked_sub(1_000)?;
        let &(anchor, _, old_price) = self.samples.iter().rev().find(|s| s.0 <= target)?;
        Some((anchor, old_price, latest, price))
    }
    pub fn observe(&mut self, exchange: u64, received: u64, price: u64) {
        if price == 0 {
            self.samples.clear();
            return;
        }
        if let Some(&(last, local, old_price)) = self.samples.back() {
            if exchange == last && received == local && price == old_price {
                return;
            }
            if exchange <= last || received < local || received.saturating_sub(local) > 1_000 {
                self.samples.clear();
            }
        }
        self.samples.push_back((exchange, received, price));
        while self.samples.len() > 64 {
            self.samples.pop_front();
        }
    }
    /// None means insufficient/stale evidence; Some(None) means no adverse direction.
    pub fn blocked(&self, now_mono: u64, now_wall: u64) -> Option<Option<Outcome>> {
        let &(exchange, received, price) = self.samples.back()?;
        if received > now_mono
            || now_mono - received > 1_000
            || exchange > now_wall
            || now_wall - exchange > 1_000
        {
            return None;
        }
        let target = exchange.checked_sub(1_000)?;
        let &(anchor, _, old_price) = self.samples.iter().rev().find(|s| s.0 <= target)?;
        if exchange - anchor > 1_500 {
            return None;
        }
        let change = i128::from(price) - i128::from(old_price);
        if change.unsigned_abs() * 10_000 < u128::from(old_price) {
            return Some(None);
        }
        Some(Some(if change > 0 {
            Outcome::Down
        } else {
            Outcome::Up
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn direction_warmup_staleness_and_clock_reset_are_fail_closed() {
        let mut g = ReferenceGuard::default();
        g.observe(10_000, 100, 100_000_000);
        assert_eq!(g.blocked(100, 10_000), None);
        g.observe(11_000, 1_100, 100_010_000);
        assert_eq!(g.blocked(1_100, 11_000), Some(Some(Outcome::Down)));
        assert_eq!(g.blocked(2_101, 12_001), None);
        g.observe(10_900, 1_200, 99_000_000);
        assert_eq!(g.blocked(1_200, 11_000), None);
        g.observe(12_000, 2_300, 98_000_000);
        assert_eq!(g.blocked(2_300, 12_000), None);
    }
    #[test]
    fn falling_and_flat_prices_and_bounded_storage() {
        let mut g = ReferenceGuard::default();
        for i in 0..100 {
            g.observe(10_000 + i * 50, i * 50, 100_000_000 - i * 1_000);
        }
        assert_eq!(g.samples.len(), 64);
        assert_eq!(g.blocked(4_950, 14_950), Some(Some(Outcome::Up)));
        let mut flat = ReferenceGuard::default();
        flat.observe(10_000, 0, 100_000_000);
        flat.observe(11_000, 1_000, 100_000_001);
        assert_eq!(flat.blocked(1_000, 11_000), Some(None));
    }
}
