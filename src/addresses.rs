use backoff::backoff::Backoff;
use std::collections::HashMap;

#[derive(Debug)]
struct ResolvedAddress<A, B> {
    addr: A,
    resolved: bool,
    used_count: usize,
    backoff: B,
    next_attempt_at: Option<tokio::time::Instant>,
}

impl<A: Clone, B: Backoff> ResolvedAddress<A, B> {
    fn new(addr: A, backoff: B) -> Self {
        Self {
            addr,
            resolved: true,
            used_count: 0,
            backoff,
            next_attempt_at: None,
        }
    }

    fn unused(&self) -> bool {
        !self.resolved && self.used_count == 0
    }

    fn add(&mut self) -> A {
        if self.next_attempt_at.is_none() {
            self.backoff.reset();
        }
        self.next_attempt_at = match self.backoff.next_backoff() {
            None => {
                self.resolved = false;
                None
            }
            Some(d) => Some(tokio::time::Instant::now() + d),
        };
        self.used_count += 1;
        self.addr.clone()
    }

    fn removed(&mut self) {
        self.used_count -= 1;
    }

    fn mark_success(&mut self) {
        self.next_attempt_at = None;
    }
}

pub(crate) struct ResolvedAddressCollection<A, B> {
    backoff_template: B,
    cursor: usize,
    v: Vec<ResolvedAddress<A, B>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AddressSlot(usize);

#[derive(Debug)]
pub(crate) enum ResolvedAddressChoice {
    NoneAvailable,
    Delay(tokio::time::Instant),
    Candidate(AddressSlot),
}

enum NewAddressStatus<A> {
    Existing(usize),
    New(A),
}

impl<A, B> ResolvedAddressCollection<A, B>
where
    A: std::hash::Hash + std::fmt::Debug + Clone + Eq,
    B: Backoff + Clone + std::fmt::Debug,
{
    pub(crate) fn new(backoff_template: B) -> Self {
        Self {
            backoff_template,
            cursor: 0,
            v: Vec::new(),
        }
    }

    pub(crate) fn new_addresses(&mut self, addrs: Vec<A>) {
        if addrs.is_empty() {
            return;
        }
        for a in self.v.iter_mut() {
            a.resolved = false;
        }
        let existing: HashMap<&A, usize> = self
            .v
            .iter()
            .enumerate()
            .map(|(i, ra)| (&ra.addr, i))
            .collect();
        let (existing, new) = addrs
            .into_iter()
            .map(|a| {
                if let Some(i) = existing.get(&a) {
                    NewAddressStatus::Existing(*i)
                } else {
                    NewAddressStatus::New(a)
                }
            })
            .partition::<Vec<_>, _>(|e| matches!(*e, NewAddressStatus::Existing(_)));
        // Mark the unchanged addresses as still resolved.
        for e in existing.into_iter() {
            match e {
                NewAddressStatus::Existing(i) => {
                    self.v[i].resolved = true;
                }
                _ => unreachable!(),
            }
        }
        // Insert the new addresses.
        let mut spot = 0;
        for a in new.into_iter() {
            match a {
                NewAddressStatus::New(a) => {
                    let n = ResolvedAddress::new(a, self.backoff_template.clone());
                    loop {
                        if spot >= self.v.len() {
                            self.v.push(n);
                            spot = usize::MAX;
                            break;
                        } else if self.v[spot].unused() {
                            self.v[spot] = n;
                            spot += 1;
                            break;
                        } else {
                            spot += 1;
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    pub(crate) fn choose(&mut self) -> ResolvedAddressChoice {
        let mut n = self.v.len();
        let mut soonest_available = None;
        let now = tokio::time::Instant::now();
        while n > 0 {
            if self.cursor >= self.v.len() {
                self.cursor = 0;
            }
            if self.v[self.cursor].resolved {
                match self.v[self.cursor].next_attempt_at {
                    None => {
                        let ret = ResolvedAddressChoice::Candidate(AddressSlot(self.cursor));
                        self.cursor += 1;
                        return ret;
                    }
                    Some(moment) => {
                        if moment <= now {
                            let ret = ResolvedAddressChoice::Candidate(AddressSlot(self.cursor));
                            self.cursor += 1;
                            return ret;
                        }
                        soonest_available = match soonest_available {
                            None => Some(moment),
                            Some(other) => Some(if moment < other { moment } else { other }),
                        };
                    }
                }
            }
            n -= 1;
            self.cursor += 1;
        }
        match soonest_available {
            None => ResolvedAddressChoice::NoneAvailable,
            Some(moment) => ResolvedAddressChoice::Delay(moment),
        }
    }

    pub(crate) fn add(&mut self, i: AddressSlot) -> A {
        self.v[i.0].add()
    }

    pub(crate) fn removed(&mut self, i: AddressSlot) {
        self.v[i.0].removed()
    }

    pub(crate) fn mark_success(&mut self, i: AddressSlot) {
        self.v[i.0].mark_success()
    }
}

#[cfg(test)]
mod tests {
    use backoff::ExponentialBackoff;
    use std::time::Duration;

    use super::*;

    fn test_backoff() -> ExponentialBackoff {
        backoff::ExponentialBackoffBuilder::new()
            .with_randomization_factor(0.0)
            .with_initial_interval(Duration::from_millis(1000))
            .build()
    }

    #[test]
    fn no_addresses_available() {
        let mut addresses = ResolvedAddressCollection::<(), _>::new(test_backoff());
        assert_matches!(addresses.choose(), ResolvedAddressChoice::NoneAvailable);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_after_first_try() {
        let mut addresses = ResolvedAddressCollection::new(test_backoff());
        addresses.new_addresses(vec![0, 1]);
        // We can use both addresses once immediately.
        assert_matches!(
            addresses.choose(),
            ResolvedAddressChoice::Candidate(AddressSlot(0))
        );
        addresses.add(AddressSlot(0));
        assert_matches!(
            addresses.choose(),
            ResolvedAddressChoice::Candidate(AddressSlot(1))
        );
        addresses.add(AddressSlot(1));
        // But then we have to wait.
        assert_matches!(addresses.choose(), ResolvedAddressChoice::Delay(_));
        tokio::time::advance(Duration::from_millis(1000)).await;
        // Now we can use both again, and they will come in the same order.
        assert_matches!(
            addresses.choose(),
            ResolvedAddressChoice::Candidate(AddressSlot(0))
        );
        addresses.add(AddressSlot(0));
        assert_matches!(
            addresses.choose(),
            ResolvedAddressChoice::Candidate(AddressSlot(1))
        );
        addresses.add(AddressSlot(1));

        assert_eq!(addresses.v.len(), 2);
        assert_matches!(
            addresses.v[0],
            ResolvedAddress {
                addr: 0,
                resolved: true,
                used_count: 2,
                ..
            }
        );
        assert_matches!(
            addresses.v[1],
            ResolvedAddress {
                addr: 1,
                resolved: true,
                used_count: 2,
                ..
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn after_success_can_use_immediately() {
        let mut addresses = ResolvedAddressCollection::new(test_backoff());
        addresses.new_addresses(vec![0, 1]);
        // We can use both addresses once immediately.
        assert_matches!(
            addresses.choose(),
            ResolvedAddressChoice::Candidate(AddressSlot(0))
        );
        addresses.add(AddressSlot(0));
        assert_matches!(
            addresses.choose(),
            ResolvedAddressChoice::Candidate(AddressSlot(1))
        );
        addresses.add(AddressSlot(1));
        // But then we have to wait.
        assert_matches!(addresses.choose(), ResolvedAddressChoice::Delay(_));
        // Connection succeeds
        addresses.mark_success(AddressSlot(1));
        // Now we can use that address again.
        assert_matches!(
            addresses.choose(),
            ResolvedAddressChoice::Candidate(AddressSlot(1))
        );
        addresses.add(AddressSlot(1));
        // But then we have to wait again.
        assert_matches!(addresses.choose(), ResolvedAddressChoice::Delay(_));

        assert_eq!(addresses.v.len(), 2);
        assert_matches!(
            addresses.v[0],
            ResolvedAddress {
                addr: 0,
                resolved: true,
                used_count: 1,
                ..
            }
        );
        assert_matches!(
            addresses.v[1],
            ResolvedAddress {
                addr: 1,
                resolved: true,
                used_count: 2,
                ..
            }
        );
    }

    #[test]
    fn new_addresses_match() {
        let mut addresses = ResolvedAddressCollection::new(test_backoff());
        addresses.new_addresses(vec![0, 1]);
        addresses.add(AddressSlot(0));
        addresses.new_addresses(vec![0, 1]);

        assert_eq!(addresses.v.len(), 2);
        assert_matches!(
            addresses.v[0],
            ResolvedAddress {
                addr: 0,
                resolved: true,
                used_count: 1,
                ..
            }
        );
        assert_matches!(
            addresses.v[1],
            ResolvedAddress {
                addr: 1,
                resolved: true,
                used_count: 0,
                ..
            }
        );
    }

    #[test]
    fn new_addresses_partial_match_keep() {
        let mut addresses = ResolvedAddressCollection::new(test_backoff());
        addresses.new_addresses(vec![0, 1]);
        addresses.add(AddressSlot(0));
        addresses.new_addresses(vec![0, 2]);

        assert_eq!(addresses.v.len(), 2);
        assert_matches!(
            addresses.v[0],
            ResolvedAddress {
                addr: 0,
                resolved: true,
                used_count: 1,
                ..
            }
        );
        assert_matches!(
            addresses.v[1],
            ResolvedAddress {
                addr: 2,
                resolved: true,
                used_count: 0,
                ..
            }
        );
    }

    #[test]
    fn new_addresses_partial_match_additional() {
        let mut addresses = ResolvedAddressCollection::new(test_backoff());
        addresses.new_addresses(vec![0, 1]);
        addresses.add(AddressSlot(0));
        addresses.new_addresses(vec![1, 2]);

        assert_eq!(addresses.v.len(), 3);
        assert_matches!(
            addresses.v[0],
            ResolvedAddress {
                addr: 0,
                resolved: false,
                used_count: 1,
                ..
            }
        );
        assert_matches!(
            addresses.v[1],
            ResolvedAddress {
                addr: 1,
                resolved: true,
                used_count: 0,
                ..
            }
        );
        assert_matches!(
            addresses.v[2],
            ResolvedAddress {
                addr: 2,
                resolved: true,
                used_count: 0,
                ..
            }
        );
    }

    #[test]
    fn new_addresses_disjoint_unused() {
        let mut addresses = ResolvedAddressCollection::new(test_backoff());
        addresses.new_addresses(vec![0, 1]);
        addresses.new_addresses(vec![2, 3]);

        assert_eq!(addresses.v.len(), 2);
        assert_matches!(
            addresses.v[0],
            ResolvedAddress {
                addr: 2,
                resolved: true,
                used_count: 0,
                ..
            }
        );
        assert_matches!(
            addresses.v[1],
            ResolvedAddress {
                addr: 3,
                resolved: true,
                used_count: 0,
                ..
            }
        );
    }

    #[test]
    fn new_addresses_disjoint_in_use() {
        let mut addresses = ResolvedAddressCollection::new(test_backoff());
        addresses.new_addresses(vec![0, 1]);
        addresses.add(AddressSlot(1));
        addresses.new_addresses(vec![2, 3]);

        assert_eq!(addresses.v.len(), 3);
        assert_matches!(
            addresses.v[0],
            ResolvedAddress {
                addr: 2,
                resolved: true,
                used_count: 0,
                ..
            }
        );
        assert_matches!(
            addresses.v[1],
            ResolvedAddress {
                addr: 1,
                resolved: false,
                used_count: 1,
                ..
            }
        );
        assert_matches!(
            addresses.v[2],
            ResolvedAddress {
                addr: 3,
                resolved: true,
                used_count: 0,
                ..
            }
        );
    }

    #[test]
    fn removed() {
        let mut addresses = ResolvedAddressCollection::new(test_backoff());
        addresses.new_addresses(vec![0]);
        addresses.add(AddressSlot(0));
        assert_matches!(addresses.v[0], ResolvedAddress { used_count: 1, .. });
        addresses.removed(AddressSlot(0));
        assert_matches!(addresses.v[0], ResolvedAddress { used_count: 0, .. });
    }

    #[test]
    fn empty_update_does_not_clobber() {
        let mut addresses = ResolvedAddressCollection::new(test_backoff());
        addresses.new_addresses(vec![0]);
        assert_matches!(addresses.v[0], ResolvedAddress { resolved: true, .. });
        addresses.new_addresses(vec![]);
        assert_matches!(addresses.v[0], ResolvedAddress { resolved: true, .. });
    }
}
