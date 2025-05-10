//! Helper to manage a pool of objects that can communicate back to a manager.

use std::future::Future;
use tokio::sync::mpsc::{OwnedPermit, Receiver, Sender};

enum ChannelReport<R> {
    /// The Reporter for the D item at the .0 position in the inventory got dropped.
    Dropped(usize),
    /// The Reporter for the D item at the .0 position in the inventory
    /// sent us message .1.
    Message(usize, R),
}

enum InventoryEntry<D> {
    Vacant(Option<usize>),
    Occupied(D),
}

/// Fixed-capacity inventory of objects that can communicate back to the owner
/// of the inventory.
pub(crate) struct Inventory<D, R> {
    entries: Box<[InventoryEntry<D>]>,
    /// None if the array is full else Some(index) of the head of a linked
    /// list of Vacant entries.
    next_free: Option<usize>,
    /// Channel end for receiving communications from entries.
    report_rx: Receiver<ChannelReport<R>>,
    /// Channel end which we never send on but clone and give to new Reporters.
    report_tx: Sender<ChannelReport<R>>,
}

pub(crate) struct Reporter<R> {
    /// Always filled in with an OwnedPermit so that the Drop implementation
    /// can send a ChannelReport::Dropped without blocking. Whenever we
    /// consume te permit to send a ChannelReport::Message instead we
    /// immediately replenish it.
    report_tx: Option<OwnedPermit<ChannelReport<R>>>,
    index: usize,
}

#[derive(Debug, PartialEq)]
pub(crate) enum InventoryReport<'a, D, R> {
    Dropped(D),
    Message(&'a mut D, R),
}

impl<R> Reporter<R> {
    pub(crate) async fn send(&mut self, report: R) {
        if let Some(permit) = self.report_tx.take() {
            self.report_tx = permit
                .send(ChannelReport::Message(self.index, report))
                .reserve_owned()
                .await
                .ok();
        }
    }
}

impl<R> Drop for Reporter<R> {
    fn drop(&mut self) {
        if let Some(permit) = self.report_tx.take() {
            let _ = permit.send(ChannelReport::Dropped(self.index));
        }
    }
}

impl<D, R> Inventory<D, R> {
    pub(crate) fn new(size: usize) -> Self {
        let (report_tx, report_rx) = tokio::sync::mpsc::channel(size * 3);
        Self {
            entries: std::iter::successors(Some(0), |i| Some(i + 1))
                .map(|i| InventoryEntry::Vacant(if i > 0 { Some(i - 1) } else { None }))
                .take(size)
                .collect(),
            next_free: if size > 0 { Some(size - 1) } else { None },
            report_rx,
            report_tx,
        }
    }

    pub(crate) fn allocate(&mut self, data: D) -> impl Future<Output = Reporter<R>> {
        let i = self
            .next_free
            .expect("Attempted to allocate more than inventory will fit");
        let old = std::mem::replace(&mut self.entries[i], InventoryEntry::Occupied(data));
        let InventoryEntry::Vacant(next_free) = old else {
            panic!("Inventory.next_free did not point at a vacant entry");
        };
        self.next_free = next_free;
        let fut = self.report_tx.clone().reserve_owned();
        async move {
            Reporter {
                report_tx: fut.await.ok(),
                index: i,
            }
        }
    }

    pub(crate) async fn recv(&mut self) -> InventoryReport<'_, D, R> {
        match self.report_rx.recv().await {
            None => panic!("should not fail since we hold a Sender"),
            Some(ChannelReport::Message(i, report)) => {
                let InventoryEntry::Occupied(data) = &mut self.entries[i] else {
                    panic!("Inventory received a report for an entry that was not occupied");
                };
                InventoryReport::Message(data, report)
            }
            Some(ChannelReport::Dropped(i)) => {
                let old =
                    std::mem::replace(&mut self.entries[i], InventoryEntry::Vacant(self.next_free));
                self.next_free = Some(i);
                let InventoryEntry::Occupied(data) = old else {
                    panic!("Inventory received a drop for an entry that was not occupied");
                };
                InventoryReport::Dropped(data)
            }
        }
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut D> + '_ {
        self.entries.iter_mut().filter_map(|e| match e {
            InventoryEntry::Vacant(_) => None,
            InventoryEntry::Occupied(ref mut e) => Some(e),
        })
    }

    #[cfg(feature = "diag")]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &D> + '_ {
        self.entries.iter().filter_map(|e| match e {
            InventoryEntry::Vacant(_) => None,
            InventoryEntry::Occupied(ref e) => Some(e),
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::poll;
    use std::pin::pin;
    use std::task::Poll;

    use super::*;

    #[tokio::test]
    async fn two_reporting() {
        let mut inv = Inventory::new(2);
        let mut one = inv.allocate(1).await;
        let mut two = inv.allocate(2).await;
        one.send("happy").await;
        two.send("sad").await;
        std::mem::drop(two);
        std::mem::drop(one);
        assert_eq!(inv.recv().await, InventoryReport::Message(&mut 1, "happy"));
        assert_eq!(inv.recv().await, InventoryReport::Message(&mut 2, "sad"));
        assert_eq!(inv.recv().await, InventoryReport::Dropped(2));
        assert_eq!(inv.recv().await, InventoryReport::Dropped(1));
        assert_eq!(poll!(pin!(inv.recv())), Poll::Pending);
    }

    #[test]
    #[should_panic]
    fn too_many() {
        let mut inv = Inventory::<_, ()>::new(2);
        let _one = inv.allocate(1);
        let _two = inv.allocate(2);
        let _three = inv.allocate(3);
    }

    #[tokio::test]
    async fn update_state() {
        let mut inv = Inventory::new(1);
        let mut one = inv.allocate(1).await;
        one.send("happy").await;
        std::mem::drop(one);
        if let InventoryReport::Message(data, _) = inv.recv().await {
            *data = 42;
        }
        assert_eq!(inv.recv().await, InventoryReport::Dropped(42));
    }

    #[tokio::test]
    async fn reuse_slots() {
        let mut inv = Inventory::<_, ()>::new(2);
        let one = inv.allocate(1);
        let _two = inv.allocate(2);
        std::mem::drop(one.await);
        let _ = inv.recv().await;
        let _three = inv.allocate(3);
    }
}
