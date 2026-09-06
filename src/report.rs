//! Helper to manage a pool of objects that can communicate back to a manager.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

enum InventoryEntry<D> {
    Vacant(Option<usize>),
    Occupied(D),
}

#[repr(transparent)]
struct EntryPointer(AtomicUsize);

impl Default for EntryPointer {
    fn default() -> Self {
        Self(usize::MAX.into())
    }
}

impl EntryPointer {
    fn is_set(&self) -> bool {
        self.0.load(Ordering::Acquire) != usize::MAX
    }

    fn set_head(&self, i: usize) {
        self.0.store(i, Ordering::Release)
    }

    fn insert(&self, i: usize) -> Option<usize> {
        match self.0.swap(i, Ordering::SeqCst) {
            usize::MAX => None,
            i => Some(i),
        }
    }

    fn consume(&self) -> Option<usize> {
        self.insert(usize::MAX)
    }

    fn clear_tail(&self, expected_i: usize) -> Result<(), ()> {
        match self.0.compare_exchange_weak(
            expected_i,
            usize::MAX,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(()),
        }
    }
}

pub(crate) trait Clear {
    fn clear(&self);
}

impl Clear for () {
    fn clear(&self) {}
}

impl<T> Clear for atomicbox::AtomicOptionBox<T> {
    fn clear(&self) {
        let _ = self.take(Ordering::AcqRel);
    }
}

#[derive(Default)]
struct InventorySharedEntry<S> {
    attention_next: EntryPointer,
    dropped: std::sync::atomic::AtomicBool,
    contents: S,
}

impl<S: Clear> Clear for InventorySharedEntry<S> {
    fn clear(&self) {
        self.dropped.store(false, Ordering::Release);
        self.contents.clear();
    }
}

struct InventoryShared<S> {
    attention_head: EntryPointer,
    attention_tail: EntryPointer,
    notify: Notify,
    entries: Box<[InventorySharedEntry<S>]>,
}

impl<S> InventoryShared<S> {
    fn dropped(&self, subject: usize) {
        self.entries[subject].dropped.store(true, Ordering::Release);
        self.request_attention(subject);
    }

    fn request_attention(&self, subject: usize) {
        if self.entries[subject].attention_next.is_set() {
            return;
        }
        match self.attention_tail.insert(subject) {
            None => {
                // head must be usize::MAX too
                self.attention_head.set_head(subject);
                self.notify.notify_waiters();
            }
            Some(t) if t == subject => (),
            Some(old_tail) => {
                if self.entries[old_tail]
                    .attention_next
                    .insert(subject)
                    .is_some()
                {
                    panic!("attention list attention_next was populated");
                }
            }
        }
    }

    async fn take_attention_head(&self) -> usize {
        loop {
            break match self.attention_head.consume() {
                None => {
                    let w = self.notify.notified();
                    match self.attention_head.consume() {
                        None => {
                            w.await;
                            continue;
                        }
                        Some(h) => h,
                    }
                }
                Some(h) => h,
            };
        }
    }

    fn next_attention(&self, subject: usize) -> Option<usize> {
        loop {
            break match self.entries[subject].attention_next.consume() {
                None => {
                    if let Err(()) = self.attention_tail.clear_tail(subject) {
                        continue;
                    }
                    None
                }
                Some(i) => Some(i),
            };
        }
    }
}

/// Fixed-capacity inventory of objects that can communicate back to the owner
/// of the inventory.
pub(crate) struct Inventory<D, S> {
    entries: Box<[InventoryEntry<D>]>,
    /// None if the array is full else Some(index) of the head of a linked
    /// list of Vacant entries.
    next_free: Option<usize>,
    shared: Arc<InventoryShared<S>>,
    attention_head: Option<usize>,
}

pub(crate) struct Reporter<S> {
    index: usize,
    shared: Arc<InventoryShared<S>>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum InventoryReport<'a, D, S> {
    Dropped(D, &'a S),
    Attention(&'a mut D, &'a S),
}

impl<S> Reporter<S> {
    pub(crate) fn request_attention(&self) {
        self.shared.request_attention(self.index)
    }

    pub(crate) fn shared(&self) -> &S {
        &self.shared.entries[self.index].contents
    }
}

impl<S> Drop for Reporter<S> {
    fn drop(&mut self) {
        self.shared.dropped(self.index)
    }
}

impl<D, S> Inventory<D, S>
where
    S: Default + Clear,
{
    pub(crate) fn new(size: usize) -> Self {
        Self {
            entries: std::iter::successors(Some(0), |i| Some(i + 1))
                .map(|i| InventoryEntry::Vacant(if i > 0 { Some(i - 1) } else { None }))
                .take(size)
                .collect(),
            shared: Arc::new(InventoryShared {
                entries: std::iter::repeat_n((), size)
                    .map(|()| InventorySharedEntry::default())
                    .collect(),
                attention_head: EntryPointer::default(),
                attention_tail: EntryPointer::default(),
                notify: Notify::new(),
            }),
            next_free: if size > 0 { Some(size - 1) } else { None },
            attention_head: None,
        }
    }

    pub(crate) fn allocate(&mut self, data: D) -> Reporter<S> {
        let i = self
            .next_free
            .expect("Attempted to allocate more than inventory will fit");
        let old = std::mem::replace(&mut self.entries[i], InventoryEntry::Occupied(data));
        let InventoryEntry::Vacant(next_free) = old else {
            panic!("Inventory.next_free did not point at a vacant entry");
        };
        self.shared.entries[i].clear();
        self.next_free = next_free;
        let shared = Arc::clone(&self.shared);
        Reporter { index: i, shared }
    }

    pub(crate) async fn recv(&mut self) -> InventoryReport<'_, D, S> {
        let subject = match self.attention_head.take() {
            Some(i) => i,
            None => self.shared.take_attention_head().await,
        };
        self.attention_head = self.shared.next_attention(subject);
        if self.shared.entries[subject].dropped.load(Ordering::Acquire) {
            let old = std::mem::replace(
                &mut self.entries[subject],
                InventoryEntry::Vacant(self.next_free),
            );
            self.next_free = Some(subject);
            let InventoryEntry::Occupied(data) = old else {
                panic!("Inventory received a drop for an entry that was not occupied");
            };
            InventoryReport::Dropped(data, &self.shared.entries[subject].contents)
        } else {
            let InventoryEntry::Occupied(data) = &mut self.entries[subject] else {
                panic!("Inventory received a report for an entry that was not occupied");
            };
            InventoryReport::Attention(data, &self.shared.entries[subject].contents)
        }
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut D> + '_ {
        self.entries.iter_mut().filter_map(|e| match e {
            InventoryEntry::Vacant(_) => None,
            InventoryEntry::Occupied(e) => Some(e),
        })
    }

    #[cfg(feature = "diag")]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&D, &S)> + '_ {
        self.entries
            .iter()
            .zip(self.shared.entries.iter())
            .filter_map(|(e, s)| match e {
                InventoryEntry::Vacant(_) => None,
                InventoryEntry::Occupied(e) => Some((e, &s.contents)),
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
        let one = inv.allocate(1);
        let two = inv.allocate(2);
        one.request_attention();
        two.request_attention();
        assert_eq!(inv.recv().await, InventoryReport::Attention(&mut 1, &()));
        assert_eq!(inv.recv().await, InventoryReport::Attention(&mut 2, &()));
        std::mem::drop(two);
        std::mem::drop(one);
        assert_eq!(inv.recv().await, InventoryReport::Dropped(2, &()));
        assert_eq!(inv.recv().await, InventoryReport::Dropped(1, &()));
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
        let one = inv.allocate(1);
        one.request_attention();
        if let InventoryReport::Attention(data, &()) = inv.recv().await {
            *data = 42;
        }
        std::mem::drop(one);
        assert_eq!(inv.recv().await, InventoryReport::Dropped(42, &()));
    }

    #[tokio::test]
    async fn reuse_slots() {
        let mut inv = Inventory::<_, ()>::new(2);
        let one = inv.allocate(1);
        let _two = inv.allocate(2);
        std::mem::drop(one);
        let _ = inv.recv().await;
        let _three = inv.allocate(3);
    }
}
