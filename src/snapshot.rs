//! A rendez-vous point for objects to offer on-demand reports and for
//! viewers to ask for those reports.
//!
//! Usage:
//!
//! ```ignore
//! use warm_channels::snapshot::Collection;
//!
//! struct Report {
//!     // Information to be reported
//! }
//!
//! static RENDEZ_VOUS: Collection::<Report> = Collection::new();
//!
//! // For participating objects
//! let participant = async {
//!     let e = RENDEZ_VOUS.add();
//!     loop {
//!         e.wait_for_report_requested().await.send(Box::new(Report {
//!             // Information to be reported
//!         }));
//!     }
//! };
//!
//! // For interested viewers
//! let viewer = async {
//!     let reports = RENDEZ_VOUS.request_all(false).collect::<Vec<_>>().await;
//! };
//! ```

use atomicbox::AtomicOptionBox;
use futures::future::FusedFuture;
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use try_lock::TryLock;

// Mask for in_use_or_next_free: if these bits are set then the entry is in use.
const ENTRY_IN_USE: usize = !((1 << (usize::BITS / 2)) - 1);
// Value for in_use_or_next_free
const NO_MORE_FREE_ENTRIES: usize = ENTRY_IN_USE - 1;

// Value for report_gen
const DROPPED_ENTRY: usize = usize::MAX;
// Flags for report_gen
const DELIVERED: usize = 1;
const FULL_REPORT: usize = 2;
const ALL_FLAGS: usize = DELIVERED | FULL_REPORT;

// Do not derive Debug: it might take the TryLocks at the wrong moment.
struct SharedEntry<R> {
    in_use_or_next_free: AtomicUsize,
    report_gen: AtomicUsize,
    report: AtomicOptionBox<R>,
    waiting_until_report_requested: TryLock<Option<Waker>>,
    waiting_for_report: TryLock<Option<Waker>>,
    requestor: tokio::sync::Mutex<usize>,
}

fn try_park(w: &TryLock<Option<Waker>>, cx: &mut Context<'_>) {
    if let Some(mut maybe_waker) = w.try_lock() {
        let park = maybe_waker
            .as_ref()
            .map(|w| !w.will_wake(cx.waker()))
            .unwrap_or(true);
        if park {
            *maybe_waker = Some(cx.waker().clone());
        }
    }
}

pub struct WaitForReportRequested<'a, 'b, R>(Option<&'a mut Entry<'b, R>>);

pub struct ReportRequest<'a, 'b, R>(&'a mut Entry<'b, R>, usize);

impl<R> ReportRequest<'_, '_, R> {
    pub fn send(self, report: Box<R>) {
        self.0.e.deliver_report(self.1, report);
        self.0.already_delivered = self.1;
    }

    pub fn want_full(&self) -> bool {
        self.1 & FULL_REPORT == FULL_REPORT
    }

    pub fn unique(&self) -> usize {
        self.0.e.in_use_or_next_free.load(Ordering::Relaxed) & !ENTRY_IN_USE
    }
}

impl<'a, 'b, R> Future for WaitForReportRequested<'a, 'b, R> {
    type Output = ReportRequest<'a, 'b, R>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let er = self.0.as_ref().expect("poll after Ready");
        let req = er.e.report_gen.load(Ordering::Acquire);
        if req > er.already_delivered {
            return Poll::Ready(ReportRequest(self.0.take().unwrap(), req | DELIVERED));
        }
        try_park(&er.e.waiting_until_report_requested, cx);
        let req = er.e.report_gen.load(Ordering::Acquire);
        if req > er.already_delivered {
            Poll::Ready(ReportRequest(self.0.take().unwrap(), req | DELIVERED))
        } else {
            Poll::Pending
        }
    }
}

impl<R> FusedFuture for WaitForReportRequested<'_, '_, R> {
    fn is_terminated(&self) -> bool {
        self.0.is_none()
    }
}

pub struct WaitForReportAvailable<'a, R> {
    e: &'a SharedEntry<R>,
    owner: &'a Collection<R>,
    i: usize,
    report_gen: usize,
    _lock: tokio::sync::MutexGuard<'a, usize>,
}

impl<R> Future for WaitForReportAvailable<'_, R> {
    type Output = Option<Box<R>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Box<R>>> {
        let available = self.e.report_gen.load(Ordering::Acquire);
        if available == DROPPED_ENTRY {
            self.owner.free_entry(self.e, self.i);
            return Poll::Ready(None);
        } else if available == self.report_gen | DELIVERED {
            return Poll::Ready(self.e.report.take(Ordering::AcqRel));
        }
        try_park(&self.e.waiting_for_report, cx);
        let available = self.e.report_gen.load(Ordering::Acquire);
        if available == DROPPED_ENTRY {
            self.owner.free_entry(self.e, self.i);
            Poll::Ready(None)
        } else if available == self.report_gen | DELIVERED {
            Poll::Ready(self.e.report.take(Ordering::AcqRel))
        } else {
            Poll::Pending
        }
    }
}

impl<R> Drop for WaitForReportAvailable<'_, R> {
    fn drop(&mut self) {
        self.e.waiting_for_report_try_wake();
    }
}

impl<R> SharedEntry<R> {
    fn waiting_for_report_try_wake(&self) {
        if let Some(waker) = self
            .waiting_for_report
            .try_lock()
            .and_then(|mut w| w.take())
        {
            waker.wake();
        }
    }

    fn deliver_report(&self, report_gen: usize, report: Box<R>) {
        self.report.store(Some(report), Ordering::SeqCst);
        self.report_gen.store(report_gen, Ordering::SeqCst);
        self.waiting_for_report_try_wake();
    }

    async fn request_report(&self, owner: &Collection<R>, i: usize, full: bool) -> Option<Box<R>> {
        let mut requestor = self.requestor.lock().await;
        let old_report_gen = *requestor;
        let report_gen =
            (old_report_gen & !ALL_FLAGS) + ALL_FLAGS + 1 + if full { FULL_REPORT } else { 0 };
        *requestor = report_gen;
        let mut expect = old_report_gen | DELIVERED;
        loop {
            match self.report_gen.compare_exchange(
                expect,
                report_gen,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Err(DROPPED_ENTRY) => {
                    // The channel has been dropped but not moved to the free list yet.
                    return None;
                }
                Err(x) if x <= old_report_gen | DELIVERED => {
                    // An earlier report was delivered.
                    // This happens if we requested a report then did not wait for it.
                    expect = x;
                }
                Err(x) => {
                    panic!(
                        "{} Unexpected report_gen {} (want {})",
                        self.in_use_or_next_free.load(Ordering::Relaxed),
                        x,
                        old_report_gen | DELIVERED
                    );
                }
                Ok(_) => {
                    break;
                }
            }
        }
        if let Some(mut maybe_waker) = self.waiting_until_report_requested.try_lock() {
            if let Some(waker) = maybe_waker.take() {
                waker.wake();
            }
        }
        WaitForReportAvailable {
            e: self,
            owner,
            i,
            report_gen,
            _lock: requestor,
        }
        .await
    }

    fn new(unique: usize) -> Self {
        Self {
            in_use_or_next_free: AtomicUsize::new(ENTRY_IN_USE | unique),
            report_gen: AtomicUsize::new(DELIVERED),
            report: AtomicOptionBox::none(),
            waiting_until_report_requested: TryLock::new(None),
            waiting_for_report: TryLock::new(None),
            requestor: tokio::sync::Mutex::new(DELIVERED),
        }
    }
}

pub struct Collection<R> {
    next_free: AtomicUsize,
    entries: boxcar::Vec<SharedEntry<R>>,
}

pub struct Entry<'a, R> {
    owner: &'a Collection<R>,
    e: &'a SharedEntry<R>,
    already_delivered: usize,
    i: usize,
}

impl<'b, R> Entry<'b, R> {
    pub fn wait_for_report_requested<'a>(&'a mut self) -> WaitForReportRequested<'a, 'b, R> {
        WaitForReportRequested(Some(self))
    }
}

impl<R> Drop for Entry<'_, R> {
    fn drop(&mut self) {
        let report_gen = self.e.report_gen.swap(DROPPED_ENTRY, Ordering::AcqRel);
        if report_gen > self.already_delivered {
            // A report is presently being requested. Wake the waiter and let
            // it free us.
            self.e.waiting_for_report_try_wake();
        } else {
            self.owner.free_entry(self.e, self.i);
        }
    }
}

impl<R> Collection<R> {
    pub const fn new() -> Self {
        Self {
            next_free: AtomicUsize::new(NO_MORE_FREE_ENTRIES),
            entries: boxcar::Vec::new(),
        }
    }

    pub fn add(&self) -> Entry<'_, R> {
        let unique = rand::random::<usize>();
        let mut next_free = self.next_free.load(Ordering::Acquire);
        let i = loop {
            if next_free == NO_MORE_FREE_ENTRIES {
                break self.entries.push(SharedEntry::new(unique));
            }
            let next_next_free = self.entries[next_free]
                .in_use_or_next_free
                .load(Ordering::SeqCst);
            if next_next_free & ENTRY_IN_USE == ENTRY_IN_USE {
                // We lost the race: somebody already claimed next_free.
                next_free = self.next_free.load(Ordering::Acquire);
                continue;
            }
            match self.next_free.compare_exchange_weak(
                next_free,
                next_next_free,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.entries[next_free]
                        .in_use_or_next_free
                        .store(ENTRY_IN_USE | unique, Ordering::Relaxed);
                    self.entries[next_free]
                        .report_gen
                        .store(DELIVERED, Ordering::Release);
                    break next_free;
                }
                Err(replacement_next_free) => {
                    next_free = replacement_next_free;
                }
            }
        };
        let e = &self.entries[i];
        Entry {
            owner: self,
            e,
            already_delivered: e.report_gen.load(Ordering::Acquire),
            i,
        }
    }

    fn free_entry(&self, e: &SharedEntry<R>, i: usize) {
        e.report.store(None, Ordering::AcqRel);
        let _ = e
            .waiting_until_report_requested
            .try_lock()
            .map(|mut v| v.take());
        let _ = e.waiting_for_report.try_lock().map(|mut v| v.take());
        let mut next_free = self.next_free.load(Ordering::Acquire);
        loop {
            e.in_use_or_next_free.store(next_free, Ordering::SeqCst);
            match self.next_free.compare_exchange_weak(
                next_free,
                i,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    break;
                }
                Err(replacement_next_free) => {
                    next_free = replacement_next_free;
                }
            }
        }
    }

    pub fn request_all(&self, full: bool) -> impl Stream<Item = Box<R>> + use<'_, R> {
        self.entries
            .iter()
            .filter_map(|(i, e)| {
                if e.in_use_or_next_free.load(Ordering::Relaxed) & ENTRY_IN_USE == ENTRY_IN_USE {
                    Some(e.request_report(self, i, full))
                } else {
                    None
                }
            })
            .collect::<FuturesUnordered<_>>()
            .filter_map(|mr| async move { mr })
    }

    pub async fn request_by_unique(&self, unique: usize, full: bool) -> Option<Box<R>> {
        if let Some((i, e)) = self
            .entries
            .iter()
            .find(|(_, e)| e.in_use_or_next_free.load(Ordering::Relaxed) == ENTRY_IN_USE | unique)
        {
            e.request_report(self, i, full).await
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::StreamExt;
    use futures::{join, poll};
    use pin_project_lite::pin_project;
    use std::pin::{Pin, pin};
    use tokio::sync::Semaphore;

    #[test]
    fn entry_alloc() {
        let aa = Collection::<()>::new();
        assert_eq!(aa.entries.count(), 0);
        let e1 = aa.add();
        assert_eq!(aa.entries.count(), 1);
        let e2 = aa.add();
        assert_eq!(aa.entries.count(), 2);
        drop(e1);
        assert_eq!(aa.entries.count(), 2);
        let e3 = aa.add();
        assert_eq!(aa.entries.count(), 2);
        let e4 = aa.add();
        assert_eq!(aa.entries.count(), 3);
        drop(e2);
        drop(e3);
        drop(e4);
        assert_eq!(aa.entries.count(), 3);
        let _e5 = aa.add();
        assert_eq!(aa.entries.count(), 3);
        let _e6 = aa.add();
        assert_eq!(aa.entries.count(), 3);
        let _e7 = aa.add();
        assert_eq!(aa.entries.count(), 3);
        let _e8 = aa.add();
        assert_eq!(aa.entries.count(), 4);
    }

    #[tokio::test]
    async fn request_none() {
        let aa = Collection::<()>::new();
        let mut r = pin!(aa.request_all(true));
        assert_eq!(poll!(r.next()), Poll::Ready(None));
    }

    #[tokio::test]
    async fn recycled_entry_skipped() {
        let aa = Collection::<()>::new();
        let _ = aa.add();
        let mut r = pin!(aa.request_all(true));
        assert_eq!(poll!(r.next()), Poll::Ready(None));
    }

    pin_project! {
        struct StartPollExpectPendingThenRelease<'a, F> {
            #[pin] inner: F,
            sem: Option<&'a Semaphore>,
        }
    }

    impl<'a, F> Future for StartPollExpectPendingThenRelease<'a, F>
    where
        F: Future,
        F::Output: std::fmt::Debug,
    {
        type Output = F::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.project();
            if let Some(sem) = this.sem.take() {
                assert_matches!(this.inner.poll(cx), Poll::Pending);
                sem.add_permits(1);
                Poll::Pending
            } else {
                this.inner.poll(cx)
            }
        }
    }

    #[tokio::test]
    async fn wait_for_report_requested_first() {
        let aa = Collection::<u32>::new();
        let sem = Semaphore::new(0);
        let reporter1 = pin!(async {
            let mut e = aa.add();
            let w = e.wait_for_report_requested();
            StartPollExpectPendingThenRelease {
                inner: async {
                    let s = w.await;
                    assert!(s.want_full());
                    s.send(Box::new(1));
                },
                sem: Some(&sem),
            }
            .await;
            let mut w_more = e.wait_for_report_requested();
            assert!(poll!(Pin::new(&mut w_more)).is_pending());
            e
        });
        let reporter2 = pin!(async {
            let mut e = aa.add();
            let w = e.wait_for_report_requested();
            StartPollExpectPendingThenRelease {
                inner: async {
                    let s = w.await;
                    assert!(s.want_full());
                    s.send(Box::new(2));
                },
                sem: Some(&sem),
            }
            .await;
            e
        });
        let requestor = pin!(async {
            // Wait for both reporters to park themselves waiting for a request
            let _ = sem.acquire_many(2);
            // then make the request.
            let reports = aa.request_all(true).collect::<Vec<_>>().await;
            assert_eq!(reports.len(), 2);
            assert_eq!(*reports[0], 1);
            assert_eq!(*reports[1], 2);
        });
        join!(reporter1, reporter2, requestor);
    }

    #[tokio::test]
    async fn drop_while_requesting_report() {
        let aa = Collection::<u32>::new();
        let sem = Semaphore::new(0);
        let e = aa.add();
        let reporter = pin!(async {
            sem.acquire().await.unwrap().forget();
            drop(e);
        });
        let requestor = pin!(async {
            let reports = StartPollExpectPendingThenRelease {
                inner: aa.request_all(true).collect::<Vec<_>>(),
                sem: Some(&sem),
            }
            .await;
            assert_eq!(reports.len(), 0);
        });
        join!(reporter, requestor);
    }

    #[tokio::test]
    async fn wait_for_report_requested_later() {
        let aa = Collection::<u32>::new();
        let sem = Semaphore::new(0);
        let mut e = aa.add();
        let reporter = pin!(async {
            sem.acquire().await.unwrap().forget();
            let s = e.wait_for_report_requested().await;
            assert!(s.want_full());
            s.send(Box::new(1));
        });
        let requestor = pin!(async {
            StartPollExpectPendingThenRelease {
                inner: async {
                    let reports = aa.request_all(true).collect::<Vec<_>>().await;
                    assert_eq!(reports.len(), 1);
                    assert_eq!(*reports[0], 1);
                },
                sem: Some(&sem),
            }
            .await;
        });
        join!(reporter, requestor);
    }

    #[tokio::test]
    async fn sequential_reports() {
        let aa = Collection::<()>::new();
        let mut e = aa.add();
        let requestor = pin!(async {
            let _ = aa.request_all(false).collect::<Vec<_>>().await;
            let _ = aa.request_all(false).collect::<Vec<_>>().await;
        });
        let reporter = pin!(async {
            e.wait_for_report_requested().await.send(Box::new(()));
            e.wait_for_report_requested().await.send(Box::new(()));
        });
        join!(reporter, requestor);
    }

    #[tokio::test]
    async fn by_unique() {
        let aa = Collection::<usize>::new();
        let mut e1 = aa.add();
        let mut e2 = aa.add();
        let reporter1 = pin!(async {
            let s = e1.wait_for_report_requested().await;
            let unique = s.unique();
            s.send(Box::new(unique));
            let s = e1.wait_for_report_requested().await;
            s.send(Box::new(unique));
        });
        let reporter2 = pin!(async {
            let s = e2.wait_for_report_requested().await;
            let unique = s.unique();
            s.send(Box::new(unique));
            let s = e2.wait_for_report_requested().await;
            s.send(Box::new(unique));
        });
        let requestor = pin!(async {
            let uniques = aa.request_all(false).collect::<Vec<_>>().await;
            for unique in uniques {
                assert_eq!(*aa.request_by_unique(*unique, true).await.unwrap(), *unique);
            }
        });
        join!(reporter1, reporter2, requestor);
    }
}
