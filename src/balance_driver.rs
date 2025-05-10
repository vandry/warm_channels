//! Shim layer for driving `tower::balance::p2c::Balance`.
//!
//! `tower::balance::p2c::Balance` only consumes items from its
//! `tower::discover::Discover` stream while the service is being actively
//! polled. This means that if the service's consumer leaves the channel idle
//! without sending RPCs for some time then `tower::discover::Change` requests
//! to add and remove members in the load balancing pool will pile up. While
//! this is not terrible for insertions (after all, subchannels won't be called
//! to service anyway until a request comes in so prepping the ready_cache in
//! advance is not strictly necessary), it is important that deletions get
//! acted on continuously so the evicted channels's resources can be reclaimed.
//!
//! The solution used here is to shim both the `tower::discover::Discover`
//! stream and the `tower_service::Service` stack, so that when we observe
//! a message flow through the stream we start a timer. If the timer fires
//! without the service having been polled (as witnessed by the stream output
//! not yet having been consumed) then we briefly take over the service to
//! poll it once.
//!
//! This comes at the small cost of the `tower_service::Service` needing to be
//! wrapped in a `std::sync::Mutex`, though the mutex should be uncontended
//! since the whole point is that we take it over only when nobody else has
//! been polling it recently anyway.
//!
//! # Usage
//!
//! ```ignore
//! // Given:
//! let discover_stream = futures::stream::empty();
//!
//! // Layer the Shim on top of the Balance like this:
//! let mut shim = crate::balance_driver::Shim::new();
//! let stack = tower::balance::p2c::Balance::new(shim.stream());
//! let (stack, shim_worker) = shim.service_and_worker(stack, discover_stream);
//! tokio::task::spawn(shim_worker);
//! ```

use futures::future::Either;
use futures::{FutureExt, Stream, StreamExt, select};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tower_service::Service;

const INTERVENE_TIME: Duration = Duration::from_millis(30);

pub struct ShimmedService<S>(Arc<Mutex<S>>);

impl<R, S> Service<R> for ShimmedService<S>
where
    S: Service<R>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.lock().unwrap().poll_ready(cx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        self.0.lock().unwrap().call(req)
    }
}

#[derive(Debug)]
enum StreamIntercept<I> {
    Empty,
    Demanding(Waker),
    Full(Vec<Option<I>>),
}

struct ShimmedStreamInner<I> {
    stream_intercept: Mutex<StreamIntercept<I>>,
    stream_delivered: AtomicU64,
}

struct ServiceNudger<'a, R, S: Service<R>>(&'a Weak<Mutex<S>>, PhantomData<R>);

impl<R, S> Future for ServiceNudger<'_, R, S>
where
    S: Service<R>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(svc) = self.0.upgrade() {
            // If try_lock fails we should hope that somebody else is finally polling it.
            if let Ok(mut locked) = svc.try_lock() {
                let _ = locked.poll_ready(cx);
            }
        }
        Poll::Ready(())
    }
}

async fn worker<I, R, S, U>(
    svc: Weak<Mutex<S>>,
    inner: Arc<ShimmedStreamInner<I>>,
    upstream: U,
    mut downstream_alive_detector: want::Giver,
) where
    U: Stream<Item = I>,
    S: Service<R>,
{
    let mut stream_sent: u64 = 0;
    let mut upstream = pin!(upstream.fuse());
    let mut downstream_alive_detector = pin!(downstream_alive_detector.want().fuse());
    let mut stream_continues = true;
    let mut intervene_deadline = None;
    while stream_continues {
        if inner.stream_delivered.load(Ordering::Acquire) < stream_sent {
            if intervene_deadline.is_none() {
                ServiceNudger(&svc, PhantomData).await;
                if inner.stream_delivered.load(Ordering::Acquire) < stream_sent {
                    // Didn't work? Wait again.
                    intervene_deadline = Some(tokio::time::Instant::now() + INTERVENE_TIME);
                }
            }
            // There are undelivered messages but we are waiting to see if
            // the service's owner will poll the service and cause them to
            // be consumed.
        } else {
            // Either there were not recently any messages to deliver
            // or they got delivered on their own.
            intervene_deadline = None;
        }
        let mut alarm = pin!(
            match intervene_deadline {
                None => Either::Left(std::future::pending()),
                Some(ref d) => Either::Right(tokio::time::sleep_until(*d)),
            }
            .fuse()
        );
        select! {
            item = upstream.next() => {
                stream_continues = item.is_some();
                let mut locked = inner.stream_intercept.lock().unwrap();
                if let StreamIntercept::Full(ref mut contents) = *locked {
                    contents.push(item);
                    std::mem::drop(locked);
                } else {
                    let old = std::mem::replace(&mut *locked, StreamIntercept::Full(vec![item]));
                    std::mem::drop(locked);
                    if let StreamIntercept::Demanding(waker) = old {
                        waker.wake();
                    }
                }
                stream_sent += 1;
                if intervene_deadline.is_none() {
                    intervene_deadline = Some(tokio::time::Instant::now() + INTERVENE_TIME);
                }
            }
            _ = alarm => {
                intervene_deadline = None;
            }
            _ = downstream_alive_detector => {
                stream_continues = false;
            }
        }
    }
}

pub struct ShimmedStream<I> {
    inner: Arc<ShimmedStreamInner<I>>,
    fetched: Vec<Option<I>>,
    _downstream_alive_sentinel: want::Taker,
}

impl<I: Unpin> Stream for ShimmedStream<I> {
    type Item = I;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(item) = self.fetched.pop() {
                self.inner.stream_delivered.fetch_add(1, Ordering::Release);
                return Poll::Ready(item);
            }
            let mut locked = self.inner.stream_intercept.lock().unwrap();
            let old = std::mem::replace(&mut *locked, StreamIntercept::Empty);
            match old {
                StreamIntercept::Empty => {
                    *locked = StreamIntercept::Demanding(cx.waker().clone());
                    return Poll::Pending;
                }
                StreamIntercept::Demanding(waker) => {
                    if waker.will_wake(cx.waker()) {
                        *locked = StreamIntercept::Demanding(waker);
                    } else {
                        *locked = StreamIntercept::Demanding(cx.waker().clone());
                        waker.wake();
                    }
                    return Poll::Pending;
                }
                StreamIntercept::Full(mut contents) => {
                    std::mem::drop(locked);
                    contents.reverse();
                    self.fetched = contents;
                }
            }
        }
    }
}

pub(crate) struct Shim<I> {
    inner: Arc<ShimmedStreamInner<I>>,
    downstream_alive_detector: want::Giver,
    downstream_alive_sentinel: Option<want::Taker>,
}

impl<I> Shim<I> {
    pub(crate) fn new() -> Self {
        let (giver, taker) = want::new();
        Self {
            inner: Arc::new(ShimmedStreamInner {
                stream_intercept: Mutex::new(StreamIntercept::Empty),
                stream_delivered: AtomicU64::new(0),
            }),
            downstream_alive_detector: giver,
            downstream_alive_sentinel: Some(taker),
        }
    }

    pub(crate) fn stream(&mut self) -> ShimmedStream<I> {
        ShimmedStream {
            inner: Arc::clone(&self.inner),
            fetched: Vec::new(),
            _downstream_alive_sentinel: self
                .downstream_alive_sentinel
                .take()
                .expect("Shim::stream called more than once"),
        }
    }

    pub(crate) fn service_and_worker<S, R, U>(
        self,
        svc: S,
        upstream: U,
    ) -> (ShimmedService<S>, impl Future<Output = ()>)
    where
        U: Stream<Item = I>,
        S: Service<R>,
    {
        let svc = Arc::new(Mutex::new(svc));
        let weak_svc = Arc::downgrade(&svc);
        let shimmed_service = ShimmedService(svc);
        (
            shimmed_service,
            worker(
                weak_svc,
                self.inner,
                upstream,
                self.downstream_alive_detector,
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use futures::{pin_mut, poll, ready};
    use std::cell::RefCell;
    use tower::ServiceExt;

    use super::*;

    struct TestService<'a> {
        poll_count: &'a RefCell<u32>,
        discover_stream: Pin<Box<ShimmedStream<()>>>,
    }

    impl<'a> TestService<'a> {
        fn new(shim: &mut Shim<()>, poll_count: &'a RefCell<u32>) -> Self {
            Self {
                poll_count,
                discover_stream: Box::pin(shim.stream()),
            }
        }
    }

    impl Service<()> for TestService<'_> {
        type Response = ();
        type Error = ();
        type Future = std::future::Ready<Result<(), ()>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            *self.poll_count.borrow_mut() += 1;
            loop {
                ready!(self.discover_stream.as_mut().poll_next(cx));
            }
        }

        fn call(&mut self, _req: ()) -> Self::Future {
            panic!("Not used in test");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn without_consumer_need_nudge() {
        let mut shim = Shim::new();
        let shim_inner = Arc::clone(&shim.inner); // Spy on the shim.
        let poll_count = RefCell::new(0);
        let inner_svc = TestService::new(&mut shim, &poll_count);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let (_svc, worker) =
            shim.service_and_worker(inner_svc, tokio_stream::wrappers::ReceiverStream::new(rx));
        pin_mut!(worker);

        // Send something along the shimmed stream.
        let _ = tx.send(()).await;
        // Let the worker process it.
        assert!(poll!(&mut worker).is_pending());
        match *shim_inner.stream_intercept.lock().unwrap() {
            StreamIntercept::Full(ref v) if v.len() == 1 => (),
            ref wrong => {
                panic!("wrong contents of stream_intercept {:?}", wrong);
            }
        }
        // But nobody is listening.
        assert_eq!(shim_inner.stream_delivered.load(Ordering::Acquire), 0);

        tokio::time::advance(INTERVENE_TIME).await;
        // Let the worker try again.
        assert!(poll!(&mut worker).is_pending());

        match *shim_inner.stream_intercept.lock().unwrap() {
            StreamIntercept::Full(_) => {
                panic!("stream_intercept should no longer be full");
            }
            _ => (),
        }
        assert_eq!(shim_inner.stream_delivered.load(Ordering::Acquire), 1);
        assert_eq!(*poll_count.borrow(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn do_not_intervene_if_consumer_present() {
        let mut shim = Shim::new();
        let shim_inner = Arc::clone(&shim.inner); // Spy on the shim.
        let poll_count = RefCell::new(0);
        let inner_svc = TestService::new(&mut shim, &poll_count);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let (mut svc, worker) =
            shim.service_and_worker(inner_svc, tokio_stream::wrappers::ReceiverStream::new(rx));
        pin_mut!(worker);

        // Send something along the shimmed stream.
        let _ = tx.send(()).await;
        // Let the worker process it.
        assert!(poll!(&mut worker).is_pending());
        // The service is being actively polled.
        assert!(poll!(svc.ready()).is_pending());
        // It's already delivered
        assert_eq!(shim_inner.stream_delivered.load(Ordering::Acquire), 1);
        assert_eq!(*poll_count.borrow(), 1);

        // Let the worker do its thing, now and after a time.
        assert!(poll!(&mut worker).is_pending());
        tokio::time::advance(INTERVENE_TIME).await;
        assert!(poll!(&mut worker).is_pending());
        // The worker did not feel the need to try any interventions.
        assert_eq!(*poll_count.borrow(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn do_not_intervene_too_soon() {
        let mut shim = Shim::new();
        let shim_inner = Arc::clone(&shim.inner); // Spy on the shim.
        let poll_count = RefCell::new(0);
        let inner_svc = TestService::new(&mut shim, &poll_count);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let (_svc, worker) =
            shim.service_and_worker(inner_svc, tokio_stream::wrappers::ReceiverStream::new(rx));
        pin_mut!(worker);

        // Send something along the shimmed stream.
        let _ = tx.send(()).await;
        // Let the worker process it.
        assert!(poll!(&mut worker).is_pending());
        // But nobody is listening.
        assert_eq!(shim_inner.stream_delivered.load(Ordering::Acquire), 0);

        tokio::time::advance(INTERVENE_TIME - std::time::Duration::from_millis(1)).await;
        // Let the worker try again.
        assert!(poll!(&mut worker).is_pending());

        // Still no
        assert_eq!(shim_inner.stream_delivered.load(Ordering::Acquire), 0);
        assert_eq!(*poll_count.borrow(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn do_not_intervene_if_service_polled_later() {
        let mut shim = Shim::new();
        let shim_inner = Arc::clone(&shim.inner); // Spy on the shim.
        let poll_count = RefCell::new(0);
        let inner_svc = TestService::new(&mut shim, &poll_count);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let (mut svc, worker) =
            shim.service_and_worker(inner_svc, tokio_stream::wrappers::ReceiverStream::new(rx));
        pin_mut!(worker);

        // Send something along the shimmed stream.
        let _ = tx.send(()).await;
        // Let the worker process it.
        assert!(poll!(&mut worker).is_pending());
        // But nobody is listening.
        assert_eq!(shim_inner.stream_delivered.load(Ordering::Acquire), 0);

        tokio::time::advance(INTERVENE_TIME - std::time::Duration::from_millis(1)).await;
        // In the nick of time the consumer wants to send an RPC
        assert!(poll!(svc.ready()).is_pending());
        assert_eq!(*poll_count.borrow(), 1);

        // After this we would have intervened.
        tokio::time::advance(INTERVENE_TIME).await;
        // Only we do not need to.
        assert_eq!(*poll_count.borrow(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn worker_quits_when_service_dropped() {
        let mut shim = Shim::new();
        let poll_count = RefCell::new(0);
        let inner_svc = TestService::new(&mut shim, &poll_count);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let (svc, worker) =
            shim.service_and_worker(inner_svc, tokio_stream::wrappers::ReceiverStream::new(rx));
        pin_mut!(worker);

        assert!(poll!(&mut worker).is_pending());
        std::mem::drop(svc);
        assert!(poll!(&mut worker).is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn worker_quits_when_stream_closed() {
        let mut shim = Shim::new();
        let poll_count = RefCell::new(0);
        let inner_svc = TestService::new(&mut shim, &poll_count);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let (_svc, worker) =
            shim.service_and_worker(inner_svc, tokio_stream::wrappers::ReceiverStream::new(rx));
        pin_mut!(worker);

        assert!(poll!(&mut worker).is_pending());
        std::mem::drop(tx);
        assert!(poll!(&mut worker).is_ready());
    }
}
