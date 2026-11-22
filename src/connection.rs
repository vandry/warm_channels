use futures::future::Either;
use futures::{FutureExt, StreamExt, TryFutureExt, select};
use hyper::body::Body;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use pin_project_lite::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, ready};
use thiserror::Error;
use tower_service::Service;

use crate::pool::PoolCommon;
use crate::report::Reporter;
use crate::shared::SubchannelShared;
use crate::subchannel::{Subchannel, SubchannelHandle};
use crate::{Connector, LoggedEvent};

struct SendRequestService<B>(SendRequest<B>);

impl<B> Service<http::Request<B>> for SendRequestService<B>
where
    B: Body + Send + 'static,
{
    type Response = http::Response<hyper::body::Incoming>;
    type Error = hyper::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        Box::pin(self.0.send_request(req))
    }
}

#[derive(Debug)]
enum HealthState<E> {
    Healthy,
    Unhealthy(E),
    NotYetChecked,
}

#[derive(Debug)]
pub struct HTTP2ConnectionHandle<E> {
    reporter: Reporter<SubchannelShared>,
    error: std::sync::Mutex<Option<E>>,
}

impl<E: Clone> HTTP2ConnectionHandle<E> {
    fn new(reporter: Reporter<SubchannelShared>) -> Self {
        Self {
            reporter,
            error: std::sync::Mutex::new(None),
        }
    }

    fn get_healthy(&self) -> HealthState<E> {
        match self.shared().flags.get().healthy() {
            Some(true) => HealthState::Healthy,
            Some(false) => HealthState::Unhealthy(self.error.lock().unwrap().clone().unwrap()),
            None => HealthState::NotYetChecked,
        }
    }
}

impl<E> SubchannelHandle for HTTP2ConnectionHandle<E> {
    fn shared(&self) -> &SubchannelShared {
        self.reporter.shared()
    }
}

#[derive(Debug, Error)]
pub enum HTTP2ConnectionError<CE: std::error::Error, HCE: std::error::Error> {
    #[error("{0}")]
    ConnectorError(#[source] CE),
    #[error("{0}")]
    HyperError(#[from] hyper::Error),
    #[error("server is unhealthy: {0}")]
    Unhealthy(#[source] HCE),
    #[error("connection dropped without reporting an error")]
    ConnectionTaskLost,
}

async fn initial_connect<A, C, ReqBody, HCE>(
    connector: &C,
    address: A,
) -> Result<
    (
        SendRequest<ReqBody>,
        hyper::client::conn::http2::Connection<TokioIo<C::IO>, ReqBody, TokioExecutor>,
    ),
    HTTP2ConnectionError<C::Error, HCE>,
>
where
    C: Connector<A>,
    C::IO: Send + 'static,
    ReqBody: Body + Send + 'static,
    ReqBody::Data: Send,
    ReqBody::Error: std::error::Error + Send + Sync + 'static,
    HCE: std::error::Error,
{
    let io = TokioIo::new(match connector.connect(address).await {
        Ok(io) => io,
        Err(e) => {
            return Err(HTTP2ConnectionError::ConnectorError(e));
        }
    });
    let (sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io).await?;
    Ok((sender, conn))
}

pin_project! {
    #[derive(Debug)]
    struct ConnectingFuture<ReqBody, CE: std::error::Error, HCE: std::error::Error> {
        alive_sentinel: Option<want::Taker>,
        #[pin] ready_notify: tokio::sync::oneshot::Receiver<Result<SendRequest<ReqBody>, HTTP2ConnectionError<CE, HCE>>>,
    }
}

impl<ReqBody, CE, HCE> Future for ConnectingFuture<ReqBody, CE, HCE>
where
    CE: std::error::Error,
    HCE: std::error::Error,
{
    type Output = Result<HTTP2ConnectionInner<ReqBody>, HTTP2ConnectionError<CE, HCE>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();
        // Wait for the first health check.
        Poll::Ready(match ready!(this.ready_notify.poll(cx)) {
            Err(_) => Err(HTTP2ConnectionError::ConnectionTaskLost),
            Ok(Ok(send_request)) => Ok(HTTP2ConnectionInner {
                send_request,
                _sentinel: this.alive_sentinel.take().unwrap(),
            }),
            Ok(Err(e)) => Err(e),
        })
    }
}

fn monitor_connection<A, C, HC, ReqBody>(
    address: A,
    shared: Arc<HTTP2ConnectionShared<C, HC>>,
    handle: Arc<HTTP2ConnectionHandle<HC::Error>>,
) -> ConnectingFuture<ReqBody, C::Error, HC::Error>
where
    A: Send + 'static,
    C: Connector<A> + Send + Sync + 'static,
    C::IO: Send + 'static,
    C::Error: Send + 'static,
    HC: crate::HealthChecker<ReqBody> + Send + Sync + 'static,
    HC::Error: std::error::Error + Clone + Send,
    ReqBody: Body + Send + Unpin + 'static,
    ReqBody::Data: Send,
    ReqBody::Error: std::error::Error + Send + Sync + 'static,
{
    let (mut giver, taker) = want::new();
    let (sender_tx, sender_rx) = tokio::sync::oneshot::channel();
    tokio::task::spawn(async move {
        let reporter = &handle.reporter;
        let stop_signal = pin!(giver.want().fuse());
        let init = pin!(initial_connect::<_, _, ReqBody, HC::Error>(
            &shared.connector,
            address,
        ));
        let (init_result, mut stop_signal) = match futures::future::select(init, stop_signal).await
        {
            Either::Left(x) => x,
            Either::Right(_) => {
                return;
            }
        };
        let (sender, conn) = match init_result {
            Ok(v) => v,
            Err(e) => {
                reporter.shared().error.store(
                    Some(Box::new(LoggedEvent::new(e.to_string()))),
                    Ordering::AcqRel,
                );
                reporter.request_attention();
                let _ = sender_tx.send(Err(e));
                return;
            }
        };
        reporter.shared().flags.set_connected();
        let mut begin_usage = Some((sender.clone(), sender_tx));
        let mut healthy = None;
        let mut hc_stream = pin!(
            shared
                .health_checker
                .watch(SendRequestService(sender))
                .fuse()
        );
        let mut conn = pin!(conn.fuse());
        loop {
            select! {
                r = conn => {
                    if let Err(e) = r {
                        if let Some((_, sender_tx)) = begin_usage.take() {
                            let _ = sender_tx.send(Err(e.into()));
                        }
                    }
                    break;
                }
                _ = stop_signal => {
                    break;
                }
                h = hc_stream.next() => {
                    match h {
                        None => (),
                        Some(Ok(())) => {
                            if !healthy.unwrap_or(false) {
                                healthy = Some(true);
                                reporter.shared().flags.set_healthy();
                                reporter.request_attention();
                            }
                        }
                        Some(Err(e)) => {
                            reporter.shared().error.store(Some(Box::new(LoggedEvent::new(e.to_string()))), Ordering::AcqRel);
                            *handle.error.lock().unwrap() = Some(e);
                            if healthy.unwrap_or(true) {
                                reporter.shared().flags.set_unhealthy();
                                reporter.request_attention();
                                healthy = Some(false);
                            }
                        }
                    }
                    if let Some((sender, sender_tx)) = begin_usage.take() {
                        let _ = sender_tx.send(Ok(sender));
                    }
                }
            }
        }
    });
    ConnectingFuture {
        alive_sentinel: Some(taker),
        ready_notify: sender_rx,
    }
}

#[derive(Debug)]
struct HTTP2ConnectionInner<ReqBody> {
    send_request: SendRequest<ReqBody>,
    _sentinel: want::Taker,
}

#[derive(Debug)]
enum HTTP2ConnectionState<ReqBody, CE: std::error::Error, HCE: std::error::Error> {
    Connecting(Pin<Box<ConnectingFuture<ReqBody, CE, HCE>>>),
    Connected(HTTP2ConnectionInner<ReqBody>),
}

#[derive(Debug)]
struct HTTP2ConnectionShared<C, HC> {
    connector: C,
    health_checker: HC,
}

#[derive(Debug)]
pub struct HTTP2Connection<A, C, HC, ReqBody>
where
    C: Connector<A>,
    HC: crate::HealthChecker<ReqBody>,
    HC::Error: std::error::Error,
{
    pool: Arc<PoolCommon>,
    handle: Arc<HTTP2ConnectionHandle<HC::Error>>,
    state: HTTP2ConnectionState<ReqBody, C::Error, HC::Error>,
}

impl<A, C, HC, ReqBody> HTTP2Connection<A, C, HC, ReqBody>
where
    A: Send + 'static,
    C: Connector<A> + Send + Sync + 'static,
    C::IO: Send,
    C::Error: Send + 'static,
    HC: crate::HealthChecker<ReqBody> + Send + Sync + 'static,
    HC::Error: std::error::Error + Clone + Send,
    ReqBody: Body + Send + Unpin + 'static,
    ReqBody::Data: Send,
    ReqBody::Error: std::error::Error + Send + Sync,
{
    fn new(
        shared: Arc<HTTP2ConnectionShared<C, HC>>,
        pool: Arc<PoolCommon>,
        reporter: Reporter<SubchannelShared>,
        address: A,
    ) -> Self {
        let handle = Arc::new(HTTP2ConnectionHandle::new(reporter));
        // Box::pin is necessary so we can poll it from a Service.
        let fut = Box::pin(monitor_connection::<_, C, HC, ReqBody>(
            address,
            shared,
            Arc::clone(&handle),
        ));
        Self {
            pool,
            handle,
            state: HTTP2ConnectionState::Connecting(fut),
        }
    }
}

impl<A, C, HC, ReqBody> HTTP2Connection<A, C, HC, ReqBody>
where
    A: Send + Sync + 'static,
    C: Connector<A> + Send + Sync + 'static,
    C::IO: Send,
    C::Error: Send + 'static,
    HC: crate::HealthChecker<ReqBody> + Send + Sync + 'static,
    HC::Error: std::error::Error + Clone + Send,
{
    #[allow(clippy::type_complexity)]
    fn poll_connect(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), HTTP2ConnectionError<C::Error, HC::Error>>> {
        match self.state {
            HTTP2ConnectionState::Connecting(ref mut fut) => {
                let handle = ready!(fut.as_mut().poll(cx))?;
                self.state = HTTP2ConnectionState::Connected(handle);
                self.poll_connect(cx)
            }
            HTTP2ConnectionState::Connected(ref mut inner) => {
                match self.handle.get_healthy() {
                    HealthState::NotYetChecked | HealthState::Healthy => {
                        inner.send_request.poll_ready(cx).map_err(Into::into)
                    }
                    HealthState::Unhealthy(e) => {
                        if self.pool.is_critically_unhealthy() {
                            // Emergency healthy
                            inner.send_request.poll_ready(cx).map_err(Into::into)
                        } else {
                            Poll::Ready(Err(HTTP2ConnectionError::Unhealthy(e)))
                        }
                    }
                }
            }
        }
    }
}

impl<A, C, HC, ReqBody> Subchannel for HTTP2Connection<A, C, HC, ReqBody>
where
    C: Connector<A>,
    HC: crate::HealthChecker<ReqBody>,
    HC::Error: std::error::Error,
{
    type Handle = HTTP2ConnectionHandle<HC::Error>;

    fn handle(&self) -> &Arc<Self::Handle> {
        &self.handle
    }
}

impl<A, C, HC, ReqBody> Service<http::Request<ReqBody>> for HTTP2Connection<A, C, HC, ReqBody>
where
    A: Send + Sync + 'static,
    C: Connector<A> + Send + Sync + 'static,
    C::IO: Send,
    C::Error: Send + 'static,
    HC: crate::HealthChecker<ReqBody> + Send + Sync + 'static,
    HC::Error: std::error::Error + Clone + Send,
    ReqBody: Body + Send + Unpin + 'static,
    ReqBody::Data: Send,
    ReqBody::Error: std::error::Error + Send + Sync,
{
    type Response = http::Response<hyper::body::Incoming>;
    type Error = HTTP2ConnectionError<C::Error, HC::Error>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.poll_connect(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => {
                self.pool.set_connection_error(e.to_string());
                Poll::Ready(Err(e))
            }
        }
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let HTTP2ConnectionState::Connected(ref mut handle) = self.state else {
            panic!("HTTP2Connection not ready");
        };
        Box::pin(handle.send_request.send_request(req).err_into())
    }
}

pub(crate) struct HTTP2ConnectionMaker<C, HC, LL, ReqBody> {
    shared: Arc<HTTP2ConnectionShared<C, HC>>,
    ll: LL,
    _reqbody: PhantomData<ReqBody>,
}

impl<C, HC, LL, ReqBody> HTTP2ConnectionMaker<C, HC, LL, ReqBody> {
    pub(crate) fn new(connector: C, health_checker: HC, ll: LL) -> Self {
        Self {
            shared: Arc::new(HTTP2ConnectionShared {
                connector,
                health_checker,
            }),
            ll,
            _reqbody: PhantomData,
        }
    }
}

impl<A, C, HC, LL, ReqBody> crate::PoolMemberMaker<A> for HTTP2ConnectionMaker<C, HC, LL, ReqBody>
where
    A: Send + Sync + 'static,
    C: Connector<A> + Send + Sync + 'static,
    C::IO: Send,
    C::Error: Send + 'static,
    HC: crate::HealthChecker<ReqBody> + Send + Sync + 'static,
    ReqBody: Body + Send + Unpin + 'static,
    ReqBody::Data: Send,
    ReqBody::Error: std::error::Error + Send + Sync,
    HC::Error: std::error::Error + Clone + Send,
    LL: tower::Layer<HTTP2Connection<A, C, HC, ReqBody>>,
    LL::Service: Service<http::Request<ReqBody>>,
{
    type ReqBody = ReqBody;
    type Connection = LL::Service;

    fn make_connection(
        &self,
        pool: Arc<PoolCommon>,
        reporter: Reporter<SubchannelShared>,
        address: A,
    ) -> Self::Connection {
        self.ll.layer(HTTP2Connection::new(
            Arc::clone(&self.shared),
            pool,
            reporter,
            address,
        ))
    }
}

#[cfg(test)]
mod tests {
    use async_stream::stream;
    use futures::Stream;
    use std::borrow::Borrow;
    use std::collections::HashMap;
    use std::hash::Hash;
    use tower::ServiceExt;
    use tower::layer::util::Identity;

    use super::*;
    use crate::PoolMemberMaker;
    use crate::pool::PoolCommonTestInterface;
    use crate::report::{Inventory, InventoryReport};
    use crate::testutil::{TestServer, TestServerAddress};
    use crate::util::AssumeAlwaysHealthy;

    #[derive(Debug)]
    struct TestHealthCheck(Arc<tokio::sync::Semaphore>);

    impl TestHealthCheck {
        fn new() -> (Self, Arc<tokio::sync::Semaphore>) {
            let sem1 = Arc::new(tokio::sync::Semaphore::new(1));
            let sem2 = Arc::clone(&sem1);
            (Self(sem1), sem2)
        }
    }

    #[derive(Debug, Clone)]
    struct TestUnhealthy;

    impl std::fmt::Display for TestUnhealthy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "mock unhealthy")
        }
    }

    impl std::error::Error for TestUnhealthy {}

    impl crate::HealthChecker<String> for TestHealthCheck {
        type Error = TestUnhealthy;

        fn watch<S, RespBody>(
            &self,
            mut svc: S,
        ) -> impl Stream<Item = Result<(), Self::Error>> + Send
        where
            S: Service<http::Request<String>, Response = http::Response<RespBody>> + Send,
            S::Error: std::fmt::Debug + Send,
            S::Future: Send,
            RespBody: Send,
        {
            stream! {
                loop {
                    self.0.acquire().await.unwrap().forget();
                    let req = http::Request::builder()
                        .uri("http://nowhere/healthz")
                        .body(String::new())
                        .expect("trivial test request");
                    let resp = svc
                        .ready()
                        .await
                        .expect("ready")
                        .call(req)
                        .await;
                    yield match resp {
                        Ok(r) => if r.status() == 200 { Ok(()) } else { Err(TestUnhealthy) },
                        Err(_) => Err(TestUnhealthy),
                    };
                }
            }
        }
    }

    #[derive(Debug, Default)]
    struct SubchannelCached {
        flags: crate::shared::FetchedSubchannelFlags,
        attention_count: usize,
        deleted: Option<Result<(), Box<LoggedEvent<String>>>>,
    }

    struct TestSetup<D> {
        maker: HTTP2ConnectionMaker<TestServer, TestHealthCheck, Identity, String>,
        server: TestServer,
        probe_again: Arc<tokio::sync::Semaphore>,
        reports: HashMap<D, SubchannelCached>,
    }

    impl<D: Eq + Hash + Clone> TestSetup<D> {
        fn new() -> Self {
            let server = TestServer::new();
            let (hc, probe_again) = TestHealthCheck::new();
            let maker = HTTP2ConnectionMaker::new(server.clone(), hc, Identity::new());
            Self {
                maker,
                server,
                probe_again,
                reports: HashMap::new(),
            }
        }

        async fn collect_report(&mut self, inv: &mut Inventory<D, SubchannelShared>) {
            match inv.recv().await {
                InventoryReport::Dropped(d, shared) => {
                    self.reports.entry(d).and_modify(|c| {
                        c.deleted = Some(match shared.error.take(Ordering::AcqRel) {
                            None => Ok(()),
                            Some(e) => Err(e),
                        })
                    });
                }
                InventoryReport::Attention(d, shared) => {
                    let r = self.reports.entry(d.clone()).or_default();
                    r.attention_count += 1;
                    r.flags = shared.flags.get();
                }
            }
        }

        fn get_report<Q>(&self, d: &Q) -> Option<&SubchannelCached>
        where
            D: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            self.reports.get(d)
        }

        async fn expect_connected<Q>(&mut self, inv: &mut Inventory<D, SubchannelShared>, name: &Q)
        where
            D: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            while !self
                .get_report(name)
                .map(|report| report.flags.connected())
                .unwrap_or_default()
            {
                self.collect_report(inv).await;
            }
        }

        async fn expect_health_report<Q>(
            &mut self,
            inv: &mut Inventory<D, SubchannelShared>,
            name: &Q,
            want: bool,
        ) where
            D: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            while self.get_report(name).map(|report| report.flags.healthy()) != Some(Some(want)) {
                self.collect_report(inv).await;
            }
        }

        async fn expect_no_subchannel<Q>(
            &mut self,
            inv: &mut Inventory<D, SubchannelShared>,
            name: &Q,
            want_error: bool,
        ) where
            D: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            loop {
                match self.get_report(name) {
                    None => {
                        if !want_error {
                            return;
                        }
                    }
                    Some(s) => match s.deleted {
                        None => (),
                        Some(Ok(())) => {
                            if want_error {
                                panic!("Expected an errored connection, got a successful one");
                            }
                            return;
                        }
                        Some(Err(ref e)) => {
                            if !want_error {
                                panic!("Expected a successful connection, got {e:?}");
                            }
                            return;
                        }
                    },
                }
                self.collect_report(inv).await;
            }
        }
    }

    #[tokio::test]
    async fn simple_success() {
        let mut t = TestSetup::new();
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let req = http::Request::builder()
            .uri("http://nowhere/success")
            .body(String::new())
            .expect("trivial test request");
        let mut c =
            t.maker
                .make_connection(pif.pool(), inv.allocate("foo"), TestServerAddress::Working);
        t.expect_connected(&mut inv, "foo").await;
        t.expect_health_report(&mut inv, "foo", true).await;
        let resp = c
            .ready()
            .await
            .expect("ready")
            .call(req)
            .await
            .expect("successful request");
        assert_eq!(resp.status(), 200);
        std::mem::drop(c);
        t.expect_no_subchannel(&mut inv, "foo", false).await;
    }

    #[tokio::test]
    async fn simple_unhealthy() {
        let mut t = TestSetup::new();
        t.server.set_healthy(false);
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let mut c =
            t.maker
                .make_connection(pif.pool(), inv.allocate("foo"), TestServerAddress::Working);
        t.expect_connected(&mut inv, "foo").await;
        t.expect_health_report(&mut inv, "foo", false).await;
        assert_matches!(c.ready().await, Err(HTTP2ConnectionError::Unhealthy(_)));
        // Connections don't drop themselves for being unhealthy.
        t.server.set_healthy(true);
        t.probe_again.add_permits(1);
        t.expect_health_report(&mut inv, "foo", true).await;
        // But the tower Service contract says you should drop connections
        // that poll_ready with Err.
        std::mem::drop(c);
        t.expect_no_subchannel(&mut inv, "foo", true).await;
    }

    #[tokio::test]
    async fn failure_to_connect() {
        let mut t = TestSetup::new();
        t.server.set_healthy(false);
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let mut c = t.maker.make_connection(
            pif.pool(),
            inv.allocate("foo"),
            TestServerAddress::FailToConnect,
        );
        assert_matches!(
            c.ready().await,
            Err(HTTP2ConnectionError::ConnectorError(_))
        );
        // The connecton gets dropped even if nobody polls it ready
        // because this status is unrecoverable and the pool will want
        // to replace it with a new connection as soon as possible.
        t.expect_no_subchannel(&mut inv, "foo", false).await;
    }

    #[tokio::test]
    async fn dropped_while_connecting() {
        let mut t = TestSetup::new();
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let _ = t.maker.make_connection(
            pif.pool(),
            inv.allocate("foo"),
            TestServerAddress::HangsOnConnect,
        );
        t.expect_no_subchannel(&mut inv, "foo", false).await;
    }

    #[tokio::test]
    async fn health_changes() {
        let mut t = TestSetup::new();
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let c =
            t.maker
                .make_connection(pif.pool(), inv.allocate("foo"), TestServerAddress::Working);
        t.expect_connected(&mut inv, "foo").await;
        t.expect_health_report(&mut inv, "foo", true).await;
        t.server.set_healthy(false);
        t.probe_again.add_permits(1);
        t.expect_health_report(&mut inv, "foo", false).await;
        let foocount = t
            .get_report("foo")
            .expect("have subchannel foo")
            .attention_count;
        t.probe_again.add_permits(1);
        // No notification if the health is still false.
        t.server.set_healthy(true);
        t.probe_again.add_permits(1);
        t.expect_health_report(&mut inv, "foo", true).await;
        assert_eq!(
            t.get_report("foo")
                .expect("have subchannel foo")
                .attention_count,
            foocount + 1
        );
        t.probe_again.add_permits(1);
        // No notification if the health is still false.
        std::mem::drop(c);
        t.expect_no_subchannel(&mut inv, "foo", true).await;
        assert_eq!(
            t.get_report("foo")
                .expect("have subchannel foo")
                .attention_count,
            foocount + 1
        );
    }

    #[tokio::test]
    async fn critically_unhealthy() {
        let mut t = TestSetup::new();
        t.server.set_healthy(false);
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let mut c =
            t.maker
                .make_connection(pif.pool(), inv.allocate("foo"), TestServerAddress::Working);
        t.expect_connected(&mut inv, "foo").await;
        t.expect_health_report(&mut inv, "foo", false).await;
        pif.set_critically_unhealthy(true);
        // Unhealthy but should accept requests anyway.
        assert!(c.ready().await.is_ok());
        pif.set_critically_unhealthy(false);
        // But not anymore.
        assert!(c.ready().await.is_err());
    }

    #[tokio::test]
    async fn broken_stream() {
        let server = TestServer::new();
        let maker =
            HTTP2ConnectionMaker::new(server, AssumeAlwaysHealthy::default(), Identity::new());
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let mut c: HTTP2Connection<_, _, _, String> = maker.make_connection(
            pif.pool(),
            inv.allocate("foo"),
            TestServerAddress::BrokenStream,
        );
        let resp = c.ready().await;
        assert_matches!(resp, Err(HTTP2ConnectionError::HyperError(_)));
    }
}
