use futures::future::Either;
use futures::{select, FutureExt, StreamExt, TryFutureExt};
use hyper::body::Body;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use pin_project_lite::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use thiserror::Error;
use tower_service::Service;

use crate::pool::PoolCommon;
use crate::report::Reporter;
use crate::Connector;

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
}

#[derive(Debug)]
struct HTTP2ConnectionHealth<E> {
    healthy: AtomicBool,
    error: std::sync::Mutex<Option<E>>,
}

impl<E: Clone> HTTP2ConnectionHealth<E> {
    fn new() -> Self {
        Self {
            healthy: AtomicBool::default(),
            error: std::sync::Mutex::new(None),
        }
    }

    fn set_healthy(&self) {
        self.healthy.store(true, Ordering::Release);
    }

    fn set_unhealthy(&self, e: E) {
        *self.error.lock().unwrap() = Some(e);
        self.healthy.store(false, Ordering::Release);
    }

    fn get(&self) -> HealthState<E> {
        match self.healthy.load(Ordering::Acquire) {
            true => HealthState::Healthy,
            false => HealthState::Unhealthy(self.error.lock().unwrap().clone().unwrap()),
        }
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
    type Output = Result<(SendRequest<ReqBody>, want::Taker), HTTP2ConnectionError<CE, HCE>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();
        // Wait for the first health check.
        Poll::Ready(match ready!(this.ready_notify.poll(cx)) {
            Err(_) => Err(HTTP2ConnectionError::ConnectionTaskLost),
            Ok(Ok(sender)) => Ok((sender, this.alive_sentinel.take().unwrap())),
            Ok(Err(e)) => Err(e),
        })
    }
}

fn monitor_connection<A, C, HC, ReqBody, F>(
    address: A,
    shared: Arc<HTTP2ConnectionShared<C, HC>>,
    health: Arc<HTTP2ConnectionHealth<HC::Error>>,
    reporter_fut: F,
) -> ConnectingFuture<ReqBody, C::Error, HC::Error>
where
    A: Send + 'static,
    C: Connector<A> + Send + Sync + 'static,
    C::IO: Send + 'static,
    C::Error: Send + 'static,
    HC: crate::HealthChecker<ReqBody> + Send + Sync + 'static,
    HC::Error: std::error::Error + Clone + Send,
    F: Future<Output = Reporter<bool>> + Send + 'static,
    ReqBody: Body + Send + Unpin + 'static,
    ReqBody::Data: Send,
    ReqBody::Error: std::error::Error + Send + Sync + 'static,
{
    let (mut giver, taker) = want::new();
    let (sender_tx, sender_rx) = tokio::sync::oneshot::channel();
    tokio::task::spawn(async move {
        // Do this as soon as possible because anything that causes us to
        // return (or panic) before this is successful means reporter won't
        // be dropped and this connection will leak.
        let mut reporter = reporter_fut.await;

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
                let _ = sender_tx.send(Err(e));
                return;
            }
        };
        let mut begin_usage = Some((sender.clone(), sender_tx));
        let mut healthy = None;
        let mut hc_stream = pin!(shared
            .health_checker
            .watch(SendRequestService(sender))
            .fuse());
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
                                health.set_healthy();
                                reporter.send(true).await;
                            }
                        }
                        Some(Err(e)) => {
                            if healthy.unwrap_or(true) {
                                healthy = Some(false);
                                health.set_unhealthy(e);
                                reporter.send(false).await;
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
enum HTTP2ConnectionState<ReqBody, CE: std::error::Error, HCE: std::error::Error> {
    Connecting(Pin<Box<ConnectingFuture<ReqBody, CE, HCE>>>),
    Connected(SendRequest<ReqBody>, #[allow(dead_code)] want::Taker),
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
    health: Arc<HTTP2ConnectionHealth<HC::Error>>,
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
    fn new<F>(
        shared: Arc<HTTP2ConnectionShared<C, HC>>,
        pool: Arc<PoolCommon>,
        reporter_fut: F,
        address: A,
    ) -> Self
    where
        F: Future<Output = Reporter<bool>> + Send + 'static,
    {
        let health = Arc::new(HTTP2ConnectionHealth::new());
        let monitor_health = Arc::clone(&health);
        // Box::pin is necessary so we can poll it from a Service.
        let fut = Box::pin(monitor_connection::<_, C, HC, ReqBody, _>(
            address,
            shared,
            monitor_health,
            reporter_fut,
        ));
        Self {
            pool,
            health,
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
    fn poll_connect(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), HTTP2ConnectionError<C::Error, HC::Error>>> {
        match self.state {
            HTTP2ConnectionState::Connecting(ref mut fut) => {
                let (sender, alive_sentinel) = ready!(fut.as_mut().poll(cx))?;
                self.state = HTTP2ConnectionState::Connected(sender, alive_sentinel);
                self.poll_connect(cx)
            }
            HTTP2ConnectionState::Connected(ref mut sender, _) => {
                match self.health.get() {
                    HealthState::Healthy => sender.poll_ready(cx).map_err(Into::into),
                    HealthState::Unhealthy(e) => {
                        if self.pool.is_critically_unhealthy() {
                            // Emergency healthy
                            sender.poll_ready(cx).map_err(Into::into)
                        } else {
                            Poll::Ready(Err(HTTP2ConnectionError::Unhealthy(e)))
                        }
                    }
                }
            }
        }
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
        let HTTP2ConnectionState::Connected(ref mut sender, _) = self.state else {
            panic!("HTTP2Connection not ready");
        };
        Box::pin(sender.send_request(req).err_into())
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

    fn make_connection<F>(
        &self,
        pool: Arc<PoolCommon>,
        reporter_fut: F,
        address: A,
    ) -> Self::Connection
    where
        F: Future<Output = Reporter<bool>> + Send + 'static,
    {
        self.ll.layer(HTTP2Connection::new(
            Arc::clone(&self.shared),
            pool,
            reporter_fut,
            address,
        ))
    }
}

#[cfg(test)]
mod tests {
    use async_stream::stream;
    use futures::Stream;
    use tower::layer::util::Identity;
    use tower::ServiceExt;

    use super::*;
    use crate::pool::PoolCommonTestInterface;
    use crate::report::{Inventory, InventoryReport};
    use crate::testutil::{TestServer, TestServerAddress};
    use crate::util::AssumeAlwaysHealthy;
    use crate::PoolMemberMaker;

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

    async fn expect_health_report<D>(inv: &mut Inventory<D, bool>, name: D, want: bool)
    where
        D: PartialEq + std::fmt::Debug,
    {
        let m = inv.recv().await;
        if let InventoryReport::Message(dr, got) = m {
            assert_eq!(*dr, name);
            assert_eq!(want, got);
        } else {
            panic!("got wrong message {:?}", m);
        }
    }

    struct TestSetup {
        maker: HTTP2ConnectionMaker<TestServer, TestHealthCheck, Identity, String>,
        server: TestServer,
        probe_again: Arc<tokio::sync::Semaphore>,
    }

    impl TestSetup {
        fn new() -> Self {
            let server = TestServer::new();
            let (hc, probe_again) = TestHealthCheck::new();
            let maker = HTTP2ConnectionMaker::new(server.clone(), hc, Identity::new());
            Self {
                maker,
                server,
                probe_again,
            }
        }
    }

    #[tokio::test]
    async fn simple_success() {
        let t = TestSetup::new();
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let req = http::Request::builder()
            .uri("http://nowhere/success")
            .body(String::new())
            .expect("trivial test request");
        let mut c =
            t.maker
                .make_connection(pif.pool(), inv.allocate("foo"), TestServerAddress::Working);
        expect_health_report(&mut inv, "foo", true).await;
        let resp = c
            .ready()
            .await
            .expect("ready")
            .call(req)
            .await
            .expect("successful request");
        assert_eq!(resp.status(), 200);
        std::mem::drop(c);
        assert_eq!(inv.recv().await, InventoryReport::Dropped("foo"));
    }

    #[tokio::test]
    async fn simple_unhealthy() {
        let t = TestSetup::new();
        t.server.set_healthy(false);
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let mut c =
            t.maker
                .make_connection(pif.pool(), inv.allocate("foo"), TestServerAddress::Working);
        expect_health_report(&mut inv, "foo", false).await;
        assert_matches!(c.ready().await, Err(HTTP2ConnectionError::Unhealthy(_)));
        // Connections don't drop themselves for being unhealthy.
        t.server.set_healthy(true);
        t.probe_again.add_permits(1);
        expect_health_report(&mut inv, "foo", true).await;
        // But the tower Service contract says you should drop connections
        // that poll_ready with Err.
        std::mem::drop(c);
        assert_eq!(inv.recv().await, InventoryReport::Dropped("foo"));
    }

    #[tokio::test]
    async fn failure_to_connect() {
        let t = TestSetup::new();
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
        assert_eq!(inv.recv().await, InventoryReport::Dropped("foo"));
    }

    #[tokio::test]
    async fn dropped_while_connecting() {
        let t = TestSetup::new();
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let _ = t.maker.make_connection(
            pif.pool(),
            inv.allocate("foo"),
            TestServerAddress::HangsOnConnect,
        );
        assert_eq!(inv.recv().await, InventoryReport::Dropped("foo"));
    }

    #[tokio::test]
    async fn health_changes() {
        let t = TestSetup::new();
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let c =
            t.maker
                .make_connection(pif.pool(), inv.allocate("foo"), TestServerAddress::Working);
        expect_health_report(&mut inv, "foo", true).await;
        t.server.set_healthy(false);
        t.probe_again.add_permits(1);
        expect_health_report(&mut inv, "foo", false).await;
        t.probe_again.add_permits(1);
        // No notification if the health is still false.
        t.server.set_healthy(true);
        t.probe_again.add_permits(1);
        expect_health_report(&mut inv, "foo", true).await;
        t.probe_again.add_permits(1);
        // No notification if the health is still false.
        std::mem::drop(c);
        assert_eq!(inv.recv().await, InventoryReport::Dropped("foo"));
    }

    #[tokio::test]
    async fn critically_unhealthy() {
        let t = TestSetup::new();
        t.server.set_healthy(false);
        let pif = PoolCommonTestInterface::new();
        let mut inv = Inventory::new(1);

        let mut c =
            t.maker
                .make_connection(pif.pool(), inv.allocate("foo"), TestServerAddress::Working);
        expect_health_report(&mut inv, "foo", false).await;
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
