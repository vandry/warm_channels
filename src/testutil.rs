use futures::future::Either;
use http::StatusCode;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tokio::io::DuplexStream;
use tower_service::Service;

use crate::channel::PoolService;
use crate::util::AssumeAlwaysHealthy;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TestServerAddress {
    Working,
    FailToConnect,
    HangsOnConnect,
    BrokenStream,
}

#[derive(Clone, Debug)]
pub(crate) struct TestServer {
    healthy: Arc<AtomicBool>,
}

impl TestServer {
    pub(crate) fn new() -> Self {
        Self {
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    pub(crate) fn set_healthy(&self, h: bool) {
        self.healthy.store(h, Ordering::Release);
    }
}

impl crate::Connector<TestServerAddress> for TestServer {
    type IO = DuplexStream;
    type Error = std::io::Error;

    fn connect(
        &self,
        addr: TestServerAddress,
    ) -> impl Future<Output = std::io::Result<DuplexStream>> + Send + Sync + 'static {
        match addr {
            TestServerAddress::Working => {
                let (s1, s2) = tokio::io::duplex(1000);
                let healthy = Arc::clone(&self.healthy);
                tokio::task::spawn(
                    hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(
                            TokioIo::new(s2),
                            service_fn(move |r| {
                                let healthy = Arc::clone(&healthy);
                                async move {
                                    let mut resp = hyper::Response::new(String::new());
                                    *resp.status_mut() = match r.uri().path() {
                                        "/healthz" => {
                                            if healthy.load(Ordering::Acquire) {
                                                StatusCode::OK
                                            } else {
                                                StatusCode::INTERNAL_SERVER_ERROR
                                            }
                                        }
                                        "/success" => StatusCode::OK,
                                        _ => StatusCode::NOT_FOUND,
                                    };
                                    Ok::<_, std::convert::Infallible>(resp)
                                }
                            }),
                        ),
                );
                Either::Left(std::future::ready(Ok(s1)))
            }
            TestServerAddress::FailToConnect => Either::Left(std::future::ready(Err(
                std::io::Error::new(std::io::ErrorKind::ConnectionRefused, String::new()),
            ))),
            TestServerAddress::HangsOnConnect => Either::Right(std::future::pending()),
            TestServerAddress::BrokenStream => {
                let (s1, _) = tokio::io::duplex(1000);
                Either::Left(std::future::ready(Ok(s1)))
            }
        }
    }
}

type TestConnection =
    crate::connection::HTTP2Connection<TestServerAddress, TestServer, AssumeAlwaysHealthy, String>;

pub(crate) struct TestSubchannel {
    inner: TestConnection,
}

impl TestSubchannel {
    fn new(inner: TestConnection) -> Self {
        Self { inner }
    }
}

impl<R> Service<R> for TestSubchannel
where
    TestConnection: Service<R>,
{
    type Response = <TestConnection as Service<R>>::Response;
    type Error = <TestConnection as Service<R>>::Error;
    type Future = <TestConnection as Service<R>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        self.inner.call(req)
    }
}

impl tower::load::Load for TestSubchannel {
    type Metric = usize;

    fn load(&self) -> usize {
        1
    }
}

pub(crate) fn test_channel(
    name: &'static str,
) -> (PoolService<String, TestSubchannel>, impl Future) {
    let rs = futures::stream::once(futures::future::ready(Ok::<_, std::convert::Infallible>(
        vec![TestServerAddress::Working],
    )));
    crate::channel::pool_service_with_subchannel_layer(
        crate::pool::PoolConfig::default(),
        name,
        TestServer::new(),
        rs,
        AssumeAlwaysHealthy::default(),
        |_| (),
        tower::ServiceBuilder::new().layer_fn(|s| TestSubchannel::new(s)),
    )
}
