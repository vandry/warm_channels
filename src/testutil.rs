use futures::future::Either;
use http::StatusCode;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::DuplexStream;

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
