//! Always-ready load-balanced gRPC client channel.
//!
//! This module provides a client channel adapted for gRPC usage with [`tonic`].
//!
//! ```
//! use std::sync::Arc;
//! use hickory_resolver::TokioResolver;
//!
//! # tokio_test::block_on(async {
//! let r = Arc::new(TokioResolver::builder_tokio().unwrap().build());
//! let uri = "https://example.org".try_into().unwrap();
//! let stream = warm_channels::resolve_uri(&uri, r).unwrap();
//! let (stack, worker) = warm_channels::grpc_channel(
//!     uri.clone(),
//!     warm_channels::grpc::GRPCChannelConfig::default(),
//!     "demo",
//!     warm_channels::stream::TCPConnector::default(),
//!     stream,
//!     |h| println!("healthy: {}", h),
//! );
//! tokio::task::spawn(worker);
//! # // Fake for demo
//! # mod pb { pub mod test_client {
//! #   pub struct TestClient;
//! #   impl TestClient {
//! #     pub fn with_origin<T, U>(t: T, u: U) -> Self { Self }
//! #     pub async fn greet<T>(self, t: T) {}
//! #   }
//! # } }
//! let client = pb::test_client::TestClient::with_origin(stack, uri);
//!
//! println!("{:?}", client.greet(tonic::Request::new(())).await);
//! # });
//! ```

use async_stream::stream;
use futures::Stream;
use futures::future::Either;
use http::Uri;
use hyper::body::Body;
use std::borrow::Cow;
use std::future::Future;
use std::pin::pin;
use std::time::Duration;
use thiserror::Error;
use tonic::body::BoxBody;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tower_service::Service;

use crate::Connector;

/// Configuration for [`GRPCHealthChecker`]
#[derive(Clone, Debug)]
pub struct GRPCHealthCheckConfig {
    /// Service name to query for health checks.
    ///
    /// This should elicit a response on the server for a "readiness"-type
    /// health check, not a "liveness"-type one. A "readiness"-type health
    /// check is one which the server answers with whether or not it
    /// recommends that clients send requests to it (as opposed to sending
    /// requests to one of its neigbouring tasks).
    ///
    /// If None, there will be no health checks and the channe
    ///
    /// Default: "ready"
    pub service: Option<Cow<'static, str>>,

    /// Deadline for each health check probe.
    ///
    /// Default: 7.5s
    pub timeout: Duration,

    /// Amount of time after one health check probe finishes until the next.
    ///
    /// Default: 9s
    pub interval: Duration,
}

impl Default for GRPCHealthCheckConfig {
    fn default() -> Self {
        Self {
            service: Some("ready".into()),
            timeout: Duration::from_millis(7500),
            interval: Duration::from_millis(9000),
        }
    }
}

/// Configuration parameters for [`grpc_channel`]
#[derive(Clone, Debug, Default)]
pub struct GRPCChannelConfig {
    /// Channnel parameters like the number of subchannels and healthiness thresholds
    pub pool: crate::pool::PoolConfig,
    /// gRPC-specific health checking parameters
    pub health_checking: GRPCHealthCheckConfig,
}

/// Error type returned for an unsuccessful health check
#[derive(Clone, Debug, Error)]
pub enum GRPCHealthCheckError {
    /// No response was received within the deadline specified in [`GRPCHealthCheckConfig`].
    #[error("no response to health check after {}s", .0.as_secs_f64())]
    DeadlineExceeded(Duration),
    /// The server reported a status other than [`ServingStatus::Serving`].
    #[error("health check serving status is {}", .0.as_str_name())]
    NotServing(ServingStatus),
    /// The server reported an undecodable [`ServingStatus`].
    #[error("health check serving status invalid")]
    ServingStatusInvalid,
    /// An RPC error occurred with the health check.
    #[error("health check: {0}")]
    RPCError(tonic::Status),
}

async fn single_hc<S>(
    client: &mut HealthClient<S>,
    c: &GRPCHealthCheckConfig,
    service: &str,
) -> Result<(), GRPCHealthCheckError>
where
    S: tonic::client::GrpcService<BoxBody>,
    S::Error: std::error::Error + Sync,
    S::ResponseBody: Body<Data = hyper::body::Bytes> + Send + 'static,
    <S::ResponseBody as Body>::Error: std::error::Error + Send + Sync,
{
    let deadline = pin!(tokio::time::sleep(c.timeout));
    let hc = pin!(client.check(HealthCheckRequest {
        service: (*service).to_owned()
    }));
    match futures::future::select(hc, deadline).await {
        Either::Left((Err(e), _)) => Err(GRPCHealthCheckError::RPCError(e)),
        Either::Left((Ok(r), _)) => match ServingStatus::try_from(r.into_inner().status) {
            Ok(ServingStatus::Serving) => Ok(()),
            Ok(st) => Err(GRPCHealthCheckError::NotServing(st)),
            Err(_) => Err(GRPCHealthCheckError::ServingStatusInvalid),
        },
        Either::Right(_) => Err(GRPCHealthCheckError::DeadlineExceeded(c.timeout)),
    }
}

/// A health checker plugin for gRPC clients. Uses the standard
/// [gRPC health check protocol](https://grpc.io/docs/guides/health-checking/).
/// Implements [`crate::HealthChecker`].
#[derive(Debug)]
pub struct GRPCHealthChecker {
    uri: Uri,
    config: GRPCHealthCheckConfig,
}

impl GRPCHealthChecker {
    /// Create a new gRPC health checker with the given configuration.
    /// The URI is necessary only for its origin, to populate HTTP headers.
    pub fn new(uri: Uri, config: GRPCHealthCheckConfig) -> Self {
        Self { uri, config }
    }
}

impl crate::HealthChecker<BoxBody> for GRPCHealthChecker {
    type Error = GRPCHealthCheckError;

    fn watch<S, RespBody>(&self, svc: S) -> impl Stream<Item = Result<(), Self::Error>>
    where
        S: Service<http::Request<BoxBody>, Response = http::Response<RespBody>> + Send,
        S::Error: std::error::Error + Send + Sync + 'static,
        S::Future: Send,
        RespBody: Body<Data = hyper::body::Bytes> + Send + 'static,
        RespBody::Error: std::error::Error + Send + Sync,
    {
        let mut client = HealthClient::with_origin(svc, self.uri.clone());
        stream! {
            let Some(ref service) = self.config.service else {
                yield Ok(());
                return;
            };
            loop {
                yield single_hc(&mut client, &self.config, service).await;
                tokio::time::sleep(self.config.interval).await;
            }
        }
    }
}

/// The type of the channel returned by [`grpc_channel`]. This implements
/// [`tower_service::Service`] with [`BoxBody`] as the HTTP request body
/// as required for wrapping a [`tonic`] gRPC client arount it.
#[cfg(feature = "metrics")]
pub type GRPCChannel<A, C, HC = GRPCHealthChecker> = crate::channel::Channel<
    crate::grpc_metrics::MetricsChannel<crate::channel::PoolService<A, BoxBody, C, HC>>,
    BoxBody,
>;

#[cfg(not(feature = "metrics"))]
pub type GRPCChannel<A, C, HC = GRPCHealthChecker> =
    crate::channel::Channel<crate::channel::PoolService<A, BoxBody, C, HC>, BoxBody>;

/// Create a new always-ready load-balanced gRPC client channel.
///
/// For all of the arguments, see [`crate::channel::pool_service`].
///
/// Returns the channel itself (which is a [`tower_service::Service`] and
/// therefore can accept requests) plus a pool worker. The worker must be
/// spawned as a task on an executor (e.g. using [`tokio::task::spawn`])
/// in order for the channel to work.
pub fn grpc_channel<A, RS, RE, C, HR, L>(
    uri: Uri,
    config: GRPCChannelConfig,
    label: L,
    connector: C,
    resolution_stream: RS,
    healthy_callback: HR,
) -> (GRPCChannel<A, C>, impl Future<Output = ()>)
where
    A: std::hash::Hash + Send + Sync + std::fmt::Debug + Eq + Clone + 'static,
    RS: Stream<Item = Result<Vec<A>, RE>> + Send + 'static,
    RE: std::error::Error + Send + 'static,
    C: Connector<A> + Send + Sync + 'static,
    C::IO: Send,
    C::Error: Send + Sync + 'static,
    HR: Fn(bool) + Send + 'static,
    L: AsRef<str> + Send + 'static,
{
    let health_checker = GRPCHealthChecker::new(uri, config.health_checking);
    let (stack, worker) = crate::channel::pool_service(
        config.pool,
        label,
        connector,
        resolution_stream,
        health_checker,
        healthy_callback,
    );
    #[cfg(feature = "metrics")]
    let stack = crate::grpc_metrics::MetricsChannel::new(stack);
    crate::channel::Channel::new(stack, worker)
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, poll};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::task::Poll;
    use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

    use super::*;
    use crate::HealthChecker;
    use crate::testutil::{TestServer, TestServerAddress};

    struct TestHealth {
        count: AtomicU64,
    }

    #[tonic::async_trait]
    impl tonic_health::pb::health_server::Health for TestHealth {
        type WatchStream = futures::stream::Empty<Result<HealthCheckResponse, tonic::Status>>;

        async fn check(
            &self,
            req: tonic::Request<HealthCheckRequest>,
        ) -> Result<tonic::Response<HealthCheckResponse>, tonic::Status> {
            assert_eq!(req.into_inner().service, "ready");
            let old = self.count.fetch_add(1, Ordering::SeqCst);
            match old {
                0 => Ok(tonic::Response::new(HealthCheckResponse {
                    status: ServingStatus::Serving.into(),
                })),
                1 => Ok(tonic::Response::new(HealthCheckResponse {
                    status: ServingStatus::NotServing.into(),
                })),
                2 => Ok(tonic::Response::new(HealthCheckResponse {
                    status: i32::MAX,
                })),
                3 => {
                    std::future::pending::<()>().await;
                    unreachable!();
                }
                _ => Err(tonic::Status::out_of_range("nope")),
            }
        }

        async fn watch(
            &self,
            _: tonic::Request<HealthCheckRequest>,
        ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
            todo!();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn grpc_health_check() {
        let config = GRPCHealthCheckConfig::default();
        let hc = GRPCHealthChecker::new("http://foo/".try_into().unwrap(), config.clone());
        let mut stream = pin!(hc.watch(tonic_health::pb::health_server::HealthServer::new(
            TestHealth {
                count: AtomicU64::new(0)
            }
        ),));
        assert_matches!(poll!(stream.next()), Poll::Ready(Some(Ok(()))));
        assert_matches!(poll!(stream.next()), Poll::Pending);

        tokio::time::advance(config.interval).await;
        assert_matches!(
            poll!(stream.next()),
            Poll::Ready(Some(Err(GRPCHealthCheckError::NotServing(
                ServingStatus::NotServing
            ))))
        );
        assert_matches!(poll!(stream.next()), Poll::Pending);

        tokio::time::advance(config.interval).await;
        assert_matches!(
            poll!(stream.next()),
            Poll::Ready(Some(Err(GRPCHealthCheckError::ServingStatusInvalid)))
        );
        assert_matches!(poll!(stream.next()), Poll::Pending);

        tokio::time::advance(config.interval).await;
        assert_matches!(poll!(stream.next()), Poll::Pending);
        tokio::time::advance(config.timeout).await;
        assert_matches!(
            poll!(stream.next()),
            Poll::Ready(Some(Err(GRPCHealthCheckError::DeadlineExceeded(_))))
        );
        assert_matches!(poll!(stream.next()), Poll::Pending);

        tokio::time::advance(config.interval).await;
        assert_matches!(
            poll!(stream.next()),
            Poll::Ready(Some(Err(GRPCHealthCheckError::RPCError(_))))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn grpc_no_health_check() {
        let mut config = GRPCHealthCheckConfig::default();
        config.service = None;
        let hc = GRPCHealthChecker::new("http://foo/".try_into().unwrap(), config.clone());
        let mut stream = pin!(hc.watch(tonic_health::pb::health_server::HealthServer::new(
            TestHealth {
                count: AtomicU64::new(4)
            }
        ),));
        assert_matches!(poll!(stream.next()), Poll::Ready(Some(Ok(()))));
        assert_matches!(poll!(stream.next()), Poll::Ready(None));
    }

    #[tokio::test]
    async fn build_channnel() {
        let rs = futures::stream::once(futures::future::ready(Ok::<_, std::convert::Infallible>(
            vec![TestServerAddress::Working],
        )));
        let _ = grpc_channel(
            "http://foo/".try_into().unwrap(),
            GRPCChannelConfig::default(),
            "test",
            TestServer::new(),
            rs,
            |_| (),
        );
    }

    #[cfg(feature = "metrics")]
    struct TestGrpcServer;

    #[cfg(feature = "metrics")]
    impl crate::Connector<()> for TestGrpcServer {
        type IO = tokio::io::DuplexStream;
        type Error = std::io::Error;

        fn connect(
            &self,
            _: (),
        ) -> impl Future<Output = std::io::Result<Self::IO>> + Send + Sync + 'static {
            let (s1, s2) = tokio::io::duplex(1000);
            let stream =
                futures::stream::once(std::future::ready(Ok::<_, std::convert::Infallible>(s2)));
            let (mut r, service) = tonic_health::server::health_reporter();
            tokio::task::spawn(
                tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming(stream),
            );
            async move {
                r.set_service_status("ready", tonic_health::ServingStatus::Serving)
                    .await;
                Ok(s1)
            }
        }
    }

    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn metrics() {
        use futures::pin_mut;
        use prometheus::Encoder;

        let rs = futures::stream::once(futures::future::ready(Ok::<_, std::convert::Infallible>(
            vec![()],
        )));
        let uri: Uri = "http://foo/".try_into().unwrap();
        let (stack, worker) = grpc_channel(
            uri.clone(),
            GRPCChannelConfig::default(),
            "test",
            TestGrpcServer,
            rs,
            |_| (),
        );
        let mut client = HealthClient::with_origin(stack, uri);
        pin_mut!(worker);
        let hc = pin!(client.check(HealthCheckRequest {
            service: String::new(),
        }));
        match futures::future::select(hc, worker).await {
            Either::Left((Ok(_), _)) => (),
            _ => {
                panic!("expected success");
            }
        }

        let encoder = prometheus::TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();

        assert!(
            String::from_utf8_lossy(&buffer).contains("grpc_service=\"grpc.health.v1.Health\"")
        );
    }
}
