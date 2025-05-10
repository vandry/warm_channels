//! Always-ready HTTP client channels for gRPC or other RPC-like requests.
//!
//! In a microservices environment, components usually serve RPCs from their
//! clients in part by making further requests to backends of their own. For
//! example an application frontend serves a request by making a query to a
//! storage backend and a notification queue before applying business logic
//! and constructing a response to send to its own client, the end user.
//!
//! When RPC frontends and backends are both replicated as multiple tasks,
//! discipline in managing and load balancing the flow of requests is
//! important. This crate aims to offer the client side of that function.
//!
//! The main focus is on offering an always-ready gRPC client channel type
//! (in the [`grpc`] module) which load-balances over multiple individual
//! actively health-checked connections. A generic HTTP client channel typr
//! (in the [`http`] module) is also provided but it is currently less
//! polished.
//!
//! The building blocks revolve around [`tower`] (for the service stack) and
//! [`hyper`] (for HTTP), plus [`tokio`] and [`rustls`]. In particular, the
//! basic load balancing uses [`tower::balance::p2c`]. Finally, the gRPC
//! layer on top is designed to be used with [`tonic`].
//!
//! Some of the features brought by this crate are:
//!
//! - As soon as they are created, and continuing in the background for their
//! lifetime, channels begin attempting to constantly maintain a
//! sufficiently-sized pool of healthy member connections to backends.
//! - If multiple backend addresses are available, the channel will attempt
//! to use all of them, using different addresses for different member
//! connections, mitigating the effect of single backend tasks going away.
//! - Name resolution follows DNS TTLs, so that if the backend is using
//! DNS-based load balancing the channel notices and reacts when its
//! assignment changes.
//! - Member connections are individually health-checked and evicted from
//! the channel when they fail. Note that this is different to plain
//! [`tower::balance::p2c::Balance`], which only polls (and potentially
//! evicts) members on use.
//! - Channels that become critically unhealthy (too few healthy members are
//! healthy) are handled in a degraded mode: we temporarily make connected
//! but unhealthy members available to accept requests and stop evicting them.
//!
//! This crate can be used by itself but is designed to be used with
//! [`comprehensive`] which will further add the following features:
//!
//! - Easy macro-based declaration of a gRPC client made available as a
//! resource to the rest of the assembly.
//! - Automatically connected to the assembly-wide health status signal so
//! that when a client channel to a required backend is unhealthy then the
//! colocated server resources also report unhealthy.
//! - TLS configuration supplied from the assembly-wide TLS resource
//! (reflecting the use of a process-wide cryptographic identiry and
//! dynamically reloaded when changed (such as on certificate rotation).
//! - Configuration of the channel's backend URI and other configurable
//! properties using a standard set of flags.
//!
//! # gRPC client example using warm_channels directly
//!
//! ```
//! use std::sync::Arc;
//! use trust_dns_resolver::system_conf::read_system_conf;
//! use trust_dns_resolver::TokioAsyncResolver;
//!
//! # tokio_test::block_on(async {
//! let (resolver_config, mut resolver_opts) = read_system_conf().unwrap();
//! let r = Arc::new(TokioAsyncResolver::tokio(resolver_config, resolver_opts));
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
//!
//! # gRPC client example using [`comprehensive`]
//!
//! ```
//! use comprehensive_grpc::GrpcClient;
//! # // Fake for demo
//! # mod pb { pub mod test_client {
//! #   #[derive(Clone)] pub struct TestClient<T: Clone>(T);
//! #   impl<T: Clone> TestClient<T> {
//! #     pub fn with_origin<U>(t: T, u: U) -> Self { Self(t) }
//! #   }
//! # } }
//!
//! #[derive(GrpcClient)]
//! struct Client(
//!     pb::test_client::TestClient<comprehensive_grpc::client::Channel>,
//!     comprehensive_grpc::client::ClientWorker,
//! );
//! ```
//!
//! `Client` may then be included as a dependency in a Comprehensive Assembly.
//! See the full [gRPC hello world client example].
//!
//! # Possible future work:
//!
//! - Dynamically sized member set, probably based on reacting to request
//! processing latency.
//!
//! # Features
//!
//! - **grpc**: Enable gRPC functionality
//! - **tls**: Enable crypto functionality
//! - **metrics**: Export metrics about channel health and gRPC requests
//! - **unix**; Enable UNIX domain sockets connector.
//!
//! All are enabled by default.
//!
//! [`comprehensive`]: https://docs.rs/comprehensive/latest/comprehensive/
//! [`rustls`]: https://docs.rs/rustls/latest/rustls/
//! [`tonic`]: https://docs.rs/tonic/latest/tonic/
//! [gRPC hello world client example]: https://github.com/vandry/comprehensive/blob/master/examples/src/helloworld-grpc-client.rs

#![warn(missing_docs)]

#[cfg(test)]
#[macro_use]
extern crate assert_matches;

use std::future::Future;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tower_service::Service;

/// A constructor for the streaming byte IO channel over which load-balanced
/// member channels will be built.
///
/// This takes an generic network addresss type `A` (usually a
/// [`std::net::SocketAddr`]) and delivers a raw byte IO channnel connected
/// to a server at that address.
///
/// Users should usually use one of these implementations of the trait:
///
/// ```
/// // For plaintext only, can connect to TCP/IP addresses or UNIX sockets:
/// warm_channels::stream::StreamConnector::default();
/// // For plaintext or TLS, depending on the URI:
/// # #[cfg(feature = "tls")]
/// # let uri = "https://host".try_into().unwrap();
/// # use std::sync::Arc;
/// # #[cfg(feature = "tls")]
/// # let tls_config = tokio_rustls::rustls::client::ClientConfig::builder()
/// #     .with_root_certificates(tokio_rustls::rustls::RootCertStore::empty())
/// #     .with_no_client_auth();
/// # #[cfg(feature = "tls")]
/// warm_channels::tls::TLSConnector::new(
///     warm_channels::stream::StreamConnector::default(),
///     &uri, Some(&tls_config));
/// ```
pub trait Connector<A> {
    /// The type of the byte-based IO channel this Connector will produce
    /// on successful connection.
    type IO: AsyncRead + AsyncWrite + Unpin;
    /// The type of the error this Connector will produce on failure.
    type Error: std::error::Error;

    /// Attempt to asynchronously connect to an address and deliver an
    /// IO channel if successful.
    ///
    /// The connector should impose a deadline on connection if appropriate,
    /// otherwise the connection may wait indefinitely, taking up a slot for
    /// subchannels all the while.
    fn connect(
        &self,
        addr: A,
    ) -> impl Future<Output = Result<Self::IO, Self::Error>> + Send + Sync + 'static;
}

/// A health checking implementation for individual channels that are part of
/// a load-balanced set.
pub trait HealthChecker<ReqBody> {
    /// The type of error this health checker returns to indicate an unhealthy
    /// connection. The error might indicate that the actual health check
    /// failed, or it could be that a lower layer error occurred.
    type Error;

    /// Begin health checking a tower Service. This should return a neverending
    /// stream of health statuses unless the health checker somehow knows that
    /// the status will not change anymore.
    ///
    /// This will usually involve sending periodic health probes. If so, this
    /// method need not deliver an item on the stream after every probe, but
    /// only when the status changes.
    ///
    /// The stream returned by this method will be driven continuously from a
    /// spawned task until either the stream ends or the connection is dropped.
    fn watch<S, RespBody>(
        &self,
        svc: S,
    ) -> impl futures::Stream<Item = Result<(), Self::Error>> + Send
    where
        S: Service<::http::Request<ReqBody>, Response = ::http::Response<RespBody>> + Send,
        S::Error: std::error::Error + Send + Sync + 'static,
        S::Future: Send,
        RespBody: hyper::body::Body<Data = hyper::body::Bytes> + Send + 'static,
        RespBody::Error: std::error::Error + Send + Sync;
}

trait PoolMemberMaker<A> {
    type ReqBody;
    type Connection: Service<::http::Request<Self::ReqBody>>;

    fn make_connection<F>(
        &self,
        pool: Arc<pool::PoolCommon>,
        reporter_fut: F,
        address: A,
    ) -> Self::Connection
    where
        F: Future<Output = report::Reporter<bool>> + Send + 'static;
}

mod addresses;
mod balance_driver;
mod channel;
mod connection;
#[cfg(feature = "diag")]
mod diag;
#[cfg(any(feature = "tls", feature = "unix"))]
mod eitherio;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod http;
#[cfg(feature = "metrics")]
mod metrics;
mod pool;
mod report;
pub mod resolver;
#[cfg(feature = "diag")]
mod snapshot;
pub mod stream;
#[cfg(test)]
mod testutil;
#[cfg(feature = "tls")]
pub mod tls;
pub mod util;

pub use channel::{pool_service, Channel};
#[cfg(feature = "grpc")]
pub use grpc::grpc_channel;
pub use http::http_channel;
pub use pool::PoolConfig;
pub use resolver::resolve_uri;

#[cfg(feature = "diag")]
pub use diag::ChannelDiagService;
