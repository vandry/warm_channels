//! Always-ready load-balanced HTTP client channel.
//!
//! This module provides a basic thin wrapper over the crate's lower level API
//! `crate::pool_service`. As the development focus has been on gRPC, this
//! module is less polished and should be considered subject to change.

use futures::Stream;
use std::future::Future;

use crate::util::AssumeAlwaysHealthy;
use crate::Connector;

/// The type of the channel returned by [`http_channel`]. This implements
/// [`tower_service::Service`] with `ReqBody` as the HTTP request body.
pub type HTTPChannel<A, C, ReqBody> = crate::channel::Channel<
    crate::channel::PoolService<A, ReqBody, C, AssumeAlwaysHealthy>,
    ReqBody,
>;

/// HTTP channel configuration.
///
/// There is currently no HTTP-specific configuration offered, so this contains
/// only the underlying pool configuration.
#[derive(Clone, Debug, Default)]
pub struct HTTPChannelConfig {
    /// Load-balanced channel configuration parameters.
    pub pool: crate::pool::PoolConfig,
}

/// Create a new always-ready load-balanced HTTP client channel.
///
/// For all of the arguments, see [`crate::channel::pool_service`].
///
/// Returns the channel itself (which is a [`tower_service::Service`] and
/// therefore can accept requests) plus a pool worker. The worker must be
/// spawned as a task on an executor (e.g. using [`tokio::task::spawn`])
/// in order for the channel to work.
///
/// Currently this just uses a no-op health checker that considers member
/// connections always healthy. For something more useful, consider using
/// the [`crate::channel::pool_service`] API directly.
pub fn http_channel<A, RS, RE, C, ReqBody, L>(
    config: HTTPChannelConfig,
    label: L,
    connector: C,
    resolution_stream: RS,
) -> (HTTPChannel<A, C, ReqBody>, impl Future<Output = ()>)
where
    A: std::hash::Hash + Send + Sync + std::fmt::Debug + Eq + Clone + 'static,
    RS: Stream<Item = Result<Vec<A>, RE>> + Send + 'static,
    RE: std::error::Error + Send + 'static,
    C: Connector<A> + Send + Sync + 'static,
    C::IO: Send,
    C::Error: Send + Sync + 'static,
    ReqBody: hyper::body::Body + Send + Unpin,
    ReqBody::Error: std::error::Error + Send + Sync,
    ReqBody::Data: Send,
    L: AsRef<str> + Send + 'static,
{
    let health_checker = AssumeAlwaysHealthy::default();
    let (stack, shim_worker) = crate::channel::pool_service(
        config.pool,
        label,
        connector,
        resolution_stream,
        health_checker,
        |_| (),
    );
    crate::channel::Channel::new(stack, shim_worker)
}

#[cfg(test)]
mod tests {
    use futures::future::Either;
    use futures::FutureExt;
    use std::pin::pin;
    use tower::ServiceExt;
    use tower_service::Service;

    use super::*;
    use crate::testutil::{TestServer, TestServerAddress};

    #[tokio::test]
    async fn end_to_end_success() {
        let rs = futures::stream::once(futures::future::ready(Ok::<_, std::convert::Infallible>(
            vec![TestServerAddress::Working],
        )));
        let (c, worker) = http_channel(HTTPChannelConfig::default(), "test", TestServer::new(), rs);
        let req = http::Request::builder()
            .uri("http://nowhere/success")
            .body(String::new())
            .expect("trivial test request");
        let mut c2 = c.clone();
        let fut = pin!(async move {
            c2.ready()
                .await
                .expect("ready")
                .call(req)
                .await
                .expect("successful request")
        });
        let worker = pin!(worker.fuse());
        let (resp, worker) = match futures::future::select(fut, worker).await {
            Either::Left(r) => r,
            Either::Right(_) => {
                panic!("lost worker");
            }
        };
        assert_eq!(resp.status(), 200);
        std::mem::drop(c);
        worker.await;
    }
}
