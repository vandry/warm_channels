use futures::select;
use futures::{FutureExt, Stream};
use requests_gauge::{CompleteAfterBody, RequestsGaugeLayer, WithCompletionSubscriber};
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use tower::load::pending_requests::PendingRequests;
use tower_layer::Layer;
use tower_service::Service;

use crate::pool::{PoolConfig, balancer_pool};
use crate::subchannel::{Subchannel, SubchannelHandle};
use crate::{Connector, HealthChecker};

type ChannelInner<S, ReqBody> =
    tower::buffer::Buffer<http::Request<ReqBody>, <S as Service<http::Request<ReqBody>>>::Future>;

/// Top-level always-ready load-balanced channel type.
///
/// This implements [`Service`] for HTTP requests and can be used directly
/// or wrapped in a [`tonic`] gRPC client.
pub struct Channel<S: Service<http::Request<ReqBody>>, ReqBody: Send + 'static>(
    ChannelInner<S, ReqBody>,
);

impl<S, ReqBody: Send> Clone for Channel<S, ReqBody>
where
    S: Service<http::Request<ReqBody>>,
    S::Future: Send + 'static,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

async fn client_worker<BW, W>(buffer_worker: BW, worker: W)
where
    BW: Future + Send + 'static,
    W: Future + Send + 'static,
{
    let _ = tokio::spawn(async move {
        let mut buffer_worker = pin!(buffer_worker.fuse());
        let mut worker = pin!(worker.fuse());
        select! {
            _ = buffer_worker => (),
            _ = worker => (),
        }
    })
    .await;
}

pub struct CountInFlight<H: SubchannelHandle>(Weak<H>);

impl<R, H: SubchannelHandle> requests_gauge::RequestsGaugeSubscriber<R> for CountInFlight<H> {
    fn request_started(&mut self) {
        if let Some(h) = self.0.upgrade() {
            h.shared().requests_in_flight.inc();
        }
    }
}

impl<H: SubchannelHandle> Drop for CountInFlight<H> {
    fn drop(&mut self) {
        if let Some(h) = self.0.upgrade() {
            h.shared().requests_in_flight.dec();
        }
    }
}

pub struct SubchannelBase<T, H> {
    inner: T,
    handle: Weak<H>,
}

impl<T, R, H> Service<R> for SubchannelBase<T, H>
where
    T: Service<WithCompletionSubscriber<R, CountInFlight<H>>>,
    H: SubchannelHandle,
{
    type Response = T::Response;
    type Error = T::Error;
    type Future = T::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        self.inner.call(WithCompletionSubscriber::new(
            req,
            CountInFlight(self.handle.clone()),
        ))
    }
}

pub struct SubchannelMaker(RequestsGaugeLayer);

impl SubchannelMaker {
    pub(crate) fn new() -> Self {
        Self(RequestsGaugeLayer::new())
    }
}

impl<S: Subchannel> Layer<S> for SubchannelMaker {
    type Service = PendingRequests<
        SubchannelBase<<RequestsGaugeLayer as Layer<S>>::Service, <S as Subchannel>::Handle>,
        CompleteAfterBody,
    >;

    fn layer(&self, inner: S) -> Self::Service {
        let handle = Arc::downgrade(inner.handle());
        PendingRequests::new(
            SubchannelBase {
                inner: self.0.layer(inner),
                handle,
            },
            CompleteAfterBody,
        )
    }
}

pub(crate) type PoolMemberService<A, ReqBody, C, HC> =
    <SubchannelMaker as Layer<crate::connection::HTTP2Connection<A, C, HC, ReqBody>>>::Service;

pub(crate) type PoolService<ReqBody, S> = crate::balance_driver::ShimmedService<
    tower::balance::p2c::Balance<
        crate::balance_driver::ShimmedStream<crate::pool::DiscoverItem<S>>,
        http::Request<ReqBody>,
    >,
>;

pub(crate) fn pool_service_with_subchannel_layer<A, RS, RE, C, HC, HR, ReqBody, L, LL>(
    config: PoolConfig,
    label: L,
    connector: C,
    resolution_stream: RS,
    health_checker: HC,
    healthy_callback: HR,
    subchannel_layer: LL,
) -> (PoolService<ReqBody, LL::Service>, impl Future<Output = ()>)
where
    A: std::hash::Hash + std::fmt::Debug + Eq + Send + Sync + Clone + 'static,
    RS: Stream<Item = Result<Vec<A>, RE>>,
    RE: std::error::Error,
    C: Connector<A> + Send + Sync + 'static,
    C::IO: Send,
    C::Error: Send + Sync + 'static,
    HC: HealthChecker<ReqBody> + Send + Sync + 'static,
    HC::Error: std::error::Error + Clone + Send + Sync,
    HR: Fn(bool) + Send + 'static,
    ReqBody: hyper::body::Body + Send + Unpin + 'static,
    ReqBody::Data: Send,
    ReqBody::Error: std::error::Error + Send + Sync,
    L: AsRef<str>,
    LL: Layer<crate::connection::HTTP2Connection<A, C, HC, ReqBody>>,
    LL::Service: Service<http::Request<ReqBody>> + Unpin + tower::load::Load,
    <LL::Service as tower::load::Load>::Metric: std::fmt::Debug,
    <LL::Service as Service<http::Request<ReqBody>>>::Error:
        std::error::Error + Send + Sync + 'static,
{
    let discover = balancer_pool(
        config,
        label,
        crate::connection::HTTP2ConnectionMaker::new(connector, health_checker, subchannel_layer),
        crate::default_backoff(),
        healthy_callback,
        resolution_stream,
    );
    let mut shim = crate::balance_driver::Shim::new();
    let stack = tower::balance::p2c::Balance::new(shim.stream());
    shim.service_and_worker(stack, discover)
}

/// Build a new always-ready load-balanced HTTP client channel.
///
/// Please see [`crate::grpc_channel`] and [`crate::http_channel`] for a
/// higher level API.
///
/// Args:
/// - `config`: Load balancing pool configuration parameters such as health
///   thresholds. See [`PoolConfig`].
/// - `label`: A string name for this channel, used for logging and metrics.
/// - `connector`: A [`Connector`] plugin for establishing underlying byte
///   IO channels.
/// - `resolution_stream`: A [`Stream`] of resolved addresses available for
///   the channel to connect to. See [`crate::resolver`].
/// - `health_checker`: A [`HealthChecker`] plugin which is responsible for
///   health-checking individual channel member connections.
/// - `healthy_callback`: A closure which will be called with a single
///   boolean argument when the health status of the load-balanced channel
///   changes, with true meaning healthy and false unhealthy.
///
/// Returns:
/// - A [`tower::balance::p2c::Balance`] service which accepts requests.
/// - A worker which must be spawned as a task on an executor (e.g.
///   using [`tokio::task::spawn`]) in order for the channel to work.
#[allow(clippy::type_complexity)]
pub fn pool_service<A, RS, RE, C, HC, HR, ReqBody, L>(
    config: PoolConfig,
    label: L,
    connector: C,
    resolution_stream: RS,
    health_checker: HC,
    healthy_callback: HR,
) -> (
    PoolService<ReqBody, PoolMemberService<A, ReqBody, C, HC>>,
    impl Future<Output = ()>,
)
where
    A: std::hash::Hash + std::fmt::Debug + Eq + Send + Sync + Clone + 'static,
    RS: Stream<Item = Result<Vec<A>, RE>>,
    RE: std::error::Error,
    C: Connector<A> + Send + Sync + 'static,
    C::IO: Send,
    C::Error: Send + Sync + 'static,
    HC: HealthChecker<ReqBody> + Send + Sync + 'static,
    HC::Error: std::error::Error + Clone + Send + Sync,
    HR: Fn(bool) + Send + 'static,
    ReqBody: hyper::body::Body + Send + Unpin + 'static,
    ReqBody::Data: Send,
    ReqBody::Error: std::error::Error + Send + Sync,
    L: AsRef<str>,
{
    pool_service_with_subchannel_layer(
        config,
        label,
        connector,
        resolution_stream,
        health_checker,
        healthy_callback,
        SubchannelMaker::new(),
    )
}

impl<S, ReqBody> Channel<S, ReqBody>
where
    ReqBody: Send,
    S: Service<http::Request<ReqBody>> + Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send + Sync,
    S::Future: Send + 'static,
{
    /// Create a new top-level cloneable channel.
    ///
    /// This layers a [`tower::buffer::Buffer`] over a service and its worker
    /// as created by [`pool_service`], returning a wrapped service and worker.
    pub fn new<W>(stack: S, worker: W) -> (Self, impl Future<Output = ()>)
    where
        W: Future + Send + 'static,
    {
        let (stack, buffer_worker) = tower::buffer::Buffer::pair(stack, 1024);
        let worker = client_worker(buffer_worker, worker);
        (Self(stack), worker)
    }
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for Channel<S, ReqBody>
where
    ReqBody: Send,
    S: Service<http::Request<ReqBody>>,
    // This is what tower::buffer::Buffer requires. Note not std::error::Error!
    S::Error: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    S::Future: Send + 'static,
{
    type Response = <ChannelInner<S, ReqBody> as Service<http::Request<ReqBody>>>::Response;
    type Error = <ChannelInner<S, ReqBody> as Service<http::Request<ReqBody>>>::Error;
    type Future = <ChannelInner<S, ReqBody> as Service<http::Request<ReqBody>>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.0, cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        Service::call(&mut self.0, request)
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use futures::future::Either;
    use futures::pin_mut;
    use std::pin::pin;
    use tower::ServiceExt;
    use tower_service::Service;

    use crate::subchannel::SubchannelHandle;
    use crate::testutil::test_channel;

    #[tokio::test(start_paused = true)]
    async fn count_requests() {
        let (mut ch, worker, handles) = test_channel("test-channel-count_requests");
        pin_mut!(worker);

        let req = http::Request::builder()
            .uri("http://nowhere/success")
            .body(String::new())
            .expect("trivial test request");
        let req_exec = pin!(ch.ready().then(|s| s.expect("ready").call(req)));
        let response = match futures::future::select(worker, req_exec).await {
            Either::Left((_, fut)) => fut.await,
            Either::Right((response, _)) => response,
        }
        .expect("success");

        let count: usize = handles
            .handles()
            .iter()
            .map(|h| h.shared().requests_in_flight.get_unsync())
            .sum();
        assert_eq!(count, 1);
        drop(response);
        let count: usize = handles
            .handles()
            .iter()
            .map(|h| h.shared().requests_in_flight.get_unsync())
            .sum();
        assert_eq!(count, 0);
    }
}
