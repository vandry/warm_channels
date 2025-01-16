//! Miscellaneous utilities for warm_channels.

use futures::Stream;

/// A no-op health checker that probes nothing and returns always healthy.
#[derive(Debug)]
pub struct AssumeAlwaysHealthy;

impl Default for AssumeAlwaysHealthy {
    fn default() -> Self {
        Self
    }
}

impl<B> crate::HealthChecker<B> for AssumeAlwaysHealthy {
    type Error = std::convert::Infallible;

    fn watch<S, RespBody>(&self, _svc: S) -> impl Stream<Item = Result<(), Self::Error>> {
        futures::stream::once(futures::future::ready(Ok(())))
    }
}
