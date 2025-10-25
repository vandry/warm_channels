//! DNS resolution stream for client channels.
//!
//! The channel creation APIs [`crate::grpc_channel`] and
//! [`crate::http_channel`] as well as their lower layer counterpart
//! [`crate::pool_service`] all accept resolved addresses in the form of a
//! [`Stream`] of collections of resolved addresses. This module's
//! [`resolve_uri`] provides such a stream:
//!
//! ```
//! use std::sync::Arc;
//! use hickory_resolver::TokioResolver;
//!
//! let r = Arc::new(TokioResolver::builder_tokio().unwrap().build());
//! let uri = "https://example.org".try_into().unwrap();
//! let stream = warm_channels::resolve_uri(&uri, r);
//! ```
//!
//! Each item delivered on the stream expresses the currently-valid set of
//! resolved addresses and completely replaces the previous set. New items
//! should be delivered on the stream as often as the set of resolved
//! addresses changes.

use async_stream::stream;
use backoff::backoff::Backoff;
use futures::{Stream, StreamExt, TryFutureExt};
use hickory_resolver::{ResolveError, TokioResolver};
use http::Uri;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use thiserror::Error;

const MIN_TTL: Duration = Duration::from_millis(10000);

#[doc(hidden)]
pub trait Resolve {
    type Error;

    fn resolve(
        &self,
        name: &str,
        port: u16,
    ) -> impl Future<Output = Result<(Duration, Vec<SocketAddr>), Self::Error>> + Send;
}

impl Resolve for TokioResolver {
    type Error = ResolveError;

    fn resolve(
        &self,
        name: &str,
        port: u16,
    ) -> impl Future<Output = Result<(Duration, Vec<SocketAddr>), Self::Error>> + Send {
        self.lookup_ip(name).map_ok(move |l| {
            (
                l.valid_until().duration_since(std::time::Instant::now()),
                l.iter().map(|i| (i, port).into()).collect(),
            )
        })
    }
}

fn resolve_continuously<RR, R>(
    resolver: RR,
    name: String,
    port: u16,
) -> impl Stream<Item = Result<Vec<SocketAddr>, R::Error>> + Send
where
    RR: AsRef<R> + Send,
    R: Resolve,
    R::Error: Send,
{
    stream! {
        let mut backoff = crate::default_backoff();
        loop {
            match resolver.as_ref().resolve(&name, port).await {
                Ok((mut ttl, addrs)) => {
                    yield Ok(addrs);
                    if ttl < MIN_TTL {
                        ttl = MIN_TTL;
                    };
                    tokio::time::sleep(ttl).await;
                    backoff.reset();
                }
                Err(e) => {
                    yield Err(e);
                    match backoff.next_backoff() {
                        Some(d) => tokio::time::sleep(d).await,
                        None => {
                            break;
                        }
                    }
                }
            }
        }
    }
}

fn ip_literal(s: &str) -> Option<IpAddr> {
    if let Ok(ip) = s.parse() {
        return Some(ip);
    }
    if let Some(left) = s.strip_prefix('[') {
        if let Some(right) = left.strip_suffix(']') {
            if let Ok(ip) = right.parse() {
                return Some(ip);
            }
        }
    }
    None
}

/// Error type returned for immediate failures by [`resolve_uri`].
/// Later errors during resolution give [`hickory_resolver`]'s
/// [`ResolveError`] type.
#[derive(Debug, Error)]
pub enum ResolveUriError {
    /// URI supplied has [`None`] for a host.
    #[error("{0}: missing host")]
    MissingHost(Uri),
    /// URI has no explicit port and an unknown scheme, so we cannot guess the port.
    #[error("{0}: scheme must be http or https")]
    WrongScheme(Uri),
}

/// Given a [`TokioResolver`] and a URI, produce a [`Stream`] of resolved
/// address updates for that URI. See the module-level documentation.
pub fn resolve_uri<'a, R, RR>(
    uri: &Uri,
    resolver: RR,
) -> Result<
    impl Stream<Item = Result<Vec<SocketAddr>, R::Error>> + 'a + use<'a, R, RR>,
    ResolveUriError,
>
where
    RR: AsRef<R> + Send + 'a,
    R: Resolve + 'a,
    R::Error: Send,
{
    let Some(host) = uri.host() else {
        return Err(ResolveUriError::MissingHost(uri.clone()));
    };

    let port = match (uri.scheme_str(), uri.port_u16()) {
        (Some("http"), None) => 80,
        (Some("https"), None) => 443,
        (_, Some(p)) => p,
        _ => {
            return Err(ResolveUriError::WrongScheme(uri.clone()));
        }
    };

    Ok(if let Some(ip) = ip_literal(host) {
        futures::stream::once(futures::future::ready(Ok(vec![(ip, port).into()]))).left_stream()
    } else {
        resolve_continuously(resolver, host.to_owned(), port).right_stream()
    })
}

#[cfg(test)]
mod tests {
    use futures::poll;
    use std::pin::pin;
    use std::sync::Mutex;
    use std::task::Poll;

    use super::*;

    struct TestResolver {
        prepared_results: Mutex<Vec<Result<(Duration, Vec<SocketAddr>), usize>>>,
        expect_name: &'static str,
        expect_port: u16,
    }

    impl Resolve for TestResolver {
        type Error = usize;

        fn resolve(
            &self,
            name: &str,
            port: u16,
        ) -> impl Future<Output = Result<(Duration, Vec<SocketAddr>), Self::Error>> + Send {
            assert_eq!(name, self.expect_name);
            assert_eq!(port, self.expect_port);
            std::future::ready(
                self.prepared_results
                    .lock()
                    .unwrap()
                    .pop()
                    .expect("no more prepared_results"),
            )
        }
    }

    impl AsRef<TestResolver> for TestResolver {
        fn as_ref(&self) -> &TestResolver {
            self
        }
    }

    #[tokio::test(start_paused = true)]
    async fn http() {
        let addr = "[::1]:1".parse().unwrap();
        let r = TestResolver {
            prepared_results: Mutex::new(vec![
                Err(5),
                Ok((MIN_TTL, vec![addr])),
                Ok((MIN_TTL + MIN_TTL, vec![addr])),
            ]),
            expect_name: "foo",
            expect_port: 80,
        };
        let uri = "http://foo/".try_into().unwrap();
        let mut s = pin!(resolve_uri(&uri, r).expect("resolve_uri"));

        // 1
        let r = poll!(s.next());
        let Poll::Ready(Some(Ok(ref v))) = r else {
            panic!("Expected resolution, got {:?}", r);
        };
        assert_eq!(*v, vec![addr]);
        assert_matches!(poll!(s.next()), Poll::Pending);

        // Not waiting long enough to get another result.
        tokio::time::advance(MIN_TTL).await;
        assert_matches!(poll!(s.next()), Poll::Pending);

        // 2
        tokio::time::advance(MIN_TTL).await;
        assert_matches!(poll!(s.next()), Poll::Ready(Some(Ok(_))));
        assert_matches!(poll!(s.next()), Poll::Pending);

        // 3
        tokio::time::advance(MIN_TTL).await;
        assert_matches!(poll!(s.next()), Poll::Ready(Some(Err(5))));
        assert_matches!(poll!(s.next()), Poll::Pending);
    }

    #[tokio::test(start_paused = true)]
    async fn https() {
        let addr = "[::1]:1".parse().unwrap();
        let r = TestResolver {
            prepared_results: Mutex::new(vec![Ok((MIN_TTL, vec![addr]))]),
            expect_name: "foo",
            expect_port: 443,
        };
        let uri = "https://foo/".try_into().unwrap();
        let mut s = pin!(resolve_uri(&uri, r).expect("resolve_uri"));
        assert_matches!(poll!(s.next()), Poll::Ready(Some(Ok(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_too_short() {
        let ttl = MIN_TTL - Duration::from_millis(1);
        let addr = "[::1]:1".parse().unwrap();
        let r = TestResolver {
            prepared_results: Mutex::new(vec![Err(5), Ok((ttl, vec![addr]))]),
            expect_name: "foo",
            expect_port: 1,
        };
        let uri = "http://foo:1/".try_into().unwrap();
        let mut s = pin!(resolve_uri(&uri, r).expect("resolve_uri"));
        assert_matches!(poll!(s.next()), Poll::Ready(Some(Ok(_))));
        assert_matches!(poll!(s.next()), Poll::Pending);
        tokio::time::advance(ttl).await;
        assert_matches!(poll!(s.next()), Poll::Pending);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_matches!(poll!(s.next()), Poll::Ready(Some(Err(5))));
        assert_matches!(poll!(s.next()), Poll::Pending);
    }
}
