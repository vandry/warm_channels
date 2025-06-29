//! Connectors for bytestream connections with TLS.
//!
//! This connector layers on top of another connector (from [`crate::stream`])
//! to add TLS to the stream.
//!
//! ```
//! use http::Uri;
//! use std::sync::Arc;
//! use hickory_resolver::TokioResolver;
//! use tokio_rustls::rustls::RootCertStore;
//! use tokio_rustls::rustls::client::ClientConfig;
//!
//! // (A real ClientConfig will have non-empty trust roots
//! //  and usually client auth.)
//! let conf = ClientConfig::builder()
//!     .with_root_certificates(Arc::new(RootCertStore::empty()))
//!     .with_no_client_auth();
//!
//! let r = Arc::new(TokioResolver::builder_tokio().unwrap().build());
//!
//! // Optionally, separate URIs for identity and connectivity
//! let connect_uri: Uri = "https://example.org".try_into().unwrap();
//! let identity_uri: Uri = "spiffe://example.org/some_jon".try_into().unwrap();
//!
//! // This URI will determine the address we connect to.
//! let stream = warm_channels::resolve_uri(&connect_uri, r).unwrap();
//! let (stack, worker) = warm_channels::grpc_channel(
//!     // This URI will form the HTTP origin.
//!     identity_uri.clone(),
//!     warm_channels::grpc::GRPCChannelConfig::default(),
//!     "demo",
//!     warm_channels::tls::TLSConnector::new(
//!         warm_channels::stream::TCPConnector::default(),
//!         // This URI will be used for SNI and server cert verification.
//!         &identity_uri,
//!         Some(&conf)
//!     ).expect("TLS connector"),
//!     stream,
//!     |h| println!("healthy: {}", h),
//! );
//! ```

use futures::future::Either;
use futures::{FutureExt, TryFutureExt};
use http::Uri;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tokio_rustls::rustls::client::ClientConfig;
use tokio_rustls::rustls::pki_types::{InvalidDnsNameError, ServerName};

use crate::Connector;
use crate::eitherio::EitherIO;

#[derive(Debug)]
enum TLSConnectorStyle {
    Plain,
    TLS(ServerName<'static>, Arc<ClientConfig>),
}

/// A [`Connector`] that optionally wraps another one with a TLS client.
#[derive(Debug)]
pub struct TLSConnector<T> {
    inner: T,
    style: TLSConnectorStyle,
}

/// Error type returned at [`TLSConnector`] creation time.
#[derive(Debug, Error)]
pub enum TLSConnectorCreationError {
    /// TLS wrapping was requested (by URI) but no TLS [`ClientConfig`] was given.
    #[error("https URI without TLS configuration")]
    MissingTLSConfig,
    /// The URI host was not parseable as a TLS [`ServerName`].
    #[error("{0}")]
    InvalidNameError(#[from] InvalidDnsNameError),
}

impl<T> TLSConnector<T> {
    /// Create a new TLS connector, given an inner connector that delivers a
    /// plaintext bytestream. If the URI scheme is https then TLS is added.
    /// Otherwise the connector acts as a passthrough.
    pub fn new(
        inner: T,
        uri: &Uri,
        config: Option<&ClientConfig>,
    ) -> Result<Self, TLSConnectorCreationError> {
        let spiffe = if uri.scheme() == Some(&http::uri::Scheme::HTTPS) {
            false
        } else if uri
            .scheme()
            .map(|s| s.as_str() == "spiffe")
            .unwrap_or_default()
        {
            true
        } else {
            return Ok(Self {
                inner,
                style: TLSConnectorStyle::Plain,
            });
        };
        let Some(c) = config else {
            return Err(TLSConnectorCreationError::MissingTLSConfig);
        };
        let mut c = c.clone();
        c.alpn_protocols = vec![b"h2".to_vec()];
        let name = if spiffe {
            // It's not clear what server name we should use with SPIFFE.
            // The ServerName will never match hostname verification since
            // it can only be either DNS or IP while SPIFFE is a URI SAN.
            // As for SNI, there is this discussion without a conclusion:
            // https://github.com/spiffe/spiffe/issues/39
            // Let's resolve it by not doing SNI at all and focus on
            // root_hint_subjects as a better means of helping the server
            // choose the right identity to present: if the server and client
            // are able to authenticate one another's SPIFFE identities at all
            // then they likely share a SPIFFE domain (possibly across a
            // federation) and the client is probably able to provide the
            // correct root_hint_subjects from the trust bundle that goes
            // with the domain.
            c.enable_sni = false;
            ServerName::try_from("spiffe").unwrap()
        } else {
            ServerName::try_from(uri.host().unwrap_or_default())?
        };
        Ok(Self {
            inner,
            style: TLSConnectorStyle::TLS(name.to_owned(), Arc::new(c)),
        })
    }
}

/// Error type returned by [`TLSConnector`] when establishing connections.
#[derive(Debug, Error)]
pub enum TLSConnectorError<T: std::error::Error> {
    /// The inner connector returned an error.
    #[error("{0}")]
    InnerError(#[from] T),
    /// An error occcurred at the TLS layer.
    #[error("{0}")]
    TLSError(#[source] std::io::Error),
}

impl<A, T> Connector<A> for TLSConnector<T>
where
    T: Connector<A>,
    T::IO: Send + Sync + 'static,
    T::Error: 'static,
{
    type IO = EitherIO<T::IO, tokio_rustls::client::TlsStream<T::IO>>;
    type Error = TLSConnectorError<T::Error>;

    fn connect(
        &self,
        addr: A,
    ) -> impl Future<Output = Result<Self::IO, Self::Error>> + Send + Sync + 'static {
        let inner = self.inner.connect(addr).err_into();
        match self.style {
            TLSConnectorStyle::Plain => {
                Either::Left(inner.map_ok(|io| EitherIO::Left { inner: io }))
            }
            TLSConnectorStyle::TLS(ref name, ref config) => {
                let name = name.clone();
                let config = Arc::clone(config);
                Either::Right(inner.and_then(move |io| {
                    tokio_rustls::TlsConnector::from(config)
                        .connect(name, io)
                        .map(|r| match r {
                            Ok(io) => Ok(EitherIO::Right { inner: io }),
                            Err(e) => Err(TLSConnectorError::TLSError(e)),
                        })
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio_rustls::rustls::RootCertStore;

    #[test]
    fn without_tls() {
        let uri = Uri::try_from("http://example.org").unwrap();
        let c = TLSConnector::new((), &uri, None).unwrap();
        assert_matches!(c.style, TLSConnectorStyle::Plain);

        let uri = Uri::try_from("https://example.org").unwrap();
        let e = TLSConnector::new((), &uri, None).expect_err("no tls");
        assert_matches!(e, TLSConnectorCreationError::MissingTLSConfig);

        let uri = Uri::try_from("spiffe://example.org").unwrap();
        let e = TLSConnector::new((), &uri, None).expect_err("no tls");
        assert_matches!(e, TLSConnectorCreationError::MissingTLSConfig);

        let uri = Uri::try_from("unknown://example.org").unwrap();
        let c = TLSConnector::new((), &uri, None).unwrap();
        assert_matches!(c.style, TLSConnectorStyle::Plain);
    }

    #[test]
    fn with_tls() {
        let conf = ClientConfig::builder()
            .with_root_certificates(Arc::new(RootCertStore::empty()))
            .with_no_client_auth();

        let uri = Uri::try_from("http://example.org").unwrap();
        let c = TLSConnector::new((), &uri, Some(&conf)).unwrap();
        assert_matches!(c.style, TLSConnectorStyle::Plain);

        let uri = Uri::try_from("https://example.org").unwrap();
        let c = TLSConnector::new((), &uri, Some(&conf)).unwrap();
        match c.style {
            TLSConnectorStyle::TLS(ServerName::DnsName(sn), co) => {
                assert_eq!(sn.as_ref(), "example.org");
                assert!(co.enable_sni);
            }
            _ => {
                panic!("wrong style");
            }
        }

        let uri = Uri::try_from("spiffe://example.org").unwrap();
        let c = TLSConnector::new((), &uri, Some(&conf)).unwrap();
        match c.style {
            TLSConnectorStyle::TLS(ServerName::DnsName(sn), co) => {
                assert_eq!(sn.as_ref(), "spiffe");
                assert!(!co.enable_sni);
            }
            _ => {
                panic!("wrong style");
            }
        }

        let uri = Uri::try_from("unknown://example.org").unwrap();
        let c = TLSConnector::new((), &uri, Some(&conf)).unwrap();
        assert_matches!(c.style, TLSConnectorStyle::Plain);
    }
}
