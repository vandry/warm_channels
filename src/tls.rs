//! Connectors for bytestream connections with TLS.
//!
//! This connector layers on top of another connector (from [`crate::stream`])
//! to add TLS to the stream.

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
        let style = if uri.scheme() == Some(&http::uri::Scheme::HTTPS) {
            match config {
                Some(c) => {
                    let mut c = c.clone();
                    c.alpn_protocols = vec![b"h2".to_vec()];
                    TLSConnectorStyle::TLS(
                        ServerName::try_from(uri.host().unwrap_or_default())?.to_owned(),
                        Arc::new(c),
                    )
                }
                None => {
                    return Err(TLSConnectorCreationError::MissingTLSConfig);
                }
            }
        } else {
            TLSConnectorStyle::Plain
        };
        Ok(Self { inner, style })
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
