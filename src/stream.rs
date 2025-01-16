//! Connectors for plaintext bytestream connections.
//!
//! HTTP connections that become members of load-balanced channels are built
//! on top of bytestream IO channels returned by these connectors. This
//! module defines a connector for TCP/IP sockets, one for UNIX sockets, and
//! one that can accept either family.

#[cfg(feature = "unix")]
use futures::TryFutureExt;
use futures::{future::Either, FutureExt};
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;

#[cfg(feature = "unix")]
use crate::eitherio::EitherIO;
use crate::Connector;

const CONNECT_TIMEOUT: Duration = Duration::from_millis(30000);

/// A [`crate::Connector`] that connects with TCP/IP.
#[derive(Debug)]
pub struct TCPConnector;

impl Default for TCPConnector {
    fn default() -> Self {
        Self
    }
}

impl Connector<SocketAddr> for TCPConnector {
    type IO = TcpStream;
    type Error = std::io::Error;

    fn connect(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = std::io::Result<TcpStream>> + Send + Sync + 'static {
        let sock = match match addr {
            SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4(),
            SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6(),
        } {
            Ok(sock) => sock,
            Err(e) => {
                return Either::Left(std::future::ready(Err(e)));
            }
        };
        Either::Right(
            tokio::time::timeout(CONNECT_TIMEOUT, sock.connect(addr)).map(|r| match r {
                Ok(r) => r,
                Err(e) => Err(std::io::Error::new(std::io::ErrorKind::TimedOut, e)),
            }),
        )
    }
}

#[cfg(feature = "unix")]
mod unix {
    use futures::FutureExt;
    use std::future::Future;
    use tokio::net::UnixStream;

    use super::CONNECT_TIMEOUT;
    use crate::Connector;

    /// A [`crate::Connector`] that connects with UNIX stream sockets.
    ///
    /// Note that the functions in the [`crate::resolver`] module resolve using
    /// DNS and will never deliver UNIX socket addresses to connect to. In order
    /// to use this, a different resolver is necessary. Since a resolver is just
    /// a stream of address sets, this will work for a hardcoded address:
    ///
    /// ```
    /// let resolver = futures::stream::once(
    ///     std::future::ready(
    ///         Ok::<_, std::convert::Infallible>(vec![
    ///             std::path::PathBuf::from("/tmp/sock"),
    ///         ])
    ///     )
    /// );
    /// ```
    #[derive(Debug)]
    pub struct UNIXConnector;

    impl Default for UNIXConnector {
        fn default() -> Self {
            Self
        }
    }

    impl<P> Connector<P> for UNIXConnector
    where
        P: AsRef<std::path::Path> + Send + Sync + 'static,
    {
        type IO = UnixStream;
        type Error = std::io::Error;

        fn connect(
            &self,
            addr: P,
        ) -> impl Future<Output = std::io::Result<UnixStream>> + Send + Sync + 'static {
            tokio::time::timeout(CONNECT_TIMEOUT, UnixStream::connect(addr)).map(|r| match r {
                Ok(r) => r,
                Err(e) => Err(std::io::Error::new(std::io::ErrorKind::TimedOut, e)),
            })
        }
    }
}

#[cfg(feature = "unix")]
pub use unix::UNIXConnector;

/// A socket address type that can express both TCP/IP and UNIX addresses.
/// Used as the type of address that [`StreamConnector`] accepts.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum IPOrUNIXAddress {
    /// An IP socket address (`sockaddr_in` or `sockaddr_in6`).
    IP(SocketAddr),
    /// An UNIX socket address (`sockaddr_unix`).
    #[cfg(feature = "unix")]
    UNIX(std::path::PathBuf),
}

impl From<SocketAddr> for IPOrUNIXAddress {
    fn from(addr: SocketAddr) -> IPOrUNIXAddress {
        IPOrUNIXAddress::IP(addr)
    }
}

#[cfg(feature = "unix")]
impl From<std::path::PathBuf> for IPOrUNIXAddress {
    fn from(addr: std::path::PathBuf) -> IPOrUNIXAddress {
        IPOrUNIXAddress::UNIX(addr)
    }
}

/// A [`crate::Connector`] that connects either with TCP/IP or UNIX stream
/// sockets depending on which kind of [`IPOrUNIXAddress`] address it is given.
#[derive(Debug)]
pub struct StreamConnector;

impl Default for StreamConnector {
    fn default() -> Self {
        Self
    }
}

impl<A: Into<IPOrUNIXAddress>> Connector<A> for StreamConnector {
    #[cfg(feature = "unix")]
    type IO = EitherIO<TcpStream, tokio::net::UnixStream>;
    #[cfg(not(feature = "unix"))]
    type IO = TcpStream;
    type Error = std::io::Error;

    fn connect(
        &self,
        addr: A,
    ) -> impl Future<Output = std::io::Result<Self::IO>> + Send + Sync + 'static {
        match addr.into() {
            #[cfg(feature = "unix")]
            IPOrUNIXAddress::IP(addr) => Either::Left(
                TCPConnector
                    .connect(addr)
                    .map_ok(|io| EitherIO::Left { inner: io }),
            ),
            #[cfg(not(feature = "unix"))]
            IPOrUNIXAddress::IP(addr) => TCPConnector.connect(addr),
            #[cfg(feature = "unix")]
            IPOrUNIXAddress::UNIX(addr) => Either::Right(
                UNIXConnector
                    .connect(addr)
                    .map_ok(|io| EitherIO::Right { inner: io }),
            ),
        }
    }
}
