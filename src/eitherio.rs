use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

pin_project! {
    #[project = EitherIOProjected]
    pub enum EitherIO<A, B> {
        Left { #[pin] inner: A },
        Right { #[pin] inner: B },
    }
}

impl<A, B> AsyncRead for EitherIO<A, B>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.project() {
            EitherIOProjected::Left { inner } => inner.poll_read(cx, buf),
            EitherIOProjected::Right { inner } => inner.poll_read(cx, buf),
        }
    }
}

impl<A, B> AsyncWrite for EitherIO<A, B>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.project() {
            EitherIOProjected::Left { inner } => inner.poll_write(cx, buf),
            EitherIOProjected::Right { inner } => inner.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            EitherIOProjected::Left { inner } => inner.poll_flush(cx),
            EitherIOProjected::Right { inner } => inner.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            EitherIOProjected::Left { inner } => inner.poll_shutdown(cx),
            EitherIOProjected::Right { inner } => inner.poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.project() {
            EitherIOProjected::Left { inner } => inner.poll_write_vectored(cx, bufs),
            EitherIOProjected::Right { inner } => inner.poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Left { inner } => inner.is_write_vectored(),
            Self::Right { inner } => inner.is_write_vectored(),
        }
    }
}
