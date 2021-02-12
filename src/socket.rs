use async_std::io::{self, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

/// A alias for 'static + Unpin + Send + Read + Write
pub trait AsyncReadWriter: 'static + Unpin + Send + Read + Write {}

impl<T> AsyncReadWriter for T where T: 'static + Unpin + Send + Read + Write {}

/// A adaptor between futures::io::{AsyncRead, AsyncWrite} and tokio::io::{AsyncRead, AsyncWrite}.
pub struct Socket {
    rw: Option<Box<dyn AsyncReadWriter>>,
    buffer: [u8; 8192],
}

impl<T> From<T> for Socket
where
    T: AsyncReadWriter,
{
    fn from(stream: T) -> Self {
        Self {
            rw: Some(Box::new(stream)),
            buffer: [0; 8192],
        }
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut rw: Option<Box<dyn AsyncReadWriter>> = None;

        std::mem::swap(&mut rw, &mut self.rw);

        let max_len = self.buffer.len().min(buf.remaining());
        let mut read_buffer = &mut self.buffer[0..max_len];

        let result = Pin::new(&mut rw.as_mut().unwrap())
            .poll_read(cx, &mut read_buffer)
            .map(|result| {
                result.map(|len| {
                    buf.put_slice(&read_buffer[..len]);
                    ()
                })
            });

        std::mem::swap(&mut rw, &mut self.rw);
        result
    }
}

impl AsyncWrite for Socket {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.rw.as_mut().unwrap()).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.rw.as_mut().unwrap()).poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.rw.as_mut().unwrap()).poll_close(cx)
    }
}
