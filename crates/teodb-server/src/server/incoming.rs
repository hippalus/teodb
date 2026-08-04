use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use prometheus::{IntCounterVec, IntGaugeVec};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

struct ConnectionGuard {
    gauge: IntGaugeVec,
    transport: &'static str,
}

impl ConnectionGuard {
    fn new(gauge: IntGaugeVec, transport: &'static str) -> Self {
        gauge.with_label_values(&[transport]).inc();
        Self { gauge, transport }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.gauge
            .with_label_values(&[self.transport])
            .dec();
    }
}

#[derive(Clone)]
pub struct ConnectionMetrics {
    active: IntGaugeVec,
    rejected: IntCounterVec,
}

impl ConnectionMetrics {
    pub fn new(active: IntGaugeVec, rejected: IntCounterVec) -> Self {
        Self { active, rejected }
    }
}

#[derive(Clone)]
pub struct IncomingSettings {
    max_connections: usize,
    idle_timeout: Duration,
    metrics: ConnectionMetrics,
    transport: &'static str,
}

impl IncomingSettings {
    pub fn new(
        max_connections: usize,
        idle_timeout: Duration,
        metrics: ConnectionMetrics,
        transport: &'static str,
    ) -> Self {
        Self {
            max_connections,
            idle_timeout,
            metrics,
            transport,
        }
    }

    fn wrap<T>(
        &self,
        inner: T,
        peer: SocketAddr,
        local: Option<SocketAddr>,
        permit: OwnedSemaphorePermit,
    ) -> LimitedIo<T> {
        LimitedIo::new(
            inner,
            LimitedIoContext {
                peer,
                local,
                idle_timeout: self.idle_timeout,
                permit,
                guard: ConnectionGuard::new(self.metrics.active.clone(), self.transport),
            },
        )
    }
}

struct LimitedIoContext {
    peer: SocketAddr,
    local: Option<SocketAddr>,
    idle_timeout: Duration,
    permit: OwnedSemaphorePermit,
    guard: ConnectionGuard,
}

pub struct LimitedIo<T> {
    inner: T,
    peer: SocketAddr,
    local: Option<SocketAddr>,
    idle_timeout: Duration,
    read_deadline: Pin<Box<tokio::time::Sleep>>,
    write_deadline: Pin<Box<tokio::time::Sleep>>,
    _permit: OwnedSemaphorePermit,
    _guard: ConnectionGuard,
}

impl<T> LimitedIo<T> {
    fn new(inner: T, context: LimitedIoContext) -> Self {
        Self {
            inner,
            peer: context.peer,
            local: context.local,
            idle_timeout: context.idle_timeout,
            read_deadline: Box::pin(tokio::time::sleep(context.idle_timeout)),
            write_deadline: Box::pin(tokio::time::sleep(context.idle_timeout)),
            _permit: context.permit,
            _guard: context.guard,
        }
    }

    fn timed_out() -> io::Error {
        io::Error::new(io::ErrorKind::TimedOut, "connection idle timeout")
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for LimitedIo<T> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.read_deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Self::timed_out()));
        }
        let before = buffer.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buffer) {
            Poll::Ready(Ok(())) => {
                if buffer.filled().len() > before {
                    let deadline = tokio::time::Instant::now() + this.idle_timeout;
                    this.read_deadline.as_mut().reset(deadline);
                    this.write_deadline.as_mut().reset(deadline);
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for LimitedIo<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &[u8]) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        if this.write_deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Self::timed_out()));
        }
        match Pin::new(&mut this.inner).poll_write(cx, buffer) {
            Poll::Ready(Ok(written)) => {
                if written > 0 {
                    let deadline = tokio::time::Instant::now() + this.idle_timeout;
                    this.read_deadline.as_mut().reset(deadline);
                    this.write_deadline.as_mut().reset(deadline);
                }
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl tonic::transport::server::Connected for LimitedIo<TcpStream> {
    type ConnectInfo = tonic::transport::server::TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        tonic::transport::server::TcpConnectInfo {
            local_addr: self.local,
            remote_addr: Some(self.peer),
        }
    }
}

#[derive(Clone)]
pub struct LimitedAcceptor {
    permits: Arc<Semaphore>,
    settings: IncomingSettings,
}

impl LimitedAcceptor {
    pub fn new(settings: IncomingSettings) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(settings.max_connections)),
            settings,
        }
    }
}

impl<S> axum_server::accept::Accept<TcpStream, S> for LimitedAcceptor {
    type Stream = LimitedIo<TcpStream>;
    type Service = S;
    type Future = std::future::Ready<io::Result<(Self::Stream, Self::Service)>>;

    fn accept(&self, stream: TcpStream, service: S) -> Self::Future {
        let peer = match stream.peer_addr() {
            Ok(peer) => peer,
            Err(error) => return std::future::ready(Err(error)),
        };
        let local = stream.local_addr().ok();
        let permit = match self.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.settings
                    .metrics
                    .rejected
                    .with_label_values(&[self.settings.transport, "connection_limit"])
                    .inc();
                return std::future::ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "connection limit reached",
                )));
            }
        };
        std::future::ready(Ok((self.settings.wrap(stream, peer, local, permit), service)))
    }
}

pub fn flight_incoming(
    listener: TcpListener,
    settings: IncomingSettings,
) -> impl Stream<Item = io::Result<LimitedIo<TcpStream>>> + Send + 'static {
    struct State {
        listener: TcpListener,
        permits: Arc<Semaphore>,
        settings: IncomingSettings,
    }
    futures::stream::unfold(
        State {
            listener,
            permits: Arc::new(Semaphore::new(settings.max_connections)),
            settings,
        },
        |state| async move {
            loop {
                let (stream, peer) = match state.listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(error) => return Some((Err(error), state)),
                };
                let Ok(permit) = state.permits.clone().try_acquire_owned() else {
                    state
                        .settings
                        .metrics
                        .rejected
                        .with_label_values(&["flight", "connection_limit"])
                        .inc();
                    drop(stream);
                    continue;
                };
                let local = stream.local_addr().ok();
                let io = state.settings.wrap(stream, peer, local, permit);
                return Some((Ok(io), state));
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use axum_server::accept::Accept as _;
    use prometheus::Opts;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    fn test_metrics() -> (IntGaugeVec, IntCounterVec) {
        (
            IntGaugeVec::new(
                Opts::new("test_active_connections", "test active connections"),
                &["transport"],
            )
            .unwrap(),
            IntCounterVec::new(
                Opts::new("test_admission_rejections_total", "test admission rejections"),
                &["transport", "reason"],
            )
            .unwrap(),
        )
    }

    fn test_settings(
        max_connections: usize,
        idle_timeout: Duration,
        active: &IntGaugeVec,
        rejected: &IntCounterVec,
    ) -> IncomingSettings {
        IncomingSettings::new(
            max_connections,
            idle_timeout,
            ConnectionMetrics::new(active.clone(), rejected.clone()),
            "rest",
        )
    }

    #[tokio::test]
    async fn connection_guard_and_permit_release_on_drop() {
        let raw = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = raw.local_addr().unwrap();
        let (active, rejected) = test_metrics();
        let settings = test_settings(1, Duration::from_secs(1), &active, &rejected);
        let acceptor = LimitedAcceptor::new(settings);

        let (client, accepted) = tokio::join!(TcpStream::connect(addr), raw.accept());
        let client = client.unwrap();
        let (accepted, ()) = acceptor
            .accept(accepted.unwrap().0, ())
            .await
            .unwrap();
        assert_eq!(active.with_label_values(&["rest"]).get(), 1);

        drop(accepted);
        drop(client);
        assert_eq!(active.with_label_values(&["rest"]).get(), 0);
    }

    #[tokio::test]
    async fn saturated_listener_closes_new_connection_without_queueing() {
        let raw = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = raw.local_addr().unwrap();
        let (active, rejected) = test_metrics();
        let settings = test_settings(1, Duration::from_secs(1), &active, &rejected);
        let acceptor = LimitedAcceptor::new(settings);

        let (first_client, first_server) = tokio::join!(TcpStream::connect(addr), raw.accept());
        let first_client = first_client.unwrap();
        let (first_server, ()) = acceptor
            .accept(first_server.unwrap().0, ())
            .await
            .unwrap();
        let (rejected_client, rejected_server) = tokio::join!(TcpStream::connect(addr), raw.accept());
        let mut rejected_client = rejected_client.unwrap();
        let rejection = acceptor
            .accept(rejected_server.unwrap().0, ())
            .await;
        assert!(rejection.is_err(), "saturated connection must be rejected");

        tokio::time::timeout(Duration::from_secs(1), rejected_client.read_u8())
            .await
            .expect("saturated connection must close promptly")
            .expect_err("saturated connection must be closed");
        assert_eq!(
            rejected
                .with_label_values(&["rest", "connection_limit"])
                .get(),
            1
        );

        drop(first_server);
        drop(first_client);
        let (replacement, accepted) = tokio::join!(TcpStream::connect(addr), raw.accept());
        let replacement = replacement.unwrap();
        let (accepted, ()) = acceptor
            .accept(accepted.unwrap().0, ())
            .await
            .expect("permit must become reusable");
        assert_eq!(active.with_label_values(&["rest"]).get(), 1);
        drop(accepted);
        drop(replacement);
        assert_eq!(active.with_label_values(&["rest"]).get(), 0);
    }

    #[tokio::test]
    async fn idle_io_times_out_but_active_transfer_refreshes_deadline() {
        let (client, server) = tokio::io::duplex(64);
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits.try_acquire_owned().unwrap();
        let (active, rejected) = test_metrics();
        let settings = test_settings(1, Duration::from_millis(40), &active, &rejected);
        let mut limited = settings.wrap(server, "127.0.0.1:1".parse().unwrap(), None, permit);

        let writer = tokio::spawn(async move {
            let mut client = client;
            for value in 0..4_u8 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                client.write_all(&[value]).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let mut values = [0_u8; 4];
        limited.read_exact(&mut values).await.unwrap();
        assert_eq!(values, [0, 1, 2, 3]);

        let error = limited.read_u8().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        writer.await.unwrap();
    }
}
