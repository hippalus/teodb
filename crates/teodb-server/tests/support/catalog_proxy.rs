use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

pub struct CatalogProxy {
    uri: String,
    forwarding: Arc<AtomicBool>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl CatalogProxy {
    pub async fn start(upstream_uri: &str) -> io::Result<Self> {
        let upstream = upstream_authority(upstream_uri)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let forwarding = Arc::new(AtomicBool::new(true));
        let task_forwarding = forwarding.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        let Ok((downstream, _)) = accepted else {
                            break;
                        };
                        let forwarding = task_forwarding.clone();
                        let upstream = upstream.clone();
                        tokio::spawn(async move {
                            if !forwarding.load(Ordering::Acquire) {
                                return;
                            }
                            let Ok(upstream) = TcpStream::connect(&upstream).await else {
                                return;
                            };
                            proxy_connection(downstream, upstream, forwarding).await;
                        });
                    }
                }
            }
        });
        Ok(Self {
            uri: format!("http://{address}"),
            forwarding,
            shutdown: Some(shutdown_tx),
            task,
        })
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn cut(&self) {
        self.forwarding.store(false, Ordering::Release);
    }

    pub fn restore(&self) {
        self.forwarding.store(true, Ordering::Release);
    }
}

impl Drop for CatalogProxy {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

async fn proxy_connection(mut downstream: TcpStream, mut upstream: TcpStream, forwarding: Arc<AtomicBool>) {
    let proxy = copy_bidirectional(&mut downstream, &mut upstream);
    tokio::pin!(proxy);
    tokio::select! {
        _ = &mut proxy => {}
        _ = wait_until_cut(forwarding) => {}
    }
}

async fn wait_until_cut(forwarding: Arc<AtomicBool>) {
    while forwarding.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
}

fn upstream_authority(uri: &str) -> io::Result<String> {
    let without_scheme = uri
        .strip_prefix("http://")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "catalog proxy supports http only"))?;
    let authority = without_scheme
        .split('/')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "catalog URI has no authority"))?;
    Ok(if authority.contains(':') {
        authority.to_owned()
    } else {
        format!("{authority}:80")
    })
}

#[cfg(test)]
mod tests {
    use super::upstream_authority;

    #[test]
    fn parses_http_authority() {
        assert_eq!(
            upstream_authority("http://127.0.0.1:8181/v1").unwrap(),
            "127.0.0.1:8181"
        );
        assert_eq!(upstream_authority("http://catalog").unwrap(), "catalog:80");
    }
}
