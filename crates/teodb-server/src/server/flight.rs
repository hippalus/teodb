//! Flight service construction.

use std::sync::Arc;

pub fn build_flight_server(
    app_state: &Arc<teodb_api::http::AppState>,
    cfg: &crate::config::TeoDBConfig,
) -> arrow_flight::flight_service_server::FlightServiceServer<teodb_api::flight::TeoFlightService> {
    let svc = teodb_api::flight::TeoFlightService::new(app_state.clone());
    arrow_flight::flight_service_server::FlightServiceServer::new(svc)
        .max_decoding_message_size(cfg.server.flight_max_decoding_message_bytes)
        .max_encoding_message_size(cfg.server.flight_max_encoding_message_bytes)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_flight::flight_descriptor::DescriptorType;
    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::{Empty, FlightDescriptor};
    use tonic::Code;

    use super::*;

    struct FlightHarness {
        client: FlightServiceClient<tonic::transport::Channel>,
        shutdown: tokio::sync::oneshot::Sender<()>,
        server: tokio::task::JoinHandle<()>,
        _app: teodb_test_support::TestApp,
    }

    impl FlightHarness {
        async fn start(max_decoding_bytes: usize, max_encoding_bytes: usize) -> Self {
            let app = teodb_test_support::TestAppBuilder::rest_api()
                .build()
                .await;
            let mut config = crate::config::TeoDBConfig::default();
            config.server.flight_max_decoding_message_bytes = max_decoding_bytes;
            config.server.flight_max_encoding_message_bytes = max_encoding_bytes;
            let service = build_flight_server(&app.state, &config);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .unwrap();
            let address = listener.local_addr().unwrap();
            let incoming = futures::stream::unfold(listener, |listener| async move {
                Some((listener.accept().await.map(|(stream, _)| stream), listener))
            });
            let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming_shutdown(incoming, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .unwrap();
            });
            let endpoint =
                tonic::transport::Endpoint::from_shared(format!("http://{address}")).expect("valid Flight endpoint");
            let channel = tokio::time::timeout(Duration::from_secs(5), endpoint.connect())
                .await
                .expect("Flight client connection timed out")
                .expect("connect Flight client");
            let client = FlightServiceClient::new(channel);
            Self {
                client,
                shutdown,
                server,
                _app: app,
            }
        }

        async fn stop(self) {
            let _ = self.shutdown.send(());
            tokio::time::timeout(Duration::from_secs(5), self.server)
                .await
                .expect("Flight server shutdown timed out")
                .expect("Flight server task panicked");
        }
    }

    #[tokio::test]
    async fn decoding_limit_rejects_an_oversized_flight_message_over_a_real_socket() {
        let mut harness = FlightHarness::start(128, 1024 * 1024).await;
        let descriptor = FlightDescriptor {
            r#type: DescriptorType::Cmd as i32,
            cmd: vec![b'x'; 4096].into(),
            path: Vec::new(),
        };

        let status = harness
            .client
            .get_flight_info(descriptor)
            .await
            .expect_err("oversized request must be rejected");
        assert_eq!(status.code(), Code::OutOfRange);

        harness.stop().await;
    }

    #[tokio::test]
    async fn encoding_limit_rejects_an_oversized_flight_message_over_a_real_socket() {
        let mut harness = FlightHarness::start(1024 * 1024, 8).await;
        let response = harness
            .client
            .list_actions(Empty {})
            .await
            .expect("stream setup succeeds before encoding the first action");
        let status = response
            .into_inner()
            .message()
            .await
            .expect_err("oversized response message must be rejected");
        assert_eq!(status.code(), Code::OutOfRange);

        harness.stop().await;
    }
}
