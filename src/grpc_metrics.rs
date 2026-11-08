use http::{Request, Response};
use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_histogram_vec};
use requests_gauge::{RequestsGaugeLayer, WithCompletionSubscriber};
use std::task::{Context, Poll};
use std::time::Instant;
use tonic::{Code, GrpcMethod};
use tower_layer::Layer;
use tower_service::Service;

const DEFAULT_HISTOGRAM_BUCKETS: [f64; 14] = [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

lazy_static! {
    static ref COUNTER_STARTED: prometheus::CounterVec = register_counter_vec!(
        "grpc_client_started_total",
        "Total number of client RPCs started.",
        &["grpc_service", "grpc_method"],
    )
    .unwrap();
    static ref COUNTER_HANDLED: prometheus::CounterVec = register_counter_vec!(
        "grpc_client_handled_total",
        "Total number of client RPCs completed, regardless of success or failure.",
        &["grpc_service", "grpc_method", "grpc_code"],
    )
    .unwrap();
    static ref HISTOGRAM: prometheus::HistogramVec = register_histogram_vec!(
        "grpc_client_handling_seconds",
        "Histogram for tracking client RPC duration",
        &["grpc_service", "grpc_method", "grpc_code"],
        DEFAULT_HISTOGRAM_BUCKETS.to_vec(),
    )
    .unwrap();
}

pub struct MetricsUpdater {
    service: String,
    method: String,
    started_at: Option<Instant>,
    response_code: Option<Code>,
}

impl MetricsUpdater {
    pub fn new(service: String, method: String) -> Self {
        Self {
            started_at: None,
            response_code: None,
            service,
            method,
        }
    }
}

impl<B, E> requests_gauge::RequestsGaugeSubscriber<Result<Response<B>, E>> for MetricsUpdater {
    fn request_started(&mut self) {
        self.started_at = Some(Instant::now());
        COUNTER_STARTED
            .with_label_values(&[self.service.as_str(), self.method.as_str()])
            .inc();
    }

    fn request_responded(&mut self, response: &Result<Response<B>, E>) {
        self.response_code = response.as_ref().ok().map(|resp| {
            resp.headers()
                .get("grpc-status")
                .map(|s| Code::from_bytes(s.as_bytes()))
                .unwrap_or(Code::Ok)
        });
    }
}

impl Drop for MetricsUpdater {
    fn drop(&mut self) {
        if let Some(started_at) = self.started_at.take() {
            let code_str = format!("{:?}", self.response_code.take().unwrap_or(Code::Unknown));
            let elapsed = Instant::now().duration_since(started_at).as_secs_f64();
            COUNTER_HANDLED
                .with_label_values(&[
                    self.service.as_str(),
                    self.method.as_str(),
                    code_str.as_str(),
                ])
                .inc();
            HISTOGRAM
                .with_label_values(&[
                    self.service.as_str(),
                    self.method.as_str(),
                    code_str.as_str(),
                ])
                .observe(elapsed);
        }
    }
}

pub struct MetricsChannel<T> {
    inner: T,
}

impl<T> MetricsChannel<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<I, O, T> Service<Request<I>> for MetricsChannel<T>
where
    T: Service<WithCompletionSubscriber<Request<I>, MetricsUpdater>, Response = Response<O>>,
    T::Future: Future<Output = Result<T::Response, T::Error>>,
{
    type Response = T::Response;
    type Error = T::Error;
    type Future = T::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<I>) -> Self::Future {
        let (service, method) = req
            .extensions()
            .get::<GrpcMethod>()
            .map_or(("", ""), |gm| (gm.service(), gm.method()));
        let updater = MetricsUpdater::new(service.into(), method.into());
        self.inner.call(WithCompletionSubscriber::new(req, updater))
    }
}

#[derive(Default)]
pub struct MetricsLayer;

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsChannel<<RequestsGaugeLayer as Layer<S>>::Service>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsChannel::new(RequestsGaugeLayer::new().layer(inner))
    }
}
