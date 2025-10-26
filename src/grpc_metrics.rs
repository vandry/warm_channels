use http::{Request, Response};
use lazy_static::lazy_static;
use pin_project_lite::pin_project;
use prometheus::{register_counter_vec, register_histogram_vec};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Instant;
use tonic::{Code, GrpcMethod};
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

pin_project! {
    pub struct MetricsChannelFuture<F> {
        service: String,
        method: String,
        started_at: Option<Instant>,
        #[pin] inner: F,
    }
}

impl<F> MetricsChannelFuture<F> {
    pub fn new(service: String, method: String, inner: F) -> Self {
        Self {
            inner,
            started_at: None,
            service,
            method,
        }
    }
}

impl<F, O, E> Future for MetricsChannelFuture<F>
where
    F: Future<Output = Result<Response<O>, E>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let started_at = this.started_at.get_or_insert_with(|| {
            COUNTER_STARTED
                .with_label_values(&[this.service.as_str(), this.method.as_str()])
                .inc();
            Instant::now()
        });
        let v = ready!(this.inner.poll(cx));
        let code = v.as_ref().map_or(Code::Unknown, |resp| {
            resp.headers()
                .get("grpc-status")
                .map(|s| Code::from_bytes(s.as_bytes()))
                .unwrap_or(Code::Ok)
        });
        let code_str = format!("{:?}", code);
        let elapsed = Instant::now().duration_since(*started_at).as_secs_f64();
        COUNTER_HANDLED
            .with_label_values(&[
                this.service.as_str(),
                this.method.as_str(),
                code_str.as_str(),
            ])
            .inc();
        HISTOGRAM
            .with_label_values(&[
                this.service.as_str(),
                this.method.as_str(),
                code_str.as_str(),
            ])
            .observe(elapsed);
        Poll::Ready(v)
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
    T: Service<Request<I>, Response = Response<O>>,
    T::Future: Future<Output = Result<T::Response, T::Error>>,
{
    type Response = T::Response;
    type Error = T::Error;
    type Future = MetricsChannelFuture<T::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<I>) -> Self::Future {
        let (service, method) = req
            .extensions()
            .get::<GrpcMethod>()
            .map_or(("", ""), |gm| (gm.service(), gm.method()));
        MetricsChannelFuture::new(service.into(), method.into(), self.inner.call(req))
    }
}
