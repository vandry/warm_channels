use futures::{Stream, StreamExt};
use http::StatusCode;
use itertools::Itertools;
use std::borrow::Cow;
use std::future::Future;
use std::pin::{Pin, pin};
use std::task::{Context, Poll};
use std::time::Duration;
use tower_service::Service;

use crate::snapshot::{Collection, Entry};

const REPORT_TIMEOUT: Duration = Duration::from_millis(4000);

#[derive(Debug)]
pub struct FullReport {
    pub resolved_addresses: Vec<String>,
    pub subchannels: Vec<String>,
}

#[derive(Debug)]
pub struct Report {
    pub id: usize,
    pub label: String,
    pub more: Option<FullReport>,
}

impl Report {
    fn header(&self) -> String {
        format!(
            r#"<li><a href="{}">{}</a></li>"#,
            self.id,
            html_escape::encode_text(&self.label)
        )
    }

    fn full(&self) -> String {
        let more = self.more.as_ref().unwrap();
        format!(
            r#"
            <html><head>
                <title>Debug: Client Channel: {}</title>
            </head>
            <body><h2><a href="https://docs.rs/warm_channels/latest/warm_channels/">warm_channels</a> channel "{}"</h2>
            <h3>Resolved addresses</h3>
            <ul>
            {}
            </ul>
            <h3>Subchannels</h3>
            <ul>
            {}
            </ul>
            </body></html>
            "#,
            html_escape::encode_text(&self.label),
            html_escape::encode_text(&self.label),
            more.resolved_addresses
                .iter()
                .map(|a| format!("<li>{}</li>\n", html_escape::encode_text(a)))
                .join(""),
            more.subchannels
                .iter()
                .map(|a| format!("<li>{}</li>\n", html_escape::encode_text(a)))
                .join("")
        )
    }
}

static REPORTING_CHANNELS: Collection<Report> = Collection::new();

pub fn add_channel() -> Entry<'static, Report> {
    REPORTING_CHANNELS.add()
}

async fn index(collection: &Collection<Report>, base: Cow<'static, str>) -> String {
    let timeout = tokio::time::sleep(REPORT_TIMEOUT);
    let mut channels = pin!(collection.request_all(false).take_until(timeout));
    let mut items = Vec::with_capacity(channels.get_ref().size_hint().1.unwrap_or(0));
    while let Some(ch) = channels.next().await {
        items.push(ch.header());
    }
    let leftover = channels.get_ref().size_hint().1.unwrap_or(0);
    if leftover > 0 {
        items.push(format!("<li>Plus up to {} channels that failed to report (have their service tasks crashed?)</li>\n", leftover));
    }
    format!(
        r#"
        <html><head>
            <title>Debug: Client Channels</title>
            {}
        </head>
        <body><h2><a href="https://docs.rs/warm_channels/latest/warm_channels/">warm_channels</a> active channels</h2>
        <ul>
        {}
        </ul></body></html>
    "#,
        base,
        items.join("\n")
    )
}

/// HTTP Service that exposes diagnostic information about channels managed
/// by this crate. This [`Service`] is meant to be installed in an HTTP
/// server and will serve human-readable debugging information about all
/// currently active channels in the process.
#[derive(Clone)]
pub struct ChannelDiagService<'a>(&'a Collection<Report>);

impl Default for ChannelDiagService<'static> {
    fn default() -> Self {
        Self(&REPORTING_CHANNELS)
    }
}

enum ChannelDiagServiceAction {
    Index(Cow<'static, str>),
    Specific(usize),
    NotFound,
}

impl<'a, B> Service<http::Request<B>> for ChannelDiagService<'a> {
    type Response = http::Response<String>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'a>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let path = request.uri().path();
        let action = if path == "/" {
            let original_uri = request.extensions().get::<axum::extract::OriginalUri>();
            ChannelDiagServiceAction::Index(match original_uri {
                Some(uri) => {
                    let path = uri.path();
                    format!(
                        r#"<base href="{}{}">"#,
                        path,
                        if path.ends_with("/") { "" } else { "/" }
                    )
                    .into()
                }
                None => "".into(),
            })
        } else {
            path.strip_prefix("/")
                .map(|s| match s.parse::<usize>() {
                    Ok(unique) => ChannelDiagServiceAction::Specific(unique),
                    Err(_) => ChannelDiagServiceAction::NotFound,
                })
                .unwrap_or(ChannelDiagServiceAction::NotFound)
        };
        let collection = self.0;
        Box::pin(async move {
            let resp = match action {
                ChannelDiagServiceAction::Index(base) => Some(index(collection, base).await),
                ChannelDiagServiceAction::Specific(unique) => collection
                    .request_by_unique(unique, true)
                    .await
                    .map(|r| r.full()),
                ChannelDiagServiceAction::NotFound => None,
            };
            Ok(match resp {
                Some(body) => http::Response::builder()
                    .header("Content-Type", "text/html")
                    .body(body)
                    .unwrap(),
                None => http::Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body("".into())
                    .unwrap(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::{FutureExt, poll, select};
    use std::pin::pin;
    use tower::ServiceExt;

    use super::*;
    use crate::channel::PoolService;
    use crate::testutil::{TestServer, TestServerAddress};
    use crate::util::AssumeAlwaysHealthy;

    fn test_channel(
        name: &'static str,
    ) -> (
        PoolService<TestServerAddress, String, TestServer, AssumeAlwaysHealthy>,
        impl Future,
    ) {
        let rs = futures::stream::once(futures::future::ready(Ok::<_, std::convert::Infallible>(
            vec![TestServerAddress::Working],
        )));
        crate::channel::pool_service(
            crate::pool::PoolConfig::default(),
            name,
            TestServer::new(),
            rs,
            AssumeAlwaysHealthy::default(),
            |_| (),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn channel_list() {
        let (_c, worker) = test_channel("test-channel-channel_list");
        let mut worker = pin!(worker.fuse());
        let mut req = pin!(
            ChannelDiagService::default()
                .oneshot(
                    http::Request::get("http://x/")
                        .body(String::from(""))
                        .unwrap()
                )
                .fuse()
        );
        select! {
            _ = worker => (),
            r = req => {
                assert!(r.unwrap().into_body().contains("test-channel-channel_list"));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn channel_does_not_respond() {
        let (_c, _worker) = test_channel("test-channel-channel_does_not_respond");
        let mut req = pin!(
            ChannelDiagService::default()
                .oneshot(
                    http::Request::get("http://x/")
                        .body(String::from(""))
                        .unwrap()
                )
                .fuse()
        );
        assert_matches!(poll!(&mut req), Poll::Pending);
        tokio::time::advance(REPORT_TIMEOUT).await;
        assert!(
            req.await
                .unwrap()
                .into_body()
                .contains("channels that failed to report")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn channel_details() {
        let (_c, worker) = test_channel("test-channel-channel_details");
        let mut worker = pin!(worker.fuse());
        let mut test = pin!(
            async {
                let unique = pin!(
                    REPORTING_CHANNELS
                        .request_all(false)
                        .filter(|r| std::future::ready(r.label == "test-channel-channel_details"))
                )
                .next()
                .await
                .expect("test channel some-channel-name")
                .id;
                ChannelDiagService::default()
                    .oneshot(
                        http::Request::get(format!("http://x/{}", unique))
                            .body(String::from(""))
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                    .into_body()
            }
            .fuse()
        );
        let body = select! {
            _ = worker => String::from("worker gone"),
            b = test => b,
        };
        assert!(
            body.contains("Working"),
            "want body with Working, got {:?}",
            body
        );
    }
}
