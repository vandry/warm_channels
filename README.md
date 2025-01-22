<!-- cargo-rdme start -->

Always-ready HTTP client channels for gRPC or other RPC-like requests.

In a microservices environment, components usually serve RPCs from their
clients in part by making further requests to backends of their own. For
example an application frontend serves a request by making a query to a
storage backend and a notification queue before applying business logic
and constructing a response to send to its own client, the end user.

When RPC frontends and backends are both replicated as multiple tasks,
discipline in managing and load balancing the flow of requests is
important. This crate aims to offer the client side of that function.

The main focus is on offering an always-ready gRPC client channel type
(in the [`grpc`] module) which load-balances over multiple individual
actively health-checked connections. A generic HTTP client channel typr
(in the [`http`] module) is also provided but it is currently less
polished.

The building blocks revolve around [`tower`] (for the service stack) and
[`hyper`] (for HTTP), plus [`tokio`] and [`rustls`]. In particular, the
basic load balancing uses [`tower::balance::p2c`]. Finally, the gRPC
layer on top is designed to be used with [`tonic`].

Some of the features brought by this crate are:

- As soon as they are created, and continuing in the background for their
lifetime, channels begin attempting to constantly maintain a
sufficiently-sized pool of healthy member connections to backends.
- If multiple backend addresses are available, the channel will attempt
to use all of them, using different addresses for different member
connections, mitigating the effect of single backend tasks going away.
- Name resolution follows DNS TTLs, so that if the backend is using
DNS-based load balancing the channel notices and reacts when its
assignment changes.
- Member connections are individually health-checked and evicted from
the channel when they fail. Note that this is different to plain
[`tower::balance::p2c::Balance`], which only polls (and potentially
evicts) members on use.
- Channels that become critically unhealthy (too few healthy members are
healthy) are handled in a degraded mode: we temporarily make connected
but unhealthy members available to accept requests and stop evicting them.

This crate can be used by itself but is designed to be used with
[`comprehensive`] which will further add the following features:

- Easy macro-based declaration of a gRPC client made available as a
resource to the rest of the assembly.
- Automatically connected to the assembly-wide health status signal so
that when a client channel to a required backend is unhealthy then the
colocated server resources also report unhealthy.
- TLS configuration supplied from the assembly-wide TLS resource
(reflecting the use of a process-wide cryptographic identiry and
dynamically reloaded when changed (such as on certificate rotation).
- Configuration of the channel's backend URI and other configurable
properties using a standard set of flags.

# gRPC client example using warm_channels directly

```rust
use std::sync::Arc;
use trust_dns_resolver::system_conf::read_system_conf;
use trust_dns_resolver::TokioAsyncResolver;

let (resolver_config, mut resolver_opts) = read_system_conf().unwrap();
let r = Arc::new(TokioAsyncResolver::tokio(resolver_config, resolver_opts));
let uri = "https://example.org".try_into().unwrap();
let stream = warm_channels::resolve_uri(&uri, r).unwrap();
let (stack, worker) = warm_channels::grpc_channel(
    uri.clone(),
    warm_channels::grpc::GRPCChannelConfig::default(),
    "demo",
    warm_channels::stream::TCPConnector::default(),
    stream,
    |h| println!("healthy: {}", h),
);
tokio::task::spawn(worker);
let client = pb::test_client::TestClient::with_origin(stack, uri);

println!("{:?}", client.greet(tonic::Request::new(())).await);
```

# gRPC client example using [`comprehensive`]

```rust
use comprehensive_grpc::GrpcClient;

#[derive(GrpcClient)]
struct Client(
    pb::test_client::TestClient<comprehensive_grpc::client::Channel>,
    comprehensive_grpc::client::ClientWorker,
);
```

`Client` may then be included as a dependency in a Comprehensive Assembly.
See the full [gRPC hello world client example].

# Possible future work:

- Dynamically sized member set, probably based on reacting to request
processing latency.

# Features

- **grpc**: Enable gRPC functionality
- **tls**: Enable crypto functionality
- **metrics**: Export metrics about channel health and gRPC requests
- **unix**; Enable UNIX domain sockets connector.

All are enabled by default.

[`comprehensive`]: https://docs.rs/comprehensive/latest/comprehensive/
[`rustls`]: https://docs.rs/rustls/latest/rustls/
[`tonic`]: https://docs.rs/tonic/latest/tonic/
[gRPC hello world client example]: https://github.com/vandry/comprehensive/blob/master/examples/src/helloworld-grpc-client.rs

<!-- cargo-rdme end -->
