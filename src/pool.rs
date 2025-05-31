use async_stream::stream;
use backoff::backoff::Backoff;
use futures::future::Either;
use futures::{FutureExt, Stream, StreamExt, select};
use humantime::format_duration;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use thiserror::Error;
use tower::discover::Change;

use crate::addresses::{AddressSlot, ResolvedAddressChoice, ResolvedAddressCollection};
use crate::report::{Inventory, InventoryReport};
use crate::{LoggedEvent, PoolMemberMaker};

/// Configuration parameters for load-balanced channels.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Maximum number of subchannels this pool will host.
    /// Currently we don't add any channels past n_subchannels_want so there is
    /// no need to set this any higher.
    pub n_subchannels_max: usize,

    /// Desired number of healthy subchannels.
    pub n_subchannels_want: usize,

    /// Number of healthy subchannels below which we report ourselves as
    /// unhealthy. Below this we also log channel errors periodically at
    /// warning level.
    pub n_subchannels_healthy_min: usize,

    /// With fewer healthy subchannels than this, we press subchannels into
    /// service even if they are unhealthy and we don't disconnect and
    /// retry any subchannels for health check failures. This only happens
    /// after we meet this minimum then dip below it.
    pub n_subchannels_healthy_critical_min: usize,

    /// If the channel is unhealthy, log the reason why after the situation
    /// persists for this amount of time. Especially useful for channels that
    /// are unhealthy at startup because clients will perceive such channels
    /// as perpetually pending with no other feedback.
    pub log_unhealthy_initial_delay: Option<Duration>,

    /// After initially logging that a channel is unhealthy, repeat the
    /// logging at this interval.
    pub log_unhealthy_repeat_delay: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            n_subchannels_max: 3,
            n_subchannels_want: 3,
            n_subchannels_healthy_min: 2,
            n_subchannels_healthy_critical_min: 1,
            log_unhealthy_initial_delay: Some(Duration::from_millis(10000)),
            log_unhealthy_repeat_delay: Some(Duration::from_millis(90000)),
        }
    }
}

#[derive(Clone, Debug, Error, PartialEq)]
enum BalancerPoolErrorKind {
    #[error("name not resolved yet")]
    NotYetResolved,
    #[error("gave up with no address resolution result")]
    ResolverGaveUp,
    #[error("still connecting")]
    NotYetConnected,
    #[error("{0}")]
    ConnectionError(String),
}

#[derive(Debug)]
pub(crate) struct PoolCommon {
    critically_unhealthy: AtomicBool,
    last_error: std::sync::Mutex<Option<LoggedEvent<BalancerPoolErrorKind>>>,
}

impl PoolCommon {
    fn new() -> Self {
        PoolCommon {
            critically_unhealthy: AtomicBool::new(false),
            last_error: std::sync::Mutex::new(Some(LoggedEvent::new(
                BalancerPoolErrorKind::NotYetResolved,
            ))),
        }
    }

    fn set_error(&self, e: BalancerPoolErrorKind) {
        let mut locked = self.last_error.lock().unwrap();
        if let Some(replacement) = LoggedEvent::new(e).deduplicate(locked.as_mut()) {
            *locked = Some(replacement);
        }
    }

    fn reset_error(&self) {
        *self.last_error.lock().unwrap() = None;
    }

    pub(crate) fn set_connection_error(&self, e: String) {
        self.set_error(BalancerPoolErrorKind::ConnectionError(e))
    }

    pub(crate) fn is_critically_unhealthy(&self) -> bool {
        self.critically_unhealthy.load(Ordering::Acquire)
    }
}

#[cfg(test)]
pub(crate) struct PoolCommonTestInterface {
    pool: Arc<PoolCommon>,
}

#[cfg(test)]
impl PoolCommonTestInterface {
    pub(crate) fn new() -> Self {
        Self {
            pool: Arc::new(PoolCommon::new()),
        }
    }

    pub(crate) fn pool(&self) -> Arc<PoolCommon> {
        Arc::clone(&self.pool)
    }

    pub(crate) fn set_critically_unhealthy(&self, cuh: bool) {
        self.pool.critically_unhealthy.store(cuh, Ordering::Release)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PoolMemberKey(u64);

pub(crate) type DiscoverItem<LLS> = Result<Change<PoolMemberKey, LLS>, std::convert::Infallible>;

#[derive(Debug, PartialEq)]
pub(crate) enum ConnectionReport {
    Connected,
    Unhealthy(LoggedEvent<String>),
    Healthy,
    ConnectionError(LoggedEvent<String>),
}

#[derive(Debug)]
struct Subchannel {
    key: Option<PoolMemberKey>,
    address_index: AddressSlot,
    healthy: bool,
    ever_healthy: bool,
    connected: bool,
}

impl Subchannel {
    fn new(key: PoolMemberKey, address_index: AddressSlot) -> Self {
        Self {
            key: Some(key),
            address_index,
            healthy: false,
            ever_healthy: false,
            connected: false,
        }
    }
}

struct UnhealthyLoggingDetails {
    unhealthiness_start: tokio::time::Instant,
    next_log_time: tokio::time::Instant,
}

impl UnhealthyLoggingDetails {
    fn new(unhealthiness_start: tokio::time::Instant, log_delay: Duration) -> Self {
        Self {
            unhealthiness_start,
            next_log_time: unhealthiness_start + log_delay,
        }
    }
}

enum UnhealthyLoggingState {
    Happy,
    UnhappyNotYetLogged(UnhealthyLoggingDetails),
    UnhappyAlreadyLogged(UnhealthyLoggingDetails),
}

impl UnhealthyLoggingState {
    fn new_unhappy(config: &PoolConfig) -> Self {
        if let Some(delay) = config.log_unhealthy_initial_delay {
            Self::UnhappyNotYetLogged(UnhealthyLoggingDetails::new(
                tokio::time::Instant::now(),
                delay,
            ))
        } else {
            Self::Happy
        }
    }
}

struct PoolRegulator<A, B, HR, L>
where
    B: Backoff + Clone + std::fmt::Debug,
{
    common: Arc<PoolCommon>,
    serial: u64,
    addresses: ResolvedAddressCollection<A, B>,
    n_subchannels_have: usize,
    n_subchannels_healthy: usize,
    critically_unhealthy: bool,
    config: PoolConfig,
    healthy_callback: HR,
    unhealthy_logging_state: UnhealthyLoggingState,
    label: L,
}

enum AddOrWait<A> {
    Wait((bool, Either<std::future::Pending<()>, tokio::time::Sleep>)),
    Add(PoolMemberKey, Subchannel, A),
}

enum PoolRegulatorAction<DR> {
    NoAction,
    PleaseDrop(PoolMemberKey),
    PleaseDropUnhealthies,
    Diag(DR),
}

impl<A, B, HR, L> PoolRegulator<A, B, HR, L>
where
    A: std::hash::Hash + std::fmt::Debug + Eq + Send + Sync + Clone + 'static,
    B: Backoff + Clone + std::fmt::Debug,
    HR: Fn(bool),
    L: AsRef<str>,
{
    fn new(config: PoolConfig, backoff: B, healthy_callback: HR, label: L) -> Self {
        let uls = if config.n_subchannels_healthy_min == 0 {
            UnhealthyLoggingState::Happy
        } else {
            UnhealthyLoggingState::new_unhappy(&config)
        };
        #[cfg(feature = "metrics")]
        crate::metrics::n_channels_want(label.as_ref(), config.n_subchannels_want);
        Self {
            common: Arc::new(PoolCommon::new()),
            serial: 0,
            addresses: ResolvedAddressCollection::new(backoff),
            n_subchannels_have: 0,
            n_subchannels_healthy: 0,
            critically_unhealthy: false,
            config,
            healthy_callback,
            unhealthy_logging_state: uls,
            label,
        }
    }

    fn maybe_add(&mut self) -> AddOrWait<A> {
        if self.n_subchannels_have < self.config.n_subchannels_want {
            match self.addresses.choose() {
                ResolvedAddressChoice::NoneAvailable => {
                    AddOrWait::Wait((true, Either::Left(std::future::pending())))
                }
                ResolvedAddressChoice::Delay(moment) => {
                    AddOrWait::Wait((false, Either::Right(tokio::time::sleep_until(moment))))
                }
                ResolvedAddressChoice::Candidate(i) => {
                    self.n_subchannels_have += 1;
                    self.n_channels_inc();
                    self.update_metrics();
                    let key = PoolMemberKey(self.serial);
                    self.serial += 1;
                    let addr = self.addresses.add(i);
                    AddOrWait::Add(key, Subchannel::new(key, i), addr)
                }
            }
        } else {
            AddOrWait::Wait((false, Either::Left(std::future::pending())))
        }
    }

    fn dec_healthy(&mut self) {
        if self.n_subchannels_healthy == self.config.n_subchannels_healthy_min {
            self.unhealthy_logging_state = UnhealthyLoggingState::new_unhappy(&self.config);
            (self.healthy_callback)(false);
        }
        if self.n_subchannels_healthy == self.config.n_subchannels_healthy_critical_min {
            self.common
                .critically_unhealthy
                .store(true, Ordering::Release);
            self.critically_unhealthy = true;
        }
        self.n_subchannels_healthy -= 1;
        self.update_metrics();
    }

    fn inc_healthy(&mut self) -> bool {
        self.n_subchannels_healthy += 1;
        self.update_metrics();
        if self.n_subchannels_healthy == self.config.n_subchannels_healthy_min {
            (self.healthy_callback)(true);
            if matches!(
                self.unhealthy_logging_state,
                UnhealthyLoggingState::UnhappyAlreadyLogged(_)
            ) {
                log::info!(
                    "{}: Channel is healthy now ({}/{} healthy)",
                    self.label.as_ref(),
                    self.n_subchannels_healthy,
                    self.config.n_subchannels_want
                );
            }
            self.unhealthy_logging_state = UnhealthyLoggingState::Happy;
            self.common.reset_error();
        }
        if self.n_subchannels_healthy == self.config.n_subchannels_healthy_critical_min {
            self.common
                .critically_unhealthy
                .store(false, Ordering::Release);
            self.critically_unhealthy = false;
            true // okay to drop unhealthies now
        } else {
            false
        }
    }

    fn accept_connection_report<DR>(
        &mut self,
        s: &mut Subchannel,
        report: ConnectionReport,
    ) -> PoolRegulatorAction<DR> {
        match report {
            ConnectionReport::Healthy => {
                if !s.ever_healthy {
                    s.ever_healthy = true;
                    self.addresses.mark_success(s.address_index);
                }
                if !s.healthy {
                    s.healthy = true;
                    if self.inc_healthy() {
                        return PoolRegulatorAction::PleaseDropUnhealthies;
                    }
                }
            }
            ConnectionReport::Unhealthy(e) => {
                if s.healthy {
                    s.healthy = false;
                    self.dec_healthy();
                }
                self.addresses.log_error(s.address_index, e);
                if !self.critically_unhealthy {
                    if let Some(key) = s.key.take() {
                        return PoolRegulatorAction::PleaseDrop(key);
                    }
                }
            }
            ConnectionReport::ConnectionError(e) => {
                self.addresses.log_error(s.address_index, e);
            }
            ConnectionReport::Connected => {
                s.connected = true;
            }
        }
        PoolRegulatorAction::NoAction
    }

    fn accept_resolution_update<RSE: std::error::Error, DR>(
        &mut self,
        r: Option<Result<Vec<A>, RSE>>,
        distress: bool,
    ) -> PoolRegulatorAction<DR> {
        match r {
            Some(Ok(addrs)) => self.addresses.new_addresses(addrs),
            Some(Err(e)) => {
                if distress {
                    self.common.set_connection_error(e.to_string());
                } else {
                    log::warn!("{}: Resolution update: {}", self.label.as_ref(), e);
                }
            }
            None => {
                if distress {
                    self.common.set_error(BalancerPoolErrorKind::ResolverGaveUp);
                }
            }
        }
        PoolRegulatorAction::NoAction
    }

    fn accept_message<DR>(
        &mut self,
        m: InventoryReport<'_, Subchannel, ConnectionReport>,
    ) -> PoolRegulatorAction<DR> {
        match m {
            InventoryReport::Message(s, r) => self.accept_connection_report(s, r),
            InventoryReport::Dropped(s) => self.connection_dropped(s),
        }
    }

    fn log_unhealthy(&mut self) -> impl Future<Output = ()> + use<A, B, HR, L> {
        if self.n_subchannels_healthy >= self.config.n_subchannels_healthy_min {
            return Either::Left(std::future::pending());
        }
        let (still, d) = match self.unhealthy_logging_state {
            UnhealthyLoggingState::Happy => {
                // Logging not enabled
                return Either::Left(std::future::pending());
            }
            UnhealthyLoggingState::UnhappyNotYetLogged(ref d) => ("", d),
            UnhealthyLoggingState::UnhappyAlreadyLogged(ref d) => (" still", d),
        };
        let now = tokio::time::Instant::now();
        if d.next_log_time <= now {
            self.common
                .last_error
                .lock()
                .unwrap()
                .as_ref()
                .inspect(|e| {
                    log::warn!(
                        "{}: Channel{} not healthy after {} ({}/{} healthy); {}",
                        self.label.as_ref(),
                        still,
                        format_duration(now - d.unhealthiness_start),
                        self.n_subchannels_healthy,
                        self.config.n_subchannels_want,
                        e
                    );
                });
            let Some(repeat_delay) = self.config.log_unhealthy_repeat_delay else {
                self.unhealthy_logging_state = UnhealthyLoggingState::Happy;
                return Either::Left(std::future::pending());
            };
            let nlt = now + repeat_delay;
            let sleep = tokio::time::sleep_until(nlt);
            let ns = UnhealthyLoggingState::UnhappyAlreadyLogged(UnhealthyLoggingDetails {
                unhealthiness_start: d.unhealthiness_start,
                next_log_time: nlt,
            });
            self.unhealthy_logging_state = ns;
            Either::Right(sleep)
        } else {
            Either::Right(tokio::time::sleep_until(d.next_log_time))
        }
    }

    fn connection_dropped<DR>(&mut self, s: Subchannel) -> PoolRegulatorAction<DR> {
        self.n_subchannels_have -= 1;
        if s.healthy {
            self.dec_healthy();
        }
        self.addresses.removed(s.address_index);
        self.update_metrics();
        PoolRegulatorAction::NoAction
    }

    fn update_metrics(&self) {
        #[cfg(feature = "metrics")]
        crate::metrics::n_channels_update(
            self.label.as_ref(),
            self.n_subchannels_have,
            self.n_subchannels_healthy,
        );
    }

    fn n_channels_inc(&self) {
        #[cfg(feature = "metrics")]
        crate::metrics::n_channels_inc(self.label.as_ref());
    }
}

#[cfg(not(feature = "diag"))]
mod no_diag {
    use std::task::{Context, Poll};

    pub(super) struct NoDiagReports;

    impl std::future::Future for NoDiagReports {
        type Output = ();

        fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    impl futures::future::FusedFuture for NoDiagReports {
        fn is_terminated(&self) -> bool {
            true
        }
    }
}

#[cfg(feature = "diag")]
fn make_diag_report<A, B, HR, L, R>(
    id: usize,
    want_full: bool,
    regulator: &PoolRegulator<A, B, HR, L>,
    subchannels: &Inventory<Subchannel, R>,
) -> Box<crate::diag::Report>
where
    A: std::hash::Hash + std::fmt::Debug + Eq + Clone,
    B: Backoff + Clone + std::fmt::Debug,
    L: AsRef<str>,
{
    crate::diag::Report {
        id,
        label: regulator.label.as_ref().to_owned(),
        more: if want_full {
            Some(crate::diag::FullReport {
                resolved_addresses: regulator.addresses.diag_list().collect(),
                subchannels: subchannels
                    .iter()
                    .map(|s| {
                        format!(
                            "{} to {:?}, {}",
                            if s.connected {
                                "Connected"
                            } else {
                                "Connecting"
                            },
                            regulator.addresses.diag_get_address(s.address_index),
                            if s.healthy {
                                "healthy"
                            } else if s.ever_healthy {
                                "unhealthy"
                            } else if s.connected {
                                "never healthy"
                            } else {
                                "connection in progress"
                            }
                        )
                    })
                    .collect(),
                n_subchannels_want: regulator.config.n_subchannels_want,
            })
        } else {
            None
        },
    }
    .into()
}

pub(crate) fn balancer_pool<A, B, HR, M, RS, RSE, L>(
    mut config: PoolConfig,
    label: L,
    connection_maker: M,
    backoff: B,
    healthy_callback: HR,
    resolv: RS,
) -> impl Stream<Item = DiscoverItem<M::Connection>>
where
    A: std::hash::Hash + std::fmt::Debug + Eq + Send + Sync + Clone + 'static,
    B: Backoff + Clone + std::fmt::Debug,
    HR: Fn(bool),
    M: PoolMemberMaker<A>,
    RS: Stream<Item = Result<Vec<A>, RSE>>,
    RSE: std::error::Error,
    L: AsRef<str>,
{
    if config.n_subchannels_want > config.n_subchannels_max {
        config.n_subchannels_want = config.n_subchannels_max;
    }
    let mut subchannels = Inventory::new(config.n_subchannels_max);
    let mut regulator = PoolRegulator::new(config, backoff, healthy_callback, label);
    #[cfg(feature = "diag")]
    let mut diag_root = crate::diag::add_channel();

    stream! {
        let mut resolv = pin!(resolv.fuse());
        let mut first_connect = true;
        #[cfg(feature = "diag")]
        let mut report_requested = diag_root.wait_for_report_requested();
        #[cfg(not(feature = "diag"))]
        let mut report_requested = no_diag::NoDiagReports;
        loop {
            let (distress, add_wait) = match regulator.maybe_add() {
                AddOrWait::Wait(r) => r,
                AddOrWait::Add(key, subchannel, addr) => {
                    if first_connect {
                        first_connect = false;
                        regulator.common.set_error(BalancerPoolErrorKind::NotYetConnected);
                    }
                    let reporter_fut = subchannels.allocate(subchannel);
                    let conn = connection_maker.make_connection(Arc::clone(&regulator.common), reporter_fut, addr);
                    yield Ok(Change::Insert(key, conn));
                    continue;
                }
            };
            let action = {
                let mut add_wait = pin!(add_wait.fuse());
                let mut reporter = pin!(subchannels.recv().fuse());
                let mut log_unhealthy = pin!(regulator.log_unhealthy().fuse());
                select! {
                    _ = add_wait => PoolRegulatorAction::NoAction,
                    _ = log_unhealthy => PoolRegulatorAction::NoAction,
                    r = resolv.next() => regulator.accept_resolution_update(r, distress),
                    report = reporter => regulator.accept_message(report),
                    diag_report = report_requested => PoolRegulatorAction::Diag(diag_report),
                }
            };
            match action {
                PoolRegulatorAction::NoAction => (),
                PoolRegulatorAction::PleaseDrop(k) => {
                    yield Ok(Change::Remove(k));
                }
                PoolRegulatorAction::PleaseDropUnhealthies => {
                    for s in subchannels.iter_mut() {
                        if s.ever_healthy && !s.healthy {
                            if let Some(k) = s.key.take() {
                                yield Ok(Change::Remove(k));
                            }
                        }
                    }
                }
                #[cfg(feature = "diag")]
                PoolRegulatorAction::Diag(diag_report) => {
                    let (unique, want_full) = (diag_report.unique(), diag_report.want_full());
                    diag_report.send(make_diag_report(unique, want_full, &regulator, &subchannels));
                    report_requested = diag_root.wait_for_report_requested();
                }
                #[cfg(not(feature = "diag"))]
                PoolRegulatorAction::Diag(_) => (),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::poll;
    use futures::stream::BoxStream;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Poll};
    use tower_service::Service;

    use super::*;

    use crate::report::Reporter;

    const TEST_CONFIG: PoolConfig = PoolConfig {
        n_subchannels_max: 3,
        n_subchannels_want: 3,
        n_subchannels_healthy_min: 2,
        n_subchannels_healthy_critical_min: 1,
        log_unhealthy_initial_delay: Some(Duration::from_millis(10000)),
        log_unhealthy_repeat_delay: Some(Duration::from_millis(90000)),
    };

    struct TestConnection {
        reporter_fut: Pin<Box<dyn Future<Output = Reporter<ConnectionReport>> + Send>>,
        common: Option<Arc<PoolCommon>>,
    }

    impl std::fmt::Debug for TestConnection {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            write!(f, "TestConnection(...)")
        }
    }

    impl<T> Service<T> for TestConnection {
        type Response = ();
        type Error = ();
        type Future = std::future::Ready<Result<(), ()>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: T) -> Self::Future {
            std::future::ready(Ok(()))
        }
    }

    struct TestMaker;

    impl PoolMemberMaker<usize> for TestMaker {
        type ReqBody = ();
        type Connection = TestConnection;

        fn make_connection<F>(
            &self,
            common: Arc<PoolCommon>,
            reporter_fut: F,
            _: usize,
        ) -> Self::Connection
        where
            F: Future<Output = Reporter<ConnectionReport>> + Send + 'static,
        {
            TestConnection {
                reporter_fut: Box::pin(reporter_fut),
                common: Some(common),
            }
        }
    }

    struct TestPool {
        common: Option<Arc<PoolCommon>>,
        health: Arc<AtomicUsize>,
        discover: BoxStream<'static, DiscoverItem<TestConnection>>,
    }

    impl TestPool {
        fn new<RS, RSE>(resolv: RS) -> Self
        where
            RS: Stream<Item = Result<Vec<usize>, RSE>> + Send + 'static,
            RSE: std::error::Error + Send + 'static,
        {
            let health = Arc::new(AtomicUsize::default());
            let health2 = Arc::clone(&health);
            let discover = Box::pin(balancer_pool(
                TEST_CONFIG.clone(),
                "TestPool",
                TestMaker,
                backoff::ExponentialBackoff::default(),
                move |h| health2.store(if h { 2 } else { 1 }, Ordering::Release),
                resolv,
            ));
            Self {
                common: None,
                health,
                discover,
            }
        }

        fn healthy(&self) -> Option<bool> {
            match self.health.load(Ordering::Acquire) {
                2 => Some(true),
                1 => Some(false),
                _ => None,
            }
        }

        async fn expect_connection(&mut self) -> (PoolMemberKey, Reporter<ConnectionReport>) {
            match poll!(self.discover.next()) {
                Poll::Ready(Some(Ok(Change::Insert(key, mut connection)))) => {
                    self.common = connection.common.take();
                    (key, connection.reporter_fut.await)
                }
                x => panic!("unexpected message {:?}", x),
            }
        }

        async fn expect_drop(&mut self, want: PoolMemberKey) {
            match poll!(self.discover.next()) {
                Poll::Ready(Some(Ok(Change::Remove(got)))) => assert_eq!(got, want),
                x => panic!("unexpected message {:?}", x),
            }
        }

        fn check_connection_error(&self, want: &str, count: usize) {
            let common = self.common.as_ref().unwrap();
            let locked = common.last_error.lock().unwrap();
            match *locked {
                Some(ref bpe) => {
                    if count == 1 {
                        assert_matches!(bpe.repeats, None);
                    } else {
                        assert_eq!(bpe.repeats.unwrap().1, count);
                    }
                    match bpe.e {
                        BalancerPoolErrorKind::ConnectionError(ref got) => assert_eq!(got, want),
                        ref x => panic!("Unexpected error {:?}", x),
                    }
                }
                ref x => panic!("Unexpected error {:?}", x),
            }
        }
    }

    fn n_addresses(n: usize) -> impl Stream<Item = Result<Vec<usize>, std::convert::Infallible>> {
        futures::stream::once(futures::future::ready(Ok(std::iter::successors(
            Some(0),
            |i| Some(i + 1),
        )
        .take(n)
        .collect())))
    }

    #[tokio::test(start_paused = true)]
    async fn no_addresses_yet() {
        testing_logger::setup();
        let mut t = TestPool::new(futures::stream::pending::<
            Result<_, std::convert::Infallible>,
        >());
        assert!(poll!(t.discover.next()).is_pending());
        tokio::time::advance(TEST_CONFIG.log_unhealthy_initial_delay.unwrap()).await;
        assert!(poll!(t.discover.next()).is_pending());
        testing_logger::validate(|captured_logs| {
            for l in captured_logs {
                if l.level != log::Level::Warn {
                    continue;
                }
                if l.body.contains("name not resolved yet") {
                    return;
                }
            }
            panic!("did not find expected NotYetResolved error in log");
        });
        assert!(t.healthy().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn no_addresses_at_all() {
        testing_logger::setup();
        let mut t = TestPool::new(futures::stream::empty::<Result<_, std::convert::Infallible>>());
        assert!(poll!(t.discover.next()).is_pending());
        tokio::time::advance(TEST_CONFIG.log_unhealthy_initial_delay.unwrap()).await;
        assert!(poll!(t.discover.next()).is_pending());
        testing_logger::validate(|captured_logs| {
            for l in captured_logs {
                if l.level != log::Level::Warn {
                    continue;
                }
                if l.body.contains("gave up with no address resolution") {
                    return;
                }
            }
            panic!("did not find expected ResolverGaveUp error in log");
        });
        assert!(t.healthy().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn no_addresses_at_all_never_log() {
        testing_logger::setup();
        let mut config = TEST_CONFIG.clone();
        config.log_unhealthy_initial_delay = None;
        let mut discover = pin!(balancer_pool(
            config,
            "no_addresses_at_all_never_log",
            TestMaker,
            backoff::ExponentialBackoff::default(),
            |_| (),
            futures::stream::empty::<Result<_, std::convert::Infallible>>(),
        ));
        assert!(poll!(discover.next()).is_pending());
        tokio::time::advance(Duration::from_secs(999999)).await;
        assert!(poll!(discover.next()).is_pending());
        testing_logger::validate(|captured_logs| {
            for l in captured_logs {
                if l.body.contains("not healthy after") {
                    panic!("logged unhealthy but should not");
                }
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn add_one_at_a_time() {
        let mut t = TestPool::new(n_addresses(1));

        let (_k1, mut c1) = t.expect_connection().await;
        assert_matches!(
            *t.common.as_ref().unwrap().last_error.lock().unwrap(),
            Some(LoggedEvent {
                e: BalancerPoolErrorKind::NotYetConnected,
                repeats: None,
                ..
            })
        );
        assert!(t.healthy().is_none());
        // We are only critically unhealthy if we drop below the threshold
        // after having been above it.
        assert_eq!(
            t.common
                .as_ref()
                .unwrap()
                .critically_unhealthy
                .load(Ordering::Acquire),
            false
        );
        // Won't make another connection to the same address until the outcome
        // of the first is known.
        assert!(poll!(t.discover.next()).is_pending());
        c1.send(ConnectionReport::Healthy).await;

        let (_k2, mut c2) = t.expect_connection().await;
        assert!(t.healthy().is_none()); // Need 2
        assert_matches!(
            *t.common.as_ref().unwrap().last_error.lock().unwrap(),
            Some(LoggedEvent {
                repeats: None,
                e: BalancerPoolErrorKind::NotYetConnected,
                ..
            })
        );
        assert_eq!(
            t.common
                .as_ref()
                .unwrap()
                .critically_unhealthy
                .load(Ordering::Acquire),
            false
        );
        assert!(poll!(t.discover.next()).is_pending());
        c2.send(ConnectionReport::Healthy).await;

        let (_k3, mut c3) = t.expect_connection().await;

        assert_eq!(
            t.healthy()
                .expect("2 healthy subchannels, should be healthy"),
            true
        );
        assert!(
            t.common
                .as_ref()
                .unwrap()
                .last_error
                .lock()
                .unwrap()
                .is_none()
        );
        c3.send(ConnectionReport::Healthy).await;
        // No more, we already have the wanted number.
        assert!(poll!(t.discover.next()).is_pending());

        std::mem::drop(c2);
        // A dropped connection is immediately replaced.
        let _ = t.expect_connection().await;
    }

    #[tokio::test(start_paused = true)]
    async fn add_all_at_once() {
        let mut t = TestPool::new(n_addresses(3));
        // With enough addresses, we add connections fast.
        let (_k1, _c1) = t.expect_connection().await;
        let (_k2, _c2) = t.expect_connection().await;
        let (_k3, _c3) = t.expect_connection().await;
    }

    #[tokio::test(start_paused = true)]
    async fn one_unhealthy() {
        let mut t = TestPool::new(n_addresses(3));
        let (_k1, mut c1) = t.expect_connection().await;
        let (_k2, mut c2) = t.expect_connection().await;
        let (k3, mut c3) = t.expect_connection().await;
        c1.send(ConnectionReport::Healthy).await;
        c2.send(ConnectionReport::Healthy).await;
        c3.send(ConnectionReport::Unhealthy(LoggedEvent::new("no".into())))
            .await;
        t.expect_drop(k3).await;
        std::mem::drop(c3);
        let (_k4, _c4) = t.expect_connection().await;
        assert_eq!(
            t.healthy()
                .expect("2 healthy subchannels, should be healthy"),
            true
        );
        assert_eq!(
            t.common
                .as_ref()
                .unwrap()
                .critically_unhealthy
                .load(Ordering::Acquire),
            false
        );
        assert!(poll!(t.discover.next()).is_pending());
    }

    #[tokio::test(start_paused = true)]
    async fn two_unhealthy() {
        let mut t = TestPool::new(n_addresses(4));
        let (_k1, mut c1) = t.expect_connection().await;
        let (k2, mut c2) = t.expect_connection().await;
        let (k3, mut c3) = t.expect_connection().await;
        c1.send(ConnectionReport::Healthy).await;
        c2.send(ConnectionReport::Unhealthy(LoggedEvent::new("no".into())))
            .await;
        c3.send(ConnectionReport::Unhealthy(LoggedEvent::new("no".into())))
            .await;
        t.expect_drop(k2).await;
        std::mem::drop(c2);
        t.expect_drop(k3).await;
        std::mem::drop(c3);
        assert!(t.healthy().is_none()); // 1 healthy is not enough
        let (_k4, _c4) = t.expect_connection().await;
        let (_k5, _c5) = t.expect_connection().await;
        assert_eq!(
            t.common
                .as_ref()
                .unwrap()
                .critically_unhealthy
                .load(Ordering::Acquire),
            false
        );
        assert!(poll!(t.discover.next()).is_pending());
    }

    #[tokio::test(start_paused = true)]
    async fn all_initially_unhealthy() {
        let mut t = TestPool::new(n_addresses(6));
        let (k1, mut c1) = t.expect_connection().await;
        let (k2, mut c2) = t.expect_connection().await;
        let (k3, mut c3) = t.expect_connection().await;
        c1.send(ConnectionReport::Unhealthy(LoggedEvent::new("no".into())))
            .await;
        c2.send(ConnectionReport::Unhealthy(LoggedEvent::new("no".into())))
            .await;
        c3.send(ConnectionReport::Unhealthy(LoggedEvent::new("no".into())))
            .await;
        // Critical unhealthy mode does not engage unless we have been
        // once above the threshold.
        t.expect_drop(k1).await;
        std::mem::drop(c1);
        t.expect_drop(k2).await;
        std::mem::drop(c2);
        t.expect_drop(k3).await;
        std::mem::drop(c3);
        assert!(t.healthy().is_none());
        assert_eq!(
            t.common
                .as_ref()
                .unwrap()
                .critically_unhealthy
                .load(Ordering::Acquire),
            false
        );
        let (_k4, _c4) = t.expect_connection().await;
        let (_k5, _c5) = t.expect_connection().await;
        let (_k6, _c6) = t.expect_connection().await;
        assert!(poll!(t.discover.next()).is_pending());
    }

    #[tokio::test(start_paused = true)]
    async fn all_unhealthy_later() {
        let mut t = TestPool::new(n_addresses(6));
        let (k1, mut c1) = t.expect_connection().await;
        let (k2, mut c2) = t.expect_connection().await;
        let (k3, mut c3) = t.expect_connection().await;
        c1.send(ConnectionReport::Healthy).await;
        c2.send(ConnectionReport::Healthy).await;
        c3.send(ConnectionReport::Healthy).await;
        // Nothing more to do for now.

        // First becomes unhealthy. Drop and replace.
        c1.send(ConnectionReport::Unhealthy(LoggedEvent::new("no".into())))
            .await;
        t.expect_drop(k1).await;
        assert_eq!(t.healthy().expect("still healthy"), true);
        assert_eq!(
            t.common
                .as_ref()
                .unwrap()
                .critically_unhealthy
                .load(Ordering::Acquire),
            false
        );
        std::mem::drop(c1);
        let (_k4, _c4) = t.expect_connection().await;

        // Second becomes unhealthy. Drop and replace.
        c2.send(ConnectionReport::Unhealthy(LoggedEvent::new("no".into())))
            .await;
        t.expect_drop(k2).await;
        assert_eq!(t.healthy().expect("not healthy anymore"), false);
        assert_eq!(
            t.common
                .as_ref()
                .unwrap()
                .critically_unhealthy
                .load(Ordering::Acquire),
            false
        );
        std::mem::drop(c2);
        let (_k5, mut c5) = t.expect_connection().await;

        // Third becomes unhealthy. Maintain.
        c3.send(ConnectionReport::Unhealthy(LoggedEvent::new("no".into())))
            .await;
        assert!(poll!(t.discover.next()).is_pending());
        assert_eq!(t.healthy().expect("not healthy anymore"), false);
        assert_eq!(
            t.common
                .as_ref()
                .unwrap()
                .critically_unhealthy
                .load(Ordering::Acquire),
            true
        );

        // One of the new connections gets healthy.
        c5.send(ConnectionReport::Healthy).await;
        // Now we are out of critically unhealthy mode and can drop the unhealthy.
        t.expect_drop(k3).await;
    }

    #[tokio::test(start_paused = true)]
    async fn drop_healthy() {
        let mut t = TestPool::new(n_addresses(3));
        let (_k1, mut c1) = t.expect_connection().await;
        let (_k2, mut c2) = t.expect_connection().await;
        let (_k3, _c3) = t.expect_connection().await;
        c1.send(ConnectionReport::Healthy).await;
        c2.send(ConnectionReport::Healthy).await;
        assert_matches!(poll!(t.discover.next()), Poll::Pending);
        assert_eq!(t.healthy().expect("healthy"), true);

        // We react to connections being dropped even if we didn't ask for it.
        std::mem::drop(c1);
        let (_k4, _c4) = t.expect_connection().await;
        assert_eq!(t.healthy().expect("unhealthy"), false);
        assert!(poll!(t.discover.next()).is_pending());
    }

    #[tokio::test(start_paused = true)]
    async fn set_connection_error() {
        let mut t = TestPool::new(n_addresses(1));
        let (_k1, _c1) = t.expect_connection().await;
        // Called when a subchannel encounters an error during use (as a Service).
        t.common
            .as_ref()
            .unwrap()
            .set_connection_error(String::from("foo"));
        t.check_connection_error("foo", 1);
        t.common
            .as_ref()
            .unwrap()
            .set_connection_error(String::from("foo"));
        t.check_connection_error("foo", 2);
        t.common
            .as_ref()
            .unwrap()
            .set_connection_error(String::from("bar"));
        t.check_connection_error("bar", 1);
    }
}
