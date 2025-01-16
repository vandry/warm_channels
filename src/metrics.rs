use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_gauge_vec};

lazy_static! {
    static ref N_CHANNELS_WANT: prometheus::GaugeVec = register_gauge_vec!(
        "client_channels_target",
        "Desired number of client channels",
        &["client_name"]
    )
    .unwrap();
    static ref N_CHANNELS_CURRENT: prometheus::GaugeVec = register_gauge_vec!(
        "client_channels_current",
        "Number of client channels currently connected",
        &["client_name"]
    )
    .unwrap();
    static ref N_CHANNELS_HEALTHY: prometheus::GaugeVec = register_gauge_vec!(
        "client_channels_healthy",
        "Number of client channels currently healthy",
        &["client_name"]
    )
    .unwrap();
    static ref N_CHANNELS_OPENED: prometheus::CounterVec = register_counter_vec!(
        "client_channels_opened",
        "Total counter of client channels opened",
        &["client_name"]
    )
    .unwrap();
}

pub(crate) fn n_channels_want(name: &str, n: usize) {
    N_CHANNELS_WANT.with_label_values(&[&name]).set(n as f64);
}

pub(crate) fn n_channels_update(name: &str, total: usize, healthy: usize) {
    N_CHANNELS_CURRENT
        .with_label_values(&[&name])
        .set(total as f64);
    N_CHANNELS_HEALTHY
        .with_label_values(&[&name])
        .set(healthy as f64);
}

pub(crate) fn n_channels_inc(name: &str) {
    N_CHANNELS_OPENED.with_label_values(&[&name]).inc();
}
