extern crate submillisecond;

use crate::metrics::{MetricsProcess, MetricsProcessHandler};
use submillisecond::Application;

fn gather_metrics() -> Vec<u8> {
    let metrics_process = MetricsProcess::get_process();
    metrics_process.gather()
}

pub fn start_server() {
    Application::new(submillisecond::router! {
        GET "/metrics" => gather_metrics
    });
}
