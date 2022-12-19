use crate::metrics::MetricsProcess;
use submillisecond::{router, Application};

fn gather_metrics() -> Vec<u8> {}

pub fn start_server() {
    Application::new(router! {
        GET "/metrics" => gather_metrics
    })
}
