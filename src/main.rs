use lunatic::{net::TcpListener, process::StartProcess, ProcessConfig};
use lunatic_log::subscriber::fmt::FmtSubscriber;
use lunatic_log::LevelFilter;
use mqtt_broker::client::ClientProcess;
use mqtt_broker::coordinator::CoordinatorSup;
use mqtt_broker::metrics::MetricsSup;
// use mqtt_broker::metrics_server;
use mqtt_broker::{metrics_server, worker};

fn main() {
    lunatic_log::init(FmtSubscriber::new(LevelFilter::Info).pretty());
    // Create a coordinator supervisor and register the coordinator under the "coordinator" name.
    MetricsSup::start_link("metrics".to_owned(), None);
    CoordinatorSup::start_link("coordinator".to_owned(), None);

    // start single worker
    worker::worker_process();

    let port = 1883;
    lunatic_log::info!("Started server on port {}", port);
    let address = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(address).unwrap();

    // start http endpoint
    metrics_server::start_server();

    // Limit client's memory usage to 5 Mb & allow sub-processes.
    let mut client_conf = ProcessConfig::new().expect("create process config");
    client_conf.set_max_memory(5_000_000);
    client_conf.set_can_spawn_processes(true);

    while let Ok((stream, _)) = listener.accept() {
        ClientProcess::start_config(stream, None, &client_conf);
    }
}
