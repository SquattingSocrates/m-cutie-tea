use lunatic::{
    net,
    process::{self, Process},
    Config, Environment,
};

// use mqtt_broker::queue::{broker, tcp_reader};
use mqtt_broker::broker;
use mqtt_broker::queue::tcp_reader;

struct ClientProcess {
    client_id: String,
    process: Process<Option<net::TcpStream>>,
}

fn main() {
    let mut client_conf = Config::new(5_000_000, None);
    client_conf.allow_namespace("lunatic::");
    client_conf.allow_namespace("wasi_snapshot_preview1::fd_write");
    client_conf.allow_namespace("wasi_snapshot_preview1::clock_time_get");
    client_conf.allow_namespace("wasi_snapshot_preview1::random_get");
    let mut client_env = Environment::new(client_conf).unwrap();
    let client_module = client_env.add_this_module().unwrap();

    // Create a broker and register it inside the environment
    let broker = process::spawn(broker::broker_process).unwrap();
    client_env.register("broker", "1.0.0", broker).unwrap();

    let port = 1883;
    println!("Started server on port {}", port);
    let address = format!("127.0.0.1:{}", port);

    let listener = net::TcpListener::bind(address).unwrap();
    while let Ok((stream, _)) = listener.accept() {
        client_module
            .spawn_with(stream, tcp_reader::handle_tcp)
            .unwrap();
    }
}
