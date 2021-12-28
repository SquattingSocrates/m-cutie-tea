use lunatic::{net, process, Config, Environment};

use mqtt_broker::queue::{broker, mqtt_queue_proc};

fn main() {
    let mut client_conf = Config::new(5_000_000, None);
    client_conf.allow_namespace("lunatic::");
    client_conf.allow_namespace("wasi_snapshot_preview1::fd_write");
    client_conf.allow_namespace("wasi_snapshot_preview1::clock_time_get");
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
            .spawn_with(stream, mqtt_queue_proc::handle_queue)
            .unwrap();
    }
}
