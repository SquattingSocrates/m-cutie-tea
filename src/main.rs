// use lunatic::{net, process, Config, Environment};
use lunatic::{net::TcpListener, process::StartProcess, Mailbox, ProcessConfig};
// use std::fs::File;
// use std::io::prelude::*;
// use std::path::Path;

// use mqtt_broker::queue::{broker, tcp_reader};
// use mqtt_broker::broker;
use mqtt_broker::client::ClientProcess;
use mqtt_broker::coordinator::CoordinatorSup;
use mqtt_broker::worker;

// fn write_to_file() {
//     // Create a path to the desired file
//     let path = Path::new("hello.txt");
//     let display = path.display();

//     // Open the path in read-only mode, returns `io::Result<File>`
//     let mut file = match File::open(&path) {
//         Err(why) => panic!("couldn't open {}: {}", display, why),
//         Ok(file) => file,
//     };

//     // Read the file contents into a string, returns `io::Result<usize>`
//     let mut s = String::new();
//     match file.read_to_string(&mut s) {
//         Err(why) => panic!("couldn't read {}: {}", display, why),
//         Ok(_) => print!("{} contains:\n{}", display, s),
//     }

//     // `file` goes out of scope, and the "hello.txt" file gets closed
// }

fn main() {
    // let mut client_conf = Config::new(5_000_000, None);
    // client_conf.allow_namespace("lunatic::");
    // client_conf.allow_namespace("wasi_snapshot_preview1::fd_write");
    // client_conf.allow_namespace("wasi_snapshot_preview1::clock_time_get");
    // client_conf.allow_namespace("wasi_snapshot_preview1::random_get");
    // let mut client_env = Environment::new(client_conf).unwrap();
    // let client_module = client_env.add_this_module().unwrap();

    // Create a coordinator supervisor and register the coordinator under the "coordinator" name.
    CoordinatorSup::start_link("coordinator".to_owned(), None);

    // start single worker
    worker::worker_process();

    let port = 1883;
    println!("Started server on port {}", port);
    let address = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(address).unwrap();

    // Limit client's memory usage to 5 Mb & allow sub-processes.
    let mut client_conf = ProcessConfig::new();
    client_conf.set_max_memory(5_000_000);
    client_conf.set_can_spawn_processes(true);

    while let Ok((stream, _)) = listener.accept() {
        ClientProcess::start_config(stream, None, &client_conf);
    }

    // // Create a broker and register it inside the environment
    // let broker = process::spawn(broker::broker_process).unwrap();
    // client_env.register("broker", "1.0.0", broker).unwrap();

    // let port = 1883;
    // println!("Started server on port {}", port);
    // let address = format!("127.0.0.1:{}", port);

    // let listener = net::TcpListener::bind(address).unwrap();
    // while let Ok((stream, _)) = listener.accept() {
    //     client_module
    //         .spawn_with(stream, session::connect_client)
    //         .unwrap();
    // }
}
