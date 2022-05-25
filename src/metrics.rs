use lunatic::{
    host,
    process::{AbstractProcess, ProcessMessage, ProcessRef, ProcessRequest},
    supervisor::Supervisor,
};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};
use serde::{Deserialize, Serialize};

/// The `MetricsSup` is supervising one global instance of the `MetricsProcess`.
pub struct MetricsSup;
impl Supervisor for MetricsSup {
    type Arg = String;
    type Children = MetricsProcess;

    fn init(config: &mut lunatic::supervisor::SupervisorConfig<Self>, name: Self::Arg) {
        // Always register the `MetricsProcess` under the name passed to the supervisor.
        config.children_args(((), Some(name)))
    }
}

pub struct MetricsProcess {
    registry: Registry,
    connected_clients: IntGauge,
    received_packets: IntCounter,
    active_queues: IntGauge,
    qos0_delivery_time: Histogram,
    qos1_delivery_time: Histogram,
    qos2_delivery_time: Histogram,
}

impl AbstractProcess for MetricsProcess {
    type Arg = ();
    type State = Self;

    fn init(_: ProcessRef<Self>, _: Self::Arg) -> Self::State {
        // Coordinator shouldn't die when a client dies. This makes the link one-directional.
        unsafe { host::api::process::die_when_link_dies(0) };

        let res = MetricsProcess {
            registry: Registry::new(),
            connected_clients: IntGauge::new("connected_clients", "active connected clients")
                .unwrap(),
            received_packets: IntCounter::new("received_packets", "counts total packets").unwrap(),
            active_queues: IntGauge::new("active_queues", "count of active queues").unwrap(),
            qos0_delivery_time: Histogram::with_opts(HistogramOpts::new(
                "qos0_delivery_time",
                "histogram of delivery times for qos0",
            ))
            .unwrap(),
            qos1_delivery_time: Histogram::with_opts(HistogramOpts::new(
                "qos1_delivery_time",
                "histogram of delivery times for qos1",
            ))
            .unwrap(),
            qos2_delivery_time: Histogram::with_opts(HistogramOpts::new(
                "qos2_delivery_time",
                "histogram of delivery times for qos2",
            ))
            .unwrap(),
        };

        res.registry
            .register(Box::new(res.connected_clients.clone()))
            .unwrap();
        res.registry
            .register(Box::new(res.received_packets.clone()))
            .unwrap();
        res.registry
            .register(Box::new(res.active_queues.clone()))
            .unwrap();
        res.registry
            .register(Box::new(res.qos0_delivery_time.clone()))
            .unwrap();
        res.registry
            .register(Box::new(res.qos1_delivery_time.clone()))
            .unwrap();
        res.registry
            .register(Box::new(res.qos2_delivery_time.clone()))
            .unwrap();

        res
    }
}

#[derive(Serialize, Deserialize)]
pub struct Connect;
impl ProcessMessage<Connect> for MetricsProcess {
    fn handle(state: &mut Self::State, _: Connect) {
        state.connected_clients.inc();
    }
}

#[derive(Serialize, Deserialize)]
pub struct Disconnect;
impl ProcessMessage<Disconnect> for MetricsProcess {
    fn handle(state: &mut Self::State, _: Disconnect) {
        state.connected_clients.dec();
    }
}

#[derive(Serialize, Deserialize)]
pub struct Packet;
impl ProcessMessage<Packet> for MetricsProcess {
    fn handle(state: &mut Self::State, _: Packet) {
        state.received_packets.inc();
    }
}

#[derive(Serialize, Deserialize)]
pub enum ActiveQueue {
    Create,
    Remove,
}
impl ProcessMessage<ActiveQueue> for MetricsProcess {
    fn handle(state: &mut Self::State, q: ActiveQueue) {
        match q {
            ActiveQueue::Create => state.active_queues.inc(),
            ActiveQueue::Remove => state.active_queues.dec(),
        }
    }
}

#[derive(Serialize, Deserialize)]
/// Consists of QoS and delivery time in ms
pub struct DeliveryTime(pub u8, pub f64);
impl ProcessMessage<DeliveryTime> for MetricsProcess {
    fn handle(state: &mut Self::State, DeliveryTime(qos, duration_ms): DeliveryTime) {
        match qos {
            0 => state.qos0_delivery_time.observe(duration_ms),
            1 => state.qos1_delivery_time.observe(duration_ms),
            2 => state.qos2_delivery_time.observe(duration_ms),
            _ => eprintln!(
                "Received invalid qos value for metrics. QoS: {} | Duration(ms): {}",
                qos, duration_ms
            ),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Gather;
impl ProcessRequest<Gather> for MetricsProcess {
    type Response = Vec<u8>;

    fn handle(state: &mut Self::State, _: Gather) -> Vec<u8> {
        // Gather the metrics.
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = state.registry.gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        buffer
    }
}
