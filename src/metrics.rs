use lunatic::{
    abstract_process, host,
    process::{AbstractProcess, ProcessRef},
    supervisor::Supervisor,
    Process, Tag,
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

#[abstract_process(visibility = pub)]
impl MetricsProcess {
    /// function that retrieves the running metrics process
    pub fn get_process() -> ProcessRef<MetricsProcess> {
        let metrics_process = ProcessRef::<MetricsProcess>::lookup("metrics").unwrap();
        metrics_process
    }

    #[init]
    fn init(_: ProcessRef<Self>, _: ()) -> Self {
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

    #[terminate]
    fn terminate(self) {
        println!("Shutdown process");
    }

    #[handle_link_trapped]
    fn handle_link_trapped(&self, tag: Tag) {
        println!("Link trapped");
    }

    // =======================
    // Message handlers
    // =======================
    #[handle_message]
    pub fn track_connect(&mut self) {
        self.connected_clients.inc();
    }

    #[handle_message]
    pub fn track_disconnect(&mut self) {
        self.connected_clients.dec();
    }

    #[handle_message]
    pub fn track_packet(&mut self) {
        self.received_packets.inc();
    }

    #[handle_message]
    pub fn track_active_queue(&mut self, q: ActiveQueue) {
        match q {
            ActiveQueue::Create => self.active_queues.inc(),
            ActiveQueue::Remove => self.active_queues.dec(),
        }
    }

    #[handle_message]
    pub fn track_delivery_time(&mut self, qos: u8, duration_ms: f64) {
        match qos {
            0 => self.qos0_delivery_time.observe(duration_ms),
            1 => self.qos1_delivery_time.observe(duration_ms),
            2 => self.qos2_delivery_time.observe(duration_ms),
            _ => eprintln!(
                "Received invalid qos value for metrics. QoS: {} | Duration(ms): {}",
                qos, duration_ms
            ),
        }
    }

    // =======================
    // Request handlers
    // =======================
    /// gather all metrics from the metrics process in prometheus format
    #[handle_request]
    pub fn gather(&self) -> Vec<u8> {
        // Gather the metrics.
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        buffer
    }
}

#[derive(Serialize, Deserialize)]
pub enum ActiveQueue {
    Create,
    Remove,
}
#[derive(Serialize, Deserialize)]
/// Consists of QoS and delivery time in ms
pub struct DeliveryTime(pub u8, pub f64);
