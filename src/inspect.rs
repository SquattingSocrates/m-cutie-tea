use crate::metrics::MetricsProcess;
use lunatic::{
    host,
    process::{AbstractProcess, ProcessRef, ProcessRequest, Request, StartProcess},
    supervisor::Supervisor,
    Process,
};
use serde::{Deserialize, Serialize};

/// The `InspectSup` is supervising one global instance of the `InspectProcess`.
pub struct InspectSup;
impl Supervisor for InspectSup {
    type Arg = String;
    type Children = InspectProcess;

    fn init(config: &mut lunatic::supervisor::SupervisorConfig<Self>, name: Self::Arg) {
        // Always register the `InspectProcess` under the name passed to the supervisor.
        config.children_args(((), Some(name)))
    }
}

/// this process will handle web UI interactions
/// which should allow a user to send messages and inspect queues etc.
pub struct InspectProcess {
    metrics: ProcessRef<MetricsProcess>,
}

impl AbstractProcess for InspectProcess {
    type Arg = ();
    type State = Self;

    fn init(_: ProcessRef<Self>, _: Self::Arg) -> Self::State {
        // Inspector shouldn't die when a client dies. This makes the link one-directional.
        unsafe { host::api::process::die_when_link_dies(0) };

        let metrics = MetricsProcess::get_process();

        InspectProcess { metrics }
    }
}
