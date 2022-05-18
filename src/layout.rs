struct Coordinator {
    topic_tree: TopicTree,
    message_log: FileLog,
    // sessions: SessionCollection,
    metrics: Metrics,
}

pub struct TopicTree {
    queues: Vec<Queue>,
}

struct Queue {
    subscriptions: Vec<(u8, u16, String, WriterProcess)>,
    queue: FileQueue,
    metrics: Metrics
}

struct FileQueue {
    log: FileLog,
    /// not a real queue since we can take next messages
    /// to process under certain circumstances (e.g. publish second
    /// message while first is still waiting for puback from client)
    queue: Vec<(MqttMessage, MessageState)>,
}

struct MessageState {
    Ready,
    Published,
}

struct FileLog {
    log_file: File,
    cursor: FileCursor,
}

struct FileCursor {
    cursor: number,
    file: File,
}
