# m-cutie-tea
A MQTT Broker based on the excellent lunatic runtime in rust

### How to start:

Run `cargo run` from within the directory

### Features (currently targeting MQTT v3):

- [x] QoS 0 messages
- [ ] Plugins
  - [ ] Define interface for different types of plugins
    - [ ] Authentication plugins
    - [ ] Message- or subscription-based plugins (e.g. pre-process or aggregate message data)
- [ ] Authentication
  - [ ] ENV based
  - [ ] File-based
- [ ] Handle faulty clients
  - [ ] Error codes on invalid packet configuration
  - [ ] Disconnect clients with malformed packets
  - [ ] Track connection attempts and ban clients after crossing a threshold
- [ ] Subscriptions
  - [ ] Re-subscriptions upon receiving messages that match pattern
  - [x] Pattern based subscriptions
  - [x] Regular subscriptions
  - [ ] Retained messages
- [ ] QoS 1 messages
  - [x] Handle QoS 1 flow
  - [x] File persistence and recovery from crash
  - [ ] Handle duplicate messages
  - [ ] Downgrading of message QoS for subscribers with lower QoS
- [ ] QoS 2 messages
  - [ ] Handle QoS 2 flow
  - [ ] Persist messages
  - [ ] Handle duplicate messages
  - [ ] Downgrading of message QoS for subscribers with lower QoS
- [ ] Persistence and recovery
  - [x] State persistence using Write-Ahead Log (WAL)
  - [x] Recovery from WAL
  - [ ] Recovery from crash loop (currenly file is read and then overwritten on the next publish, previous WAL state is not preserved)
  - [ ] Configurability of WAL file location
  - [ ] Compaction of WAL after it reaches some size
- [ ] Session state
  - [ ] Keep subscriptions if clean_session = false
  - [ ] Keep and resend messages of QoS 1 and QoS 2 if clean_session = false
  - [ ] Will handling
  - [ ] Client keep-alive window
- [ ] Secure connections (TLS)
- [ ] Inspection of broker state
  - [x] Prometheus metrics enpoint
  - [ ] Health/liveness endpoints for kubernetes setup
  - [ ] Web Dashboard
- [ ] Process scalability and performance
  - [ ] Add configuration for controlling number of workers
  - [ ] Improve (de)serialisation of messages (e.g. use serde_bytes)
  - [ ] Add clusterability (waiting on distributed lunatic)

