# m-cutie-tea
A MQTT Broker based on the excellent lunatic runtime in rust

Overview of process types:

TCPReader:
    ParseMqtt messages
    -> Request queues for publishing from broker
    -> subscribe TCPWriter to queues via broker
    -> publish directly to queues
    -> communicate order of messages with qos > 0 to TCPWriter

TCPWriter:
    Receive message from Queue (publish/pubrel/pubrec, suback, unsuback)
    Receive messages from tcpReader (ping, connect, disconnect, order of incoming messages)
    -> Encode and write response to tcp stream
    -> preserve response order according to incoming order
    -> Buffer messsage with qos > 0 after client is disconnected for reconnects


Broker:
    Connect TCPWriter with publish Queues
    Spawns new TCPWriter
    Link TCPWriter to subscribed queues
    Maintain list of open sessions for userIDs


Queue:
    Publish messages to subscribers
    Persist messages according to QoS
    Retain "last good message"
    Manage message delivery state when QoS > 0




Milestones:
[x] Enable publishing and subscribing
[x] Add wildcard matching
[ ] Add session lifetime and will/testament adherence
[ ] Authentication
[ ] Delete previous connection state on clean reconnect
[ ] Preserve connection state on non-clean reconnects
[ ] Enable QoS 1 publishing
[ ] Enable QoS 2 publishing
[ ] Solid 


