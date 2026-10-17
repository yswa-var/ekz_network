The goal is to let someone expose a service running on their private computer to users on the internet, without opening inbound router ports or configuring port forwarding.

What will ekz_network actually do?

> Imagine you're running a Minecraft server, website, SSH server, or database on your laptop. Normally, another person on the internet can directly connect to it using ekz 100's of services one tunnel!!!.

Whats working??
- Binary Protocol: Framed TCP messages (OPEN, DATA, CLOSE).
- Multiplexing: Multiple logical streams over one connection.
- Stream Management: Concurrent registry, workers, and cleanup.
- Flow Control: Bounded queues and backpressure primitives.
- TCP Forwarding: Bidirectional data transfer.
- Concurrency: Serialized writes and graceful shutdown.
- Testing: Passing end-to-end TCP echo integration test.
