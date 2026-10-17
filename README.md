The goal is to let someone expose a service running on their private computer to users on the internet, without opening inbound router ports or configuring port forwarding.

What will ekz_network actually do?

> Imagine you're running a Minecraft server, website, SSH server, or database on your laptop. Normally, another person on the internet cannot directly connect to it because your laptop is behind a router or NAT.

Status: Experimental / work in progress. The current implementation is a local TCP-forwarding prototype, 

Whats working??
- Binary framing: encode/decode typed frames (OPEN, DATA, CLOSE) with stream IDs and payload lengths.
- Logical streams: multiplex multiple stream IDs over a single TCP tunnel connection, with per-stream queues and workers.
- Stream registry: synchronized registration, duplicate-ID prevention, pending dial tracking, and cleanup.
- Backpressure primitives: bounded queues and a credit-based flow-control experiment.
- TCP forwarding: forward incoming DATA to a configured local TCP service; package local responses as outgoing DATA frames.
- Single tunnel writer: serialize outbound frames to avoid interleaved writes.
- Lifecycle management: stream closure, connection cleanup, and shutdown coordination.
- Integration test: a reported passing end-to-end echo test through the tunnel and local TCP echo service.
```
Test client
    │ OPEN / DATA / CLOSE frames
    ▼
Tunnel TCP listener
    │
    ▼
Frame decoder → StreamRegistry
                    │
                    ├── Stream 42 ── local TCP connection
                    ├── Stream 43 ── local TCP connection
                    └── Stream 44 ── local TCP connection
                           │
                     Local service
                           │ response bytes
                           ▼
                   Local reader goroutine
                           │ DATA frame
                           ▼
                    TunnelWriter
                           │
                           ▼
                      Test client
```
