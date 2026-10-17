package main

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestTunnelIntegration(t *testing.T) {
	const echoAddr = "127.0.0.1:3000"
	const tunnelAddr = "127.0.0.1:9000"

	go func() {
		if err := StartEchoServer(echoAddr); err != nil {
			t.Errorf("echo server: %v", err)
		}
	}()
	waitForTCPListener(t, echoAddr)

	go func() {
		if err := StartTunnelServer(tunnelAddr); err != nil {
			t.Errorf("tunnel server: %v", err)
		}
	}()
	waitForTCPListener(t, tunnelAddr)

	if err := TestTunnelEcho(tunnelAddr); err != nil {
		t.Fatalf("tunnel echo: %v", err)
	}
}

func waitForTCPListener(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("listener %s did not become ready: %v", addr, fmt.Errorf("%w", lastErr))
}
