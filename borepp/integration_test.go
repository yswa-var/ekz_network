package main

import (
	"net"
	"testing"
)

func TestTunnelIntegration(t *testing.T) {
	echoLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoLn.Close()
	echoAddr := echoLn.Addr().String()

	go func() {
		_ = ServeEcho(echoLn)
	}()

	tunnelLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tunnelLn.Close()
	tunnelAddr := tunnelLn.Addr().String()

	go func() {
		_ = ServeTunnelServer(tunnelLn, echoAddr)
	}()

	if err := TestTunnelEcho(tunnelAddr); err != nil {
		t.Fatalf("tunnel echo: %v", err)
	}
	if err := TestMultiplexedEcho(tunnelAddr); err != nil {
		t.Fatalf("multiplexed tunnel echo: %v", err)
	}
	if err := TestLargeMultiplexedEcho(tunnelAddr); err != nil {
		t.Fatalf("large multiplexed tunnel echo: %v", err)
	}
}
