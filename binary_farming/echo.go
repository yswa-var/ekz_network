package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"time"
)

func StartEchoServer(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		go func() {
			defer conn.Close()
			_, _ = io.Copy(conn, conn)
		}()
	}
}

func TestEchoServer(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	expected := []byte("hello bore++")
	n, err := conn.Write(expected)
	if err != nil {
		return err
	}
	if n != len(expected) {
		return io.ErrShortWrite
	}

	actual := make([]byte, len(expected))
	if _, err := io.ReadFull(conn, actual); err != nil {
		return err
	}
	if !bytes.Equal(expected, actual) {
		return fmt.Errorf("unexpected echo response: got %q, want %q", actual, expected)
	}

	return nil
}

func TestTunnelEcho(tunnelAddr string) error {
	conn, err := net.Dial("tcp", tunnelAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}

	const streamID = uint32(42)
	expected := []byte("hello bore++")

	if err := WriteFrame(conn, Frame{
		Type:     FrameOpen,
		StreamID: streamID,
	}); err != nil {
		return err
	}
	if err := WriteFrame(conn, Frame{
		Type:     FrameData,
		StreamID: streamID,
		Payload:  expected,
	}); err != nil {
		return err
	}

	var received []byte
	for len(received) < len(expected) {
		response, err := DecodeFrame(conn)
		if err != nil {
			return err
		}
		if response.StreamID != streamID {
			return fmt.Errorf("unexpected stream ID: got %d, want %d", response.StreamID, streamID)
		}
		if response.Type != FrameData {
			return fmt.Errorf("unexpected frame type: got %d, want %d", response.Type, FrameData)
		}

		received = append(received, response.Payload...)
		if len(received) > len(expected) {
			return fmt.Errorf("received payload exceeds expected length: got %d, want %d", len(received), len(expected))
		}
	}

	if !bytes.Equal(received, expected) {
		return fmt.Errorf("unexpected payload: got %q, want %q", received, expected)
	}

	return nil
}
