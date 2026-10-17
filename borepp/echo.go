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

	return ServeEcho(listener)
}

func ServeEcho(listener net.Listener) error {
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

func TestMultiplexedEcho(tunnelAddr string) error {
	conn, err := net.Dial("tcp", tunnelAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}

	expected := make(map[uint32][]byte)
	for streamID := uint32(1); streamID <= 100; streamID++ {
		expected[streamID] = []byte(fmt.Sprintf("message-from-stream-%d", streamID))
	}
	received := make(map[uint32][]byte, len(expected))

	for streamID := range expected {
		if err := WriteFrame(conn, Frame{
			Type:     FrameOpen,
			StreamID: streamID,
		}); err != nil {
			return err
		}
	}
	for streamID, payload := range expected {
		if err := WriteFrame(conn, Frame{
			Type:     FrameData,
			StreamID: streamID,
			Payload:  payload,
		}); err != nil {
			return err
		}
	}

	completed := 0
	for completed < len(expected) {
		response, err := DecodeFrame(conn)
		if err != nil {
			return err
		}

		want, ok := expected[response.StreamID]
		if !ok {
			return fmt.Errorf("unexpected stream ID: %d", response.StreamID)
		}
		if response.Type != FrameData {
			return fmt.Errorf("unexpected frame type on stream %d: got %d, want %d", response.StreamID, response.Type, FrameData)
		}

		before := len(received[response.StreamID])
		received[response.StreamID] = append(received[response.StreamID], response.Payload...)
		after := len(received[response.StreamID])
		if after > len(want) {
			return fmt.Errorf("stream %d payload exceeds expected length: got %d, want %d", response.StreamID, after, len(want))
		}
		if before < len(want) && after == len(want) {
			completed++
		}
	}

	for streamID, want := range expected {
		if !bytes.Equal(received[streamID], want) {
			return fmt.Errorf("unexpected payload on stream %d: got %q, want %q", streamID, received[streamID], want)
		}
	}

	return nil
}

func makePayload(streamID uint32, size int) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte((int(streamID) + i) % 256)
	}
	return buf
}

func TestLargeMultiplexedEcho(tunnelAddr string) error {
	conn, err := net.Dial("tcp", tunnelAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}

	const (
		streamCount = uint32(10)
		payloadSize = 1 << 20
	)
	expected := make(map[uint32][]byte, streamCount)
	for streamID := uint32(1); streamID <= streamCount; streamID++ {
		expected[streamID] = makePayload(streamID, payloadSize)
	}

	sendErr := make(chan error, 1)
	go func() {
		for streamID := uint32(1); streamID <= streamCount; streamID++ {
			if err := WriteFrame(conn, Frame{
				Type:     FrameOpen,
				StreamID: streamID,
			}); err != nil {
				sendErr <- err
				return
			}
		}
		for streamID := uint32(1); streamID <= streamCount; streamID++ {
			if err := WriteFrame(conn, Frame{
				Type:     FrameData,
				StreamID: streamID,
				Payload:  expected[streamID],
			}); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}()

	received := make(map[uint32][]byte, streamCount)
	completed := uint32(0)
	for completed < streamCount {
		response, err := DecodeFrame(conn)
		if err != nil {
			return err
		}

		want, ok := expected[response.StreamID]
		if !ok {
			return fmt.Errorf("unexpected stream ID: %d", response.StreamID)
		}
		if response.Type != FrameData {
			return fmt.Errorf("unexpected frame type on stream %d: got %d, want %d", response.StreamID, response.Type, FrameData)
		}

		before := len(received[response.StreamID])
		received[response.StreamID] = append(received[response.StreamID], response.Payload...)
		after := len(received[response.StreamID])
		if after > len(want) {
			return fmt.Errorf("stream %d payload exceeds expected length: got %d, want %d", response.StreamID, after, len(want))
		}
		if before < len(want) && after == len(want) {
			completed++
		}
	}

	if err := <-sendErr; err != nil {
		return err
	}
	for streamID, want := range expected {
		if !bytes.Equal(received[streamID], want) {
			return fmt.Errorf("unexpected payload on stream %d", streamID)
		}
	}

	return nil
}
