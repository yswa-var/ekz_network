package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
)

type StreamState uint8

const (
	StreamIdle StreamState = iota
	StreamOpen
	StreamClosed
)

type Stream struct {
	ID    uint32
	State StreamState
}

func (s *Stream) CanReceiveData() bool {
	return s.State == StreamOpen
}

func (s *Stream) Open() error {
	if s.State != StreamIdle {
		return fmt.Errorf("can't open stream: invalid state %d", s.State)
	}
	s.State = StreamOpen
	return nil
}

func (s *Stream) Close() error {
	// 1. Reject if state is not StreamOpen
	if s.State != StreamOpen {
		return fmt.Errorf("can't Close stream: invalid state %d", s.State)
	}
	// 2. Set state to StreamClosed
	s.State = StreamClosed

	// 3. Return nil
	return nil
}

func SendData(
	conn net.Conn,
	streamID uint32,
	data []byte,
	flow *FlowControl,
) error {
	chunks := SplitPayload(data, MaxChunkSize)

	for _, chunk := range chunks {
		flow.WaitAndConsume(len(chunk))

		frame := Frame{
			Type:     FrameData,
			StreamID: streamID,
			Payload:  chunk,
		}

		encoded := EncodeFrame(frame)
		if _, err := io.Copy(conn, bytes.NewReader(encoded)); err != nil {
			return err
		}
	}

	return nil
}

type TunnelWriter struct {
	conn   net.Conn
	frames chan Frame
	done   chan struct{}
}

func (t *TunnelWriter) Run() error {
	defer close(t.done)

	for frame := range t.frames {
		encoded := EncodeFrame(frame)

		_, err := io.Copy(t.conn, bytes.NewReader(encoded))
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TunnelWriter) Submit(frame Frame) error {
	select {
	case t.frames <- frame:
		return nil
	case <-t.done:
		return fmt.Errorf("tunnel writer stopped")
	}
}
