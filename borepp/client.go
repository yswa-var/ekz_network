package main

import (
	"bytes"
	"io"
	"net"
)

func WriteFrame(conn net.Conn, frame Frame) error {
	_, err := io.Copy(conn, bytes.NewReader(EncodeFrame(frame)))
	return err
}

func TestTunnelClient() error {
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		return err
	}
	defer conn.Close()

	frames := []Frame{
		{Type: FrameOpen, StreamID: 42},
		{Type: FrameOpen, StreamID: 43},
		{Type: FrameData, StreamID: 42, Payload: []byte("A1")},
		{Type: FrameData, StreamID: 43, Payload: []byte("B1")},
		{Type: FrameData, StreamID: 42, Payload: []byte("A2")},
		{Type: FrameData, StreamID: 43, Payload: []byte("B2")},
		{Type: FrameClose, StreamID: 42},
		{Type: FrameClose, StreamID: 43},
	}

	for _, frame := range frames {
		if err := WriteFrame(conn, frame); err != nil {
			return err
		}
	}
	return nil
}
