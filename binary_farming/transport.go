package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
)

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

func (writer *TunnelWriter) Run() error {
	defer close(writer.done)

	for frame := range writer.frames {
		encoded := EncodeFrame(frame)
		if _, err := io.Copy(writer.conn, bytes.NewReader(encoded)); err != nil {
			return err
		}
	}

	return nil
}

func (writer *TunnelWriter) Submit(frame Frame) error {
	select {
	case writer.frames <- frame:
		return nil
	case <-writer.done:
		return fmt.Errorf("tunnel writer stopped")
	}
}

