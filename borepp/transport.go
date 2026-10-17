package main

import (
	"bytes"
	"context"
	"errors"
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

func ServeTunnel(conn net.Conn, registry *StreamRegistry) error {
	defer conn.Close()

	writer := &TunnelWriter{
		conn:   conn,
		frames: make(chan Frame, 16),
		done:   make(chan struct{}),
	}
	go func() {
		if err := writer.Run(); err != nil {
			fmt.Println(err)
		}
	}()
	defer func() {
		_ = conn.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = registry.Shutdown(ctx)
		close(writer.frames)
		<-writer.done
	}()

	for {
		frame, err := DecodeFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if err := HandleIncomingFrame(registry, frame, writer); err != nil {
			return err
		}
	}
}

func (writer *TunnelWriter) Run() error {
	defer close(writer.done)

	for frame := range writer.frames {
		if err := WriteFrame(writer.conn, frame); err != nil {
			return err
		}
	}

	return nil
}

func (writer *TunnelWriter) Send(frame Frame) error {
	select {
	case writer.frames <- frame:
		return nil
	case <-writer.done:
		return fmt.Errorf("tunnel writer stopped")
	}
}

func (writer *TunnelWriter) Submit(frame Frame) error {
	return writer.Send(frame)
}
