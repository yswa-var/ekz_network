package main

import (
	"errors"
	"io"
	"net"
	"time"
)

func ConnectLocal(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 5*time.Second)
}

func ReadLocal(conn net.Conn, buf []byte) (int, error) {
	return conn.Read(buf)
}

func ReadLocalFrame(conn net.Conn, streamID uint32) (Frame, error) {
	buf := make([]byte, 32*1024)
	n, err := conn.Read(buf)
	if n > 0 {
		return Frame{
			Type:     FrameData,
			StreamID: streamID,
			Payload:  buf[:n],
		}, err
	}

	return Frame{}, err
}

func PumpLocalToTunnel(conn net.Conn, streamID uint32, writer *TunnelWriter) error {
	for {
		frame, err := ReadLocalFrame(conn, streamID)
		if len(frame.Payload) > 0 {
			if sendErr := writer.Send(frame); sendErr != nil {
				return sendErr
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func ForwardToLocal(conn net.Conn, payload []byte) error {
	for len(payload) > 0 {
		n, err := conn.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}

	return nil
}
