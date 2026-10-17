package main

import (
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
