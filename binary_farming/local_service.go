package main

import (
	"fmt"
	"io"
	"net"
)

func StartLocalService(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	fmt.Println("Local service listening on", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		go func() {
			defer conn.Close()
			_, _ = io.Copy(io.Discard, io.TeeReader(conn, &loggingWriter{}))
		}()
	}
}

type loggingWriter struct{}

func (w *loggingWriter) Write(p []byte) (int, error) {
	fmt.Printf("LOCAL SERVICE RECEIVED: %q\n", p)
	return len(p), nil
}
