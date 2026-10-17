package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"
)

const MaxConnections = 2

func StartTunnelServer(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		registry := NewStreamRegistry()
		go func() {
			if err := ServeTunnel(conn, registry); err != nil {
				log.Println(err)
			}
		}()
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	message := make([]byte, 0, 1024)

	messageDeadline := time.Now().Add(60 * time.Second)

	for {
		idleDeadline := time.Now().Add(10 * time.Second)

		deadline := idleDeadline
		if messageDeadline.Before(deadline) {
			deadline = messageDeadline
		}

		if err := conn.SetReadDeadline(deadline); err != nil {
			return
		}

		b, err := reader.ReadByte()
		if err != nil {
			return
		}

		if b == '\n' {
			fmt.Println("Message:", string(message))
			_, err := conn.Write(append(message, '\n'))
			if err != nil {
				fmt.Println(err)
				return
			}
			message = message[:0]

			messageDeadline = time.Now().Add(60 * time.Second)
			continue
		}

		if len(message) >= MaxMessageSize {
			fmt.Println("Message too large")
			return
		}

		message = append(message, b)
	}
}

// func main() {
// 	ln, err := net.Listen("tcp", ":3000")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer ln.Close()

// 	slots := make(chan struct{}, MaxConnections)
// 	for {
// 		conn, err := ln.Accept()
// 		if err != nil {
// 			fmt.Println(err)
// 			return
// 		}

// 		select {
// 		case slots <- struct{}{}:
// 			go func() {
// 				defer func() { <-slots }()
// 				handleClient(conn)
// 			}()

// 		default:
// 			conn.Close() // Server at capacity
// 		}
// 	}
// }
