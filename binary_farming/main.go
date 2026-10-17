package main

import (
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: go run . [server|client|demo]")
	}

	switch os.Args[1] {
	case "server":
		log.Fatal(StartTunnelServer(":9000"))

	case "client":
		if err := TestTunnelClient(); err != nil {
			log.Fatal(err)
		}
		log.Println("OPEN frame sent")

	case "demo":
		runDemo()

	default:
		log.Fatal("unknown mode")
	}
}
