package main

import (
	"bytes"
	"context"
	"fmt"
)

func runDemo() {
	demoBinaryFraming()
	demoStreamStateMachine()
	demoFrameRouting()
	demoFlowControl()
	demoTunnelToLocal()
}

func demoTunnelToLocal() {
	printSection("Tunnel to local TCP forwarding")

	registry := NewStreamRegistry("127.0.0.1:3000")

	frames := []Frame{
		{Type: FrameOpen, StreamID: 42},
		{Type: FrameData, StreamID: 42, Payload: []byte("hello bore++")},
		{Type: FrameClose, StreamID: 42},
	}

	for _, frame := range frames {
		if err := HandleIncomingFrame(registry, frame); err != nil {
			panic(err)
		}
	}

	if err := registry.Shutdown(context.Background()); err != nil {
		panic(err)
	}

	fmt.Println("Tunnel forwarding completed")
}

func demoBinaryFraming() {
	printSection("Binary framing and frame decoder")

	original := Frame{
		Type:     FrameData,
		StreamID: 7,
		Payload:  []byte("hello from a frame"),
	}

	encoded := EncodeFrame(original)
	decoded, err := DecodeFrame(bytes.NewReader(encoded))
	if err != nil {
		panic(err)
	}

	fmt.Printf("encoded %d bytes -> stream=%d type=%d payload=%q\n",
		len(encoded), decoded.StreamID, decoded.Type, decoded.Payload)
}

func demoStreamStateMachine() {
	printSection("Stream state machine")

	stream := &Stream{
		ID:   1,
		Done: make(chan struct{}),
	}
	fmt.Printf("initial state: %s\n", streamStateName(stream.State))

	_ = stream.Open()
	fmt.Printf("after OPEN: %s\n", streamStateName(stream.State))
	fmt.Printf("second OPEN rejected: %v\n", stream.Open())

	_ = stream.Close()
	fmt.Printf("after CLOSE: %s\n", streamStateName(stream.State))
	fmt.Printf("DATA after CLOSE rejected: %v\n", stream.HandleData([]byte("late data")))
}

func demoFrameRouting() {
	printSection("OPEN / DATA / CLOSE routing")

	registry := NewStreamRegistry("127.0.0.1:3000")
	streamID := uint32(42)

	frames := []Frame{
		{Type: FrameOpen, StreamID: streamID},
		{Type: FrameData, StreamID: streamID, Payload: []byte("routed payload")},
		{Type: FrameClose, StreamID: streamID},
	}

	for _, frame := range frames {
		if err := HandleIncomingFrame(registry, frame); err != nil {
			panic(err)
		}
		fmt.Printf("routed frame type %d\n", frame.Type)
	}

	if err := registry.Shutdown(context.Background()); err != nil {
		panic(err)
	}

	_, stillRegistered := registry.Get(streamID)
	fmt.Printf("stream removed after CLOSE: %t\n", !stillRegistered)
}

func demoFlowControl() {
	printSection("Flow control")

	flow := NewFlowControl(4)
	fmt.Printf("consume 4 credits: %t (remaining=%d)\n", flow.Consume(4), flow.Credit)

	consumed := make(chan struct{})
	go func() {
		flow.WaitAndConsume(3)
		close(consumed)
	}()

	fmt.Println("consumer waits for a window update")
	flow.WindowUpdate(3)
	<-consumed
	fmt.Printf("window updated: 3 credits consumed (remaining=%d)\n", flow.Credit)
}

func printSection(title string) {
	fmt.Printf("\n--- %s ---\n", title)
}

func streamStateName(state StreamState) string {
	switch state {
	case StreamIdle:
		return "IDLE"
	case StreamOpen:
		return "OPEN"
	case StreamClosed:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}
