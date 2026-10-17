package main

import (
	"fmt"
	"sync"
)

type StreamState uint8

const (
	StreamIdle StreamState = iota
	StreamOpen
	StreamClosed
)

type Stream struct {
	mu    sync.Mutex
	ID    uint32
	State StreamState
}

func (stream *Stream) CanReceiveData() bool {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	return stream.State == StreamOpen
}

func (stream *Stream) Open() error {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if stream.State != StreamIdle {
		return fmt.Errorf("invalid state: %d", stream.State)
	}

	stream.State = StreamOpen
	return nil
}

func (stream *Stream) Close() error {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if stream.State != StreamOpen {
		return fmt.Errorf("can't Close stream: invalid state %d", stream.State)
	}

	stream.State = StreamClosed
	return nil
}

func (stream *Stream) HandleData(payload []byte) error {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if stream.State != StreamOpen {
		return fmt.Errorf("stream %d is not open", stream.ID)
	}

	fmt.Println(string(payload))
	return nil
}
