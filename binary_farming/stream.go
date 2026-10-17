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
	mu       sync.Mutex
	ID       uint32
	State    StreamState
	Incoming chan Frame
	Done     chan struct{}
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

	close(stream.Done)

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

func (r *StreamRegistry) CloseAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, stream := range r.streams {
		_ = stream.Close()
		delete(r.streams, id)
	}
}

func (r *StreamRegistry) OpenStream(id uint32) (*Stream, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.shuttingDown {
		return nil, fmt.Errorf("registry is shutting down")
	}

	if _, exists := r.streams[id]; exists {
		return nil, fmt.Errorf("stream %d already exists", id)
	}

	stream := &Stream{
		ID:       id,
		State:    StreamIdle,
		Incoming: make(chan Frame, 16),
		Done:     make(chan struct{}),
	}

	if err := stream.Open(); err != nil {
		return nil, err
	}

	r.streams[id] = stream
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		stream.RunWorker(r)
	}()

	return stream, nil
}

func (s *Stream) Enqueue(frame Frame) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.State != StreamOpen {
		return fmt.Errorf("stream %d is not open", s.ID)
	}

	select {
	case s.Incoming <- frame:
		return nil
	default:
		return fmt.Errorf("stream %d incoming queue is full", s.ID)
	}
}

func (r *StreamRegistry) EnqueueFrame(frame Frame) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.shuttingDown {
		return fmt.Errorf("registry is shutting down")
	}

	stream, exists := r.streams[frame.StreamID]
	if !exists {
		return fmt.Errorf("stream not found")
	}

	return stream.Enqueue(frame)
}

func (s *Stream) RunWorker(registry *StreamRegistry) {
	defer registry.RemoveIfSame(s)

	for {
		select {
		case frame := <-s.Incoming:
			switch frame.Type {
			case FrameData:
				if err := s.HandleData(frame.Payload); err != nil {
					fmt.Println(err)
				}
			case FrameClose:
				if err := s.Close(); err != nil {
					fmt.Println(err)
				}
				return
			}
		case <-s.Done:
			return
		}
	}
}
