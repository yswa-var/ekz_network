package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
)

type StreamState uint8

const (
	StreamIdle StreamState = iota
	StreamOpen
	StreamClosed
)

const InitialStreamWindow = 64 * 1024

type Stream struct {
	mu         sync.Mutex
	ID         uint32
	State      StreamState
	Incoming   chan Frame
	Done       chan struct{}
	LocalConn  net.Conn
	sendMu     sync.Mutex
	sendCredit int64
	sendCond   *sync.Cond
}

func (stream *Stream) CanReceiveData() bool {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	return stream.State == StreamOpen
}

func (stream *Stream) IsClosed() bool {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	return stream.State == StreamClosed
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

func (stream *Stream) Connect(addr string) error {
	conn, err := ConnectLocal(addr)
	if err != nil {
		return err
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	stream.LocalConn = conn

	return nil
}

func (stream *Stream) Close() error {
	// Keep the state transition and condition broadcast ordered with
	// WaitAndConsumeCredit's check so a waiter cannot miss the close signal.
	stream.sendMu.Lock()
	defer stream.sendMu.Unlock()

	stream.mu.Lock()

	if stream.State != StreamOpen {
		state := stream.State
		stream.mu.Unlock()
		return fmt.Errorf("can't close stream: invalid state %d", state)
	}

	stream.State = StreamClosed
	close(stream.Done)
	conn := stream.LocalConn

	stream.mu.Unlock()

	if stream.sendCond != nil {
		stream.sendCond.Broadcast()
	}

	if conn != nil {
		return conn.Close()
	}

	return nil
}

func (stream *Stream) HandleData(payload []byte) error {
	stream.mu.Lock()
	if stream.State != StreamOpen {
		stream.mu.Unlock()
		return fmt.Errorf("stream %d is not open", stream.ID)
	}

	conn := stream.LocalConn
	stream.mu.Unlock()

	if conn == nil {
		return fmt.Errorf("stream %d has no local connection", stream.ID)
	}

	return ForwardToLocal(conn, payload)
}

func (r *StreamRegistry) CloseAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, stream := range r.streams {
		_ = stream.Close()
		delete(r.streams, id)
	}
}

func (r *StreamRegistry) OpenStream(
	id uint32,
	writer *TunnelWriter) (*Stream, error) {
	if writer == nil {
		return nil, fmt.Errorf("tunnel writer is nil")
	}

	r.mu.Lock()
	if r.shuttingDown {
		r.mu.Unlock()
		return nil, fmt.Errorf("registry is shutting down")
	}

	if _, exists := r.streams[id]; exists {
		r.mu.Unlock()
		return nil, fmt.Errorf("stream %d already exists", id)
	}

	if r.pending[id] {
		r.mu.Unlock()
		return nil, fmt.Errorf("stream %d is being opened", id)
	}

	r.pending[id] = true
	r.dialWG.Add(1)
	localAddr := r.localAddr
	r.mu.Unlock()
	defer r.dialWG.Done()

	stream := &Stream{
		ID:         id,
		State:      StreamIdle,
		Incoming:   make(chan Frame, 16),
		Done:       make(chan struct{}),
		sendCredit: InitialStreamWindow,
	}

	stream.sendCond = sync.NewCond(&stream.sendMu)

	if err := stream.Open(); err != nil {
		r.mu.Lock()
		delete(r.pending, id)
		r.mu.Unlock()
		return nil, err
	}

	if err := stream.Connect(localAddr); err != nil {
		r.mu.Lock()
		delete(r.pending, id)
		r.mu.Unlock()
		return nil, err
	}

	r.mu.Lock()
	delete(r.pending, id)
	if r.shuttingDown {
		r.mu.Unlock()
		_ = stream.Close()
		return nil, fmt.Errorf("registry is shutting down")
	}

	r.streams[id] = stream
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		stream.RunWorker(r, writer)
	}()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		err := PumpLocalToTunnel(stream.LocalConn, stream, writer)
		if err != nil && !errors.Is(err, net.ErrClosed) {
			fmt.Println("local reader:", err)
		}
		if err == nil && !stream.IsClosed() {
			if err := writer.Send(Frame{
				Type:     FrameClose,
				StreamID: stream.ID,
			}); err != nil {
				fmt.Println("local reader:", err)
			}
		}
		_ = stream.Close()
	}()
	r.mu.Unlock()

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

func (s *Stream) RunWorker(
	registry *StreamRegistry,
	writer *TunnelWriter,
) {
	defer registry.RemoveIfSame(s)

	for {
		select {
		case frame := <-s.Incoming:
			switch frame.Type {
			case FrameData:
				if err := s.HandleData(frame.Payload); err != nil {
					fmt.Println(err)
					continue
				}
				update := Frame{
					Type:     FrameWindowUpdate,
					StreamID: s.ID,
					Payload:  EncodeWindowUpdate(uint64(len(frame.Payload))),
				}

				if err := writer.Send(update); err != nil {
					fmt.Println(err)
					return
				}
			case FrameWindowUpdate:
				n, err := DecodeWindowUpdate(frame.Payload)
				if err != nil {
					fmt.Println(err)
					continue
				}

				if n > math.MaxInt64 {
					fmt.Println("window update too large")
					continue
				}

				if err := s.AddCredit(int64(n)); err != nil {
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

func (s *Stream) AddCredit(n int64) error {
	if n <= 0 {
		return fmt.Errorf("credit amount must be positive")
	}

	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	s.sendCredit += n
	s.sendCond.Broadcast()

	return nil
}

func (s *Stream) WaitAndConsumeCredit(n int64) error {
	if n <= 0 {
		return fmt.Errorf("credit amount must be positive")
	}

	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	for {
		s.mu.Lock()
		closed := s.State == StreamClosed
		s.mu.Unlock()
		if closed {
			return fmt.Errorf("stream %d is closed", s.ID)
		}

		if s.sendCredit >= n {
			s.sendCredit -= n
			return nil
		}

		s.sendCond.Wait()
	}
}

func EncodeWindowUpdate(n uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

func DecodeWindowUpdate(payload []byte) (uint64, error) {
	if len(payload) != 8 {
		return 0, fmt.Errorf("invalid window update payload")
	}

	return binary.BigEndian.Uint64(payload), nil
}
