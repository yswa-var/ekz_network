package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

type Frame struct {
	Type     uint8
	StreamID uint32
	Seq      uint64
	Payload  []byte
}

func EncodeFrame(frame Frame) []byte {
	buf := make([]byte, 9+len(frame.Payload))

	buf[0] = frame.Type
	binary.BigEndian.PutUint32(buf[1:5], frame.StreamID)
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(frame.Payload)))
	copy(buf[9:], frame.Payload)

	return buf
}

func DecodeFrame(reader io.Reader) (Frame, error) {
	header := make([]byte, 9)
	if _, err := io.ReadFull(reader, header); err != nil {
		return Frame{}, err
	}

	length := binary.BigEndian.Uint32(header[5:9])
	if length > MaxMessageSize {
		return Frame{}, fmt.Errorf("frame too large")
	}

	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return Frame{}, err
	}

	return Frame{
		Type:     header[0],
		StreamID: binary.BigEndian.Uint32(header[1:5]),
		Payload:  payload,
	}, nil
}

func HandleFrame(s *Stream, frame Frame) error {
	if frame.StreamID != s.ID {
		return fmt.Errorf("wrong stream ID")
	}

	switch frame.Type {
	case FrameOpen:
		err := s.Open()
		if err != nil {
			return err
		}

	case FrameData:
		// Reject if s.CanReceiveData() is false
		// Otherwise print the payload as a string
		err := s.HandleData(frame.Payload)
		if err != nil {
			return err
		}

	case FrameClose:
		err := s.Close()
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown frame type: %d", frame.Type)
	}

	return nil
}

type StreamRegistry struct {
	mu      sync.RWMutex
	streams map[uint32]*Stream
}

func NewStreamRegistry() *StreamRegistry {
	return &StreamRegistry{
		streams: make(map[uint32]*Stream),
	}
}

func (r *StreamRegistry) Get(id uint32) (*Stream, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// Look up the stream by ID.
	stream, ok := r.streams[id]

	// Return the stream and whether it exists.
	return stream, ok
}

func (r *StreamRegistry) Register(id uint32) (*Stream, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.streams[id]; exists {
		return nil, fmt.Errorf("stream %d already exists", id)
	}

	stream := &Stream{
		ID:    id,
		State: StreamIdle,
	}

	r.streams[id] = stream

	return stream, nil
}

func (s *Stream) HandleData(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.State != StreamOpen {
		return fmt.Errorf("stream %d is not open", s.ID)
	}

	fmt.Println(string(payload))
	return nil
}

func (r *StreamRegistry) Remove(id uint32) error {
	// 1. Acquire exclusive write lock
	r.mu.Lock()
	defer r.mu.Unlock()

	// 2. Check if stream exists
	//    If not, return an error
	if _, exists := r.streams[id]; !exists {
		return fmt.Errorf("stream %d not exists", id)
	}

	// 3. Delete stream from map
	delete(r.streams, id)

	// 4. Return nil
	return nil
}

func HandleIncomingFrame(r *StreamRegistry, frame Frame) error {
	switch frame.Type {
	case FrameOpen:
		// 1. Register frame.StreamID
		stream, err := r.Register(frame.StreamID)
		if err != nil {
			return err
		}

		err = stream.Open()
		if err != nil {
			r.Remove(frame.StreamID)
			return err
		}
		return nil
	case FrameData:
		stream, ok := r.Get(frame.StreamID)
		if !ok {
			return fmt.Errorf("stream %d not exists", frame.StreamID)
		}

		return stream.HandleData(frame.Payload)
	case FrameClose:
		stream, ok := r.Get(frame.StreamID)
		if !ok {
			return fmt.Errorf("stream %d not exists", frame.StreamID)
		}

		err := stream.Close()
		if err != nil {
			return err
		}

		return r.Remove(stream.ID)

	default:
		return fmt.Errorf("unsupported frame type: %d", frame.Type)
	}
}
