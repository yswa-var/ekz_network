package main

import (
	"encoding/binary"
	"fmt"
	"io"
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
		if !s.CanReceiveData() {
			return fmt.Errorf("cannot receive DATA: stream %d is not open", s.ID)
		}

		fmt.Println(string(frame.Payload))

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
