package main

import "fmt"

func HandleFrame(stream *Stream, frame Frame) error {
	if frame.StreamID != stream.ID {
		return fmt.Errorf("wrong stream ID")
	}

	switch frame.Type {
	case FrameOpen:
		return stream.Open()
	case FrameData:
		return stream.HandleData(frame.Payload)
	case FrameClose:
		return stream.Close()
	default:
		return fmt.Errorf("unknown frame type: %d", frame.Type)
	}
}

func HandleIncomingFrame(registry *StreamRegistry, frame Frame) error {
	switch frame.Type {
	case FrameOpen:
		stream, err := registry.Register(frame.StreamID)
		if err != nil {
			return err
		}

		if err := stream.Open(); err != nil {
			_ = registry.Remove(frame.StreamID)
			return err
		}
		return nil

	case FrameData:
		stream, ok := registry.Get(frame.StreamID)
		if !ok {
			return fmt.Errorf("stream %d not exists", frame.StreamID)
		}
		return stream.HandleData(frame.Payload)

	case FrameClose:
		stream, ok := registry.Get(frame.StreamID)
		if !ok {
			return fmt.Errorf("stream %d not exists", frame.StreamID)
		}

		if err := stream.Close(); err != nil {
			return err
		}
		return registry.Remove(stream.ID)

	default:
		return fmt.Errorf("unsupported frame type: %d", frame.Type)
	}
}
