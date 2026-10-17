package main

import (
	"context"
	"fmt"
)

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

func HandleIncomingFrame(registry *StreamRegistry, frame Frame, writer *TunnelWriter) error {
	switch frame.Type {
	case FrameOpen:
		_, err := registry.OpenStream(frame.StreamID, writer)
		return err

	case FrameData, FrameWindowUpdate, FrameClose:
		return registry.EnqueueFrame(frame)

	default:
		return fmt.Errorf("unsupported frame type: %d", frame.Type)
	}
}

func (r *StreamRegistry) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	r.shuttingDown = true
	r.mu.Unlock()

	r.dialWG.Wait()

	finished := make(chan struct{})

	go func() {
		r.wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		return nil

	case <-ctx.Done():
		r.CloseAll()
		<-finished
		return ctx.Err()
	}
}
