package main

import (
	"fmt"
	"sync"
)

type StreamRegistry struct {
	mu      sync.RWMutex
	streams map[uint32]*Stream
}

func NewStreamRegistry() *StreamRegistry {
	return &StreamRegistry{
		streams: make(map[uint32]*Stream),
	}
}

func (registry *StreamRegistry) Get(id uint32) (*Stream, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	stream, ok := registry.streams[id]
	return stream, ok
}

func (registry *StreamRegistry) Register(id uint32) (*Stream, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if _, exists := registry.streams[id]; exists {
		return nil, fmt.Errorf("stream %d already exists", id)
	}

	stream := &Stream{ID: id, State: StreamIdle}
	registry.streams[id] = stream
	return stream, nil
}

func (registry *StreamRegistry) Remove(id uint32) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if _, exists := registry.streams[id]; !exists {
		return fmt.Errorf("stream %d not exists", id)
	}

	delete(registry.streams, id)
	return nil
}
