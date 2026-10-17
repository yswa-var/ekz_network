package main

import (
	"fmt"
	"sync"
)

type StreamRegistry struct {
	mu           sync.RWMutex
	streams      map[uint32]*Stream
	pending      map[uint32]bool
	wg           sync.WaitGroup
	dialWG       sync.WaitGroup
	shuttingDown bool
	localAddr    string
}

func NewStreamRegistry(localAddr string) *StreamRegistry {
	return &StreamRegistry{
		streams:   make(map[uint32]*Stream),
		pending:   make(map[uint32]bool),
		localAddr: localAddr,
	}
}

func (registry *StreamRegistry) Get(id uint32) (*Stream, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	stream, ok := registry.streams[id]
	return stream, ok
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

func (r *StreamRegistry) RemoveIfSame(s *Stream) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	current, exists := r.streams[s.ID]
	if !exists || current != s {
		return false
	}

	delete(r.streams, s.ID)
	return true
}
