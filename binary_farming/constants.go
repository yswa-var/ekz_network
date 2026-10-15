package main

const (
	MaxMessageSize = 1024 * 1024
	MaxChunkSize   = 1024
)

const (
	FrameOpen  uint8 = 1
	FrameData  uint8 = 2
	FrameClose uint8 = 3
)
