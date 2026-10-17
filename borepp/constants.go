package main

const (
	MaxMessageSize = 1024 * 1024
	MaxChunkSize   = 1024
)

const (
	FrameOpen         = 1
	FrameData         = 2
	FrameClose        = 3
	FrameWindowUpdate = 4
)
