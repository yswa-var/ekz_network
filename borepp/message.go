package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

func SplitPayload(payload []byte, maxChunkSize int) [][]byte {
	if maxChunkSize <= 0 {
		return nil
	}

	var chunks [][]byte
	for len(payload) > 0 {
		chunkSize := maxChunkSize
		if chunkSize > len(payload) {
			chunkSize = len(payload)
		}

		chunks = append(chunks, payload[:chunkSize])
		payload = payload[chunkSize:]
	}

	return chunks
}

func Encode(payload []byte) []byte {
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	return buf
}

func Decode(reader io.Reader) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header)
	if length > MaxMessageSize {
		return nil, fmt.Errorf("message too large %d", length)
	}

	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}

	return payload, nil
}
