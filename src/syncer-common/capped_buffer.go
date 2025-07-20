package common

import (
	"errors"
	"io"
	"sync"
)

type CappedBuffer struct {
	config       *BaseConfig
	maxSizeBytes int

	buffer          []byte
	mutex           sync.Mutex
	conditionalSync *sync.Cond

	closeOnceSync sync.Once
	closed        bool
}

func NewCappedBuffer(config *BaseConfig, maxSizeBytes int) *CappedBuffer {
	sizedBuffer := &CappedBuffer{
		config:       config,
		buffer:       make([]byte, 0, maxSizeBytes),
		maxSizeBytes: maxSizeBytes,
	}
	sizedBuffer.conditionalSync = sync.NewCond(&sizedBuffer.mutex)
	return sizedBuffer
}

// Implements io.Writer
func (buf *CappedBuffer) Write(payload []byte) (writtenBytes int, err error) {
	if len(payload) == 0 {
		return 0, nil
	}

	buf.mutex.Lock()
	defer buf.mutex.Unlock()

	if buf.closed {
		return 0, errors.New("buffer is closed")
	}

	for len(buf.buffer)+len(payload) > buf.maxSizeBytes && !buf.closed {
		LogTrace(buf.config, ">> Waiting for more space in capped buffer...")
		buf.conditionalSync.Wait() // Wait for the reader
	}

	// Check again if buffer was closed while waiting
	if buf.closed {
		return 0, errors.New("buffer is closed")
	}

	writtenBytes = len(payload)
	buf.buffer = append(buf.buffer, payload...)
	LogTrace(buf.config, ">> Writing", writtenBytes, "bytes to capped buffer...")

	buf.conditionalSync.Broadcast() // Notify the reader that new data is available

	return writtenBytes, nil
}

// Implements io.Reader
func (buf *CappedBuffer) Read(payload []byte) (readBytes int, err error) {
	if len(payload) == 0 {
		return 0, nil
	}

	buf.mutex.Lock()
	defer buf.mutex.Unlock()

	for len(buf.buffer) == 0 && !buf.closed {
		LogTrace(buf.config, "<< Waiting for more data in capped buffer...")
		buf.conditionalSync.Wait() // Wait for the writer
	}

	if len(buf.buffer) == 0 && buf.closed {
		return 0, io.EOF
	}

	maxReadBytes := len(payload)
	readBytes = copy(payload, buf.buffer)
	buf.buffer = buf.buffer[readBytes:]
	LogTrace(buf.config, "<< Reading "+IntToString(readBytes)+"/"+IntToString(maxReadBytes)+" bytes from capped buffer...")

	buf.conditionalSync.Broadcast() // Notify the writer that space is now available

	return readBytes, nil
}

func (buf *CappedBuffer) Close() error {
	buf.closeOnceSync.Do(func() {
		buf.mutex.Lock()

		LogTrace(buf.config, "== Closing capped buffer...")
		buf.closed = true

		buf.conditionalSync.Broadcast() // Wake up any waiting writers/readers

		buf.mutex.Unlock()
	})
	return nil
}
