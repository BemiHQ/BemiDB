package syncerCommon

import (
	"errors"
	"io"
	"sync"

	"github.com/BemiHQ/BemiDB/src/common"
)

type CappedBuffer struct {
	Config       *common.CommonConfig
	MaxSizeBytes int

	buffer          []byte
	mutex           sync.Mutex
	conditionalSync *sync.Cond

	closeOnceSync sync.Once
	closed        bool
}

func NewCappedBuffer(config *common.CommonConfig, maxSizeBytes int) *CappedBuffer {
	sizedBuffer := &CappedBuffer{
		Config:       config,
		buffer:       make([]byte, 0, maxSizeBytes),
		MaxSizeBytes: maxSizeBytes,
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

	for len(buf.buffer)+len(payload) > buf.MaxSizeBytes && !buf.closed {
		common.LogTrace(buf.Config, ">> Waiting for more space in capped buffer...")
		buf.conditionalSync.Wait() // Wait for the reader
	}

	// Check again if buffer was closed while waiting
	if buf.closed {
		return 0, errors.New("buffer is closed")
	}

	writtenBytes = len(payload)
	buf.buffer = append(buf.buffer, payload...)
	common.LogTrace(buf.Config, ">> Writing", writtenBytes, "bytes to capped buffer...")

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
		common.LogTrace(buf.Config, "<< Waiting for more data in capped buffer...")
		buf.conditionalSync.Wait() // Wait for the writer
	}

	if len(buf.buffer) == 0 && buf.closed {
		return 0, io.EOF
	}

	maxReadBytes := len(payload)
	readBytes = copy(payload, buf.buffer)
	buf.buffer = buf.buffer[readBytes:]
	common.LogTrace(buf.Config, "<< Reading "+common.IntToString(readBytes)+"/"+common.IntToString(maxReadBytes)+" bytes from capped buffer...")

	buf.conditionalSync.Broadcast() // Notify the writer that space is now available

	return readBytes, nil
}

func (buf *CappedBuffer) Close() error {
	buf.closeOnceSync.Do(func() {
		buf.mutex.Lock()

		common.LogTrace(buf.Config, "== Closing capped buffer...")
		buf.closed = true

		buf.conditionalSync.Broadcast() // Wake up any waiting writers/readers

		buf.mutex.Unlock()
	})
	return nil
}
