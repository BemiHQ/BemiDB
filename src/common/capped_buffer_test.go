package common

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

func initTestConfig() *CommonConfig {
	return &CommonConfig{
		LogLevel: LOG_LEVEL_INFO, // Use INFO to avoid excessive logging during tests
	}
}

func TestCappedBufferWrite(t *testing.T) {
	t.Run("Writes data to buffer", func(t *testing.T) {
		config := initTestConfig()
		bufferSize := 100 // 100 bytes
		buffer := NewCappedBuffer(config, bufferSize)
		writeData := []byte("hello world")

		writtenBytes, err := buffer.Write(writeData)

		if err != nil {
			t.Fatalf("Failed to write to buffer: %v", err)
		}
		if writtenBytes != len(writeData) {
			t.Errorf("Expected to write %d bytes, but wrote %d", len(writeData), writtenBytes)
		}
	})

	t.Run("Waits to write data to a full buffer", func(t *testing.T) {
		config := initTestConfig()
		bufferSize := 11
		buffer := NewCappedBuffer(config, bufferSize)
		writeDataFull := []byte("hello world")
		buffer.Write(writeDataFull)
		writeDataOverflow := []byte("overflow")
		done := make(chan struct{})

		go func() {
			buffer.Write(writeDataOverflow)
			close(done)
		}()

		select {
		case <-done:
			t.Error("Write to full buffer should block, but it returned immediately")
		case <-time.After(100 * time.Millisecond):
			// This is expected - Write should block
		}
	})

	t.Run("Writes data to a buffer after it was read (more space available)", func(t *testing.T) {
		config := initTestConfig()
		bufferSize := 11
		buffer := NewCappedBuffer(config, bufferSize)
		writeDataFull := []byte("hello world")
		buffer.Write(writeDataFull)
		writeDataOverflow := []byte("over")
		done := make(chan struct{})

		go func() {
			writtenBytes, err := buffer.Write(writeDataOverflow)
			if err != nil {
				t.Errorf("Failed to write to buffer: %v", err)
			}
			if writtenBytes != len(writeDataOverflow) {
				t.Errorf("Expected to write %d bytes, but wrote %d", len(writeDataOverflow), writtenBytes)
			}

			close(done)
		}()

		readData := make([]byte, 5)
		readBytes, err := buffer.Read(readData)
		if err != nil {
			t.Fatalf("Failed to read from buffer: %v", err)
		}
		if readBytes != 5 {
			t.Errorf("Expected to read 5 bytes, but read %d", readBytes)
		}

		select {
		case <-done:
			// This is expected - Write should proceed
		case <-time.After(100 * time.Millisecond):
			t.Error("Write should have proceeded after Read")
		}
	})

	t.Run("Receive error when writing to a closed buffer", func(t *testing.T) {
		config := initTestConfig()
		bufferSize := 100
		buffer := NewCappedBuffer(config, bufferSize)
		writeData := []byte("hello world")

		buffer.Close()

		writtenBytes, err := buffer.Write(writeData)

		if err == nil {
			t.Error("Write to closed buffer should return an error")
		}
		if err.Error() != "buffer is closed" {
			t.Errorf("Expected error message 'buffer is closed', but got: %v", err)
		}
		if writtenBytes != 0 {
			t.Errorf("Expected to write 0 bytes, but wrote %d", writtenBytes)
		}
	})
}

func TestCappedBufferRead(t *testing.T) {
	t.Run("Reads data from buffer", func(t *testing.T) {
		config := initTestConfig()
		bufferSize := 100 // 100 bytes
		buffer := NewCappedBuffer(config, bufferSize)
		writeData := []byte("hello world")
		buffer.Write(writeData)
		readData := make([]byte, len(writeData))

		readBytes, err := buffer.Read(readData)

		if err != nil {
			t.Fatalf("Failed to read from buffer: %v", err)
		}
		if readBytes != len(writeData) {
			t.Errorf("Expected to read %d bytes, but read %d", len(writeData), readBytes)
		}
		if !bytes.Equal(readData, writeData) {
			t.Errorf("Read data does not match written data. Got %q, want %q", readData, writeData)
		}
	})

	t.Run("Waits to read data from an empty buffer", func(t *testing.T) {
		config := initTestConfig()
		bufferSize := 100 // 100 bytes
		buffer := NewCappedBuffer(config, bufferSize)
		done := make(chan struct{}) // Create a channel to signal when read is done
		readData := make([]byte, 10)

		// Start a goroutine to read from the buffer
		go func() {
			buffer.Read(readData)
			close(done)
		}()

		// Wait for a short time to see if Read blocks
		select {
		case <-done:
			t.Error("Read from empty buffer should block, but it returned immediately")
		case <-time.After(100 * time.Millisecond):
			// This is expected - Read should block
		}
	})

	t.Run("Waits and reads data from a buffer after it was closed", func(t *testing.T) {
		config := initTestConfig()
		bufferSize := 100 // 100 bytes
		buffer := NewCappedBuffer(config, bufferSize)
		writeData := []byte("hello world")
		buffer.Write(writeData)
		done := make(chan struct{}) // Create a channel to signal when read is done
		readData := make([]byte, 11)

		// Start a goroutine to read from the buffer
		go func() {
			readBytes, err := buffer.Read(readData)
			if err != nil {
				t.Errorf("Failed to read from buffer: %v", err)
			}
			if readBytes != len(writeData) {
				t.Errorf("Expected to read %d bytes, but read %d", len(writeData), readBytes)
			}
			close(done)
		}()

		buffer.Close() // Close the buffer to unblock the read

		// Wait for the read to complete
		select {
		case <-done:
			if !bytes.Equal(readData, writeData) {
				t.Errorf("Read data does not match written data. Got %q, want %q", readData, writeData)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Read should have returned after Close")
		}
	})

	t.Run("Reads data from a buffer after it was written to (more data available)", func(t *testing.T) {
		config := initTestConfig()
		bufferSize := 11
		buffer := NewCappedBuffer(config, bufferSize)
		readData := make([]byte, 5)
		done := make(chan struct{})
		writeData := []byte("hello world")

		go func() {
			readBytes, err := buffer.Read(readData)
			if err != nil {
				t.Errorf("Failed to read from buffer: %v", err)
			}
			if readBytes != len(readData) {
				t.Errorf("Expected to read %d bytes, but read %d", len(readData), readBytes)
			}
			if !bytes.Equal(readData, writeData[:len(readData)]) {
				t.Errorf("Read data does not match written data. Got %q, want %q", readData, writeData[:len(readData)])
			}
			close(done)
		}()

		buffer.Write(writeData)

		select {
		case <-done:
			// This is expected - Read should proceed
		case <-time.After(100 * time.Millisecond):
			t.Error("Read should have proceeded after Write")
		}
	})

	t.Run("Receive EOF when reading from a closed and empty buffer", func(t *testing.T) {
		config := initTestConfig()
		bufferSize := 100
		buffer := NewCappedBuffer(config, bufferSize)
		readData := make([]byte, 10)

		buffer.Close()

		readBytes, err := buffer.Read(readData)

		if err != io.EOF {
			t.Errorf("Read from closed and empty buffer should return EOF, but got: %v", err)
		}
		if readBytes != 0 {
			t.Errorf("Expected to read 0 bytes, but read %d", readBytes)
		}
	})
}

func TestCappedBufferConcurrentReadWrite(t *testing.T) {
	t.Run("Concurrent read and write operations", func(t *testing.T) {
		config := initTestConfig()
		bufferSize := 100 // 100 bytes
		buffer := NewCappedBuffer(config, bufferSize)
		iterations := 100
		writeData := []byte("test data")

		// WaitGroup to wait for all goroutines to complete
		var wg sync.WaitGroup
		wg.Add(2) // One for reader, one for writer

		// Start writer goroutine
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_, err := buffer.Write(writeData)
				if err != nil {
					t.Errorf("Write error at iteration %d: %v", i, err)
					return
				}
			}
		}()

		// Start reader goroutine
		go func() {
			defer wg.Done()
			readData := make([]byte, len(writeData))
			for i := 0; i < iterations; i++ {
				_, err := buffer.Read(readData)
				if err != nil {
					t.Errorf("Read error at iteration %d: %v", i, err)
					return
				}
				if !bytes.Equal(readData, writeData) {
					t.Errorf("Read data does not match at iteration %d. Got %q, want %q", i, readData, writeData)
					return
				}
			}
		}()

		// Wait for both goroutines to complete
		wg.Wait()
	})

	t.Run("Multiple sequential read and write operations", func(t *testing.T) {
		config := initTestConfig()
		bufferSize := 20
		buffer := NewCappedBuffer(config, bufferSize)
		data1 := []byte("first")
		data2 := []byte("second")
		data3 := []byte("third")
		readData1 := make([]byte, 5) // "first"
		readData2 := make([]byte, 6) // "second"
		readData3 := make([]byte, 5) // "third"

		buffer.Write(data1)
		buffer.Write(data2)
		buffer.Write(data3)

		readBytes1, _ := buffer.Read(readData1)
		if readBytes1 != 5 || string(readData1) != "first" {
			t.Errorf("First read failed: got %q, want %q", readData1, "first")
		}

		readBytes2, _ := buffer.Read(readData2)
		if readBytes2 != 6 || string(readData2) != "second" {
			t.Errorf("Second read failed: got %q, want %q", readData2, "second")
		}

		readBytes3, _ := buffer.Read(readData3)
		if readBytes3 != 5 || string(readData3) != "third" {
			t.Errorf("Third read failed: got %q, want %q", readData3, "third")
		}
	})
}
