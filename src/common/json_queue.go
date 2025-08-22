package common

import (
	"encoding/binary"
	"encoding/json"
	"io"
)

type JsonQueueWriter struct {
	Writer io.Writer
}

func NewJsonQueueWriter(w io.Writer) *JsonQueueWriter {
	return &JsonQueueWriter{Writer: w}
}

func (w *JsonQueueWriter) Write(value interface{}) error {
	jsonData, err := json.Marshal(value)
	if err != nil {
		return err
	}

	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(jsonData)))

	_, err = w.Writer.Write(lenBytes)
	if err != nil {
		return err
	}

	_, err = w.Writer.Write(jsonData)
	return err
}

func (w *JsonQueueWriter) Close() error {
	if closer, ok := w.Writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// -------------------------------------------------------------------------------------------------

type JsonQueueReader struct {
	Reader io.Reader
}

func NewJsonQueueReader(r io.Reader) *JsonQueueReader {
	return &JsonQueueReader{Reader: r}
}

func (r *JsonQueueReader) Read(value interface{}) (int, error) {
	lenBytes := make([]byte, 4)
	_, err := io.ReadFull(r.Reader, lenBytes)
	if err != nil {
		return 0, err // Propagate io.EOF or any other errors
	}

	length := binary.BigEndian.Uint32(lenBytes)

	jsonData := make([]byte, length)
	_, err = io.ReadFull(r.Reader, jsonData)
	if err != nil {
		return 0, err // This will return io.ErrUnexpectedEOF if the stream is closed before the full message is read.
	}

	err = json.Unmarshal(jsonData, value)
	if err != nil {
		return 0, err
	}

	return int(length), nil
}

func (r *JsonQueueReader) Close() error {
	if closer, ok := r.Reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
