package syncerCommon

import (
	"testing"

	"github.com/BemiHQ/BemiDB/src/common"
)

func TestJsonQueue(t *testing.T) {
	config := &common.CommonConfig{
		LogLevel: "DEBUG",
	}
	buffer := NewCappedBuffer(config, 1024)
	writer := NewJsonQueueWriter(buffer)
	reader := NewJsonQueueReader(buffer)

	original := map[string]interface{}{
		"name":  "test",
		"value": 42,
	}
	err := writer.Write(original)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	var result interface{}
	_, err = reader.Read(&result)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if original["name"] != result.(map[string]interface{})["name"] {
		t.Errorf("Expected name %s, got %s", original["name"], result.(map[string]interface{})["name"])
	}
	if float64(original["value"].(int)) != result.(map[string]interface{})["value"].(float64) {
		t.Errorf("Expected value %d, got %f", original["value"], result.(map[string]interface{})["value"])
	}
}
