package main

type DebeziumData struct {
	Operation string `json:"op"`
}

type DebeziumOperation string

const (
	InsertOperation   DebeziumOperation = "c"
	UpdateOperation   DebeziumOperation = "u"
	DeleteOperation   DebeziumOperation = "d"
	MessageOperation  DebeziumOperation = "m"
	SnapshotOperation DebeziumOperation = "r"
)

type DebeziumRowSource struct {
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	CommittedAtNs int64  `json:"ts_ns"`
}

type DebeziumRowData struct {
	Operation string                 `json:"op"`
	Before    map[string]interface{} `json:"before"`
	After     map[string]interface{} `json:"after"`
	Source    DebeziumRowSource      `json:"source"`
}
