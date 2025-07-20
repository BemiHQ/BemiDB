package common

import (
	"bytes"
	"encoding/json"
	"net/http"
	"runtime"
	"runtime/debug"
	"time"
)

func PanicIfError(config *BaseConfig, err error) {
	if err != nil {
		sendAnonymousErrorReport(config, err)
		panic(err)
	}
}

type AnonymousErrorData struct {
	Command    string `json:"command"`
	OsName     string `json:"osName"`
	Version    string `json:"version"`
	Error      string `json:"error"`
	StackTrace string `json:"stackTrace"`
	S3Bucket   string `json:"s3Bucket"`
}

func sendAnonymousErrorReport(config *BaseConfig, err error) {
	if config.DisableAnonymousAnalytics {
		return
	}

	data := AnonymousErrorData{
		Command:    "syncer",
		OsName:     runtime.GOOS + "-" + runtime.GOARCH,
		Version:    VERSION,
		Error:      err.Error(),
		StackTrace: string(debug.Stack()),
		S3Bucket:   config.Aws.S3Endpoint + "/" + config.Aws.S3Bucket,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}

	client := http.Client{Timeout: 5 * time.Second}
	_, _ = client.Post("https://api.bemidb.com/api/errors", "application/json", bytes.NewBuffer(jsonData))
}
