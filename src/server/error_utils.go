package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"time"
)

func PanicIfError(config *Config, err error) {
	if err != nil {
		sendAnonymousErrorReport(config, err)
		printUnexpectedError(config, err)
		os.Exit(1)
	}
}

func Panic(config *Config, message string) {
	err := errors.New(message)
	PanicIfError(config, err)
}

func PrintErrorAndExit(config *Config, message string) {
	LogError(config, message+"\n")
	os.Exit(1)
}

func HandleUnexpectedError(config *Config, err error) {
	sendAnonymousErrorReport(config, err)
	printUnexpectedError(config, err)
	os.Exit(1)
}

func printUnexpectedError(config *Config, err error) {
	errorMessage := err.Error()
	stackTrace := string(debug.Stack())

	fmt.Println("Unexpected error:", errorMessage)
	fmt.Println(stackTrace)
}

type AnonymousErrorData struct {
	Command    string `json:"command"`
	OsName     string `json:"osName"`
	Version    string `json:"version"`
	Error      string `json:"error"`
	StackTrace string `json:"stackTrace"`
	S3Bucket   string `json:"s3Bucket"`
}

func sendAnonymousErrorReport(config *Config, err error) {
	if config.DisableAnonymousAnalytics {
		return
	}

	data := AnonymousErrorData{
		Command:    "server",
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
