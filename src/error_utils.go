package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

func PanicIfError(config *Config, err error) {
	if err != nil {
		sendAnonymousErrorReport(config, err)
		printUnexpectedError(config, err)
		os.Exit(1)
	}
}

func HandleUnexpectedError(config *Config, err error) {
	sendAnonymousErrorReport(config, err)
	printUnexpectedError(config, err)
	os.Exit(1)
}

func printUnexpectedError(config *Config, err error) {
	errorMessage := err.Error()
	stackTrace := string(debug.Stack())

	title := "Unexpected error: " + strings.Split(errorMessage, "\n")[0]
	body := "* Version: " + VERSION +
		"\n* OS: " + runtime.GOOS + "-" + runtime.GOARCH +
		"\n\n```\n" + errorMessage + "\n\n" + stackTrace + "\n```"

	fmt.Println("Unexpected error:", errorMessage)
	fmt.Println(stackTrace)
	fmt.Println("________________________________________________________________________________")
	fmt.Println("\nPlease submit a new issue by simply visiting the following link:")
	fmt.Println(
		"https://github.com/BemiHQ/BemiDB/issues/new?title=" +
			url.QueryEscape(title) +
			"&body=" +
			url.QueryEscape(body),
	)
	fmt.Println("\nAlternatively, send us an email at hi@bemidb.com")
}

type AnonymousErrorData struct {
	Command    string `json:"command"`
	OsName     string `json:"osName"`
	Version    string `json:"version"`
	Error      string `json:"error"`
	StackTrace string `json:"stackTrace"`
	PgHost     string `json:"pgHost"`
}

func sendAnonymousErrorReport(config *Config, err error) {
	if config.DisableAnonymousAnalytics {
		return
	}

	data := AnonymousErrorData{
		Command:    flag.Arg(0),
		OsName:     runtime.GOOS + "-" + runtime.GOARCH,
		Version:    VERSION,
		Error:      err.Error(),
		StackTrace: string(debug.Stack()),
		PgHost:     ParseDatabaseHost(config.Pg.DatabaseUrl),
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}

	client := http.Client{Timeout: 5 * time.Second}
	_, _ = client.Post("https://api.bemidb.com/api/errors", "application/json", bytes.NewBuffer(jsonData))
}
