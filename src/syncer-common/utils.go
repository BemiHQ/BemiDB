package common

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func IntToString(i int) string {
	return strconv.Itoa(i)
}

func Float64ToString(i float64) string {
	return strconv.FormatFloat(i, 'f', -1, 64)
}

func StringToInt(s string) int {
	int, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}

	return int
}

func HexToString(s string) (string, error) {
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

func Base64ToHex(s string) string {
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return strings.ToUpper(hex.EncodeToString(decoded))
}

func TimeToUtcStringMs(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.999999")
}

func StringMsToUtcTime(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05.999999", s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

type AnonymousAnalyticsData struct {
	Command  string `json:"command"`
	OsName   string `json:"osName"`
	Version  string `json:"version"`
	S3Bucket string `json:"s3Bucket"`
	Name     string `json:"name"`
}

func SendAnonymousAnalytics(config *BaseConfig, command string, name string) {
	if config.DisableAnonymousAnalytics {
		return
	}

	data := AnonymousAnalyticsData{
		Command:  command,
		OsName:   runtime.GOOS + "-" + runtime.GOARCH,
		Version:  VERSION,
		S3Bucket: config.Aws.S3Endpoint + "/" + config.Aws.S3Bucket,
		Name:     name,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}

	client := http.Client{Timeout: 5 * time.Second}
	_, _ = client.Post("https://api.bemidb.com/api/analytics", "application/json", bytes.NewBuffer(jsonData))
}
