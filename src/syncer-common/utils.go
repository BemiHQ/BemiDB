package syncerCommon

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

	"github.com/BemiHQ/BemiDB/src/common"
)

var DUCKDB_BOOT_QUERIES = []string{
	"SET memory_limit='2GB'",
	"SET threads=2",
}

func Int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
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

func StringToInt64(s string) int64 {
	int64Value, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}

	return int64Value
}

func StringToFloat64(s string) float64 {
	floatValue, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(err)
	}

	return floatValue
}

func StringDateToTime(str string) time.Time {
	// Golang's time.Parse() function does not support parsing dates with 5+ digit years
	// So we need to handle this case manually by parsing the year separately
	var nonStandardYear int
	parts := strings.Split(str, "-")
	if len(parts) == 3 && len(parts[0]) > 4 {
		nonStandardYear = StringToInt(parts[0])
		str = str[len(parts[0])-4:] // Remove the prefix from str leaving only the standard 10 characters (YYYY-MM-DD)
	}

	// Parse the date string as a standard date
	parsedTime, err := time.Parse("2006-01-02", str)

	// If the year is non-standard, add the year difference to the parsed time after parsing
	if err == nil && nonStandardYear != 0 {
		parsedTime = parsedTime.AddDate(nonStandardYear-parsedTime.Year(), 0, 0)
		return parsedTime
	}

	return parsedTime
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

func SendAnonymousAnalytics(config *common.CommonConfig, command string, name string) {
	if config.DisableAnonymousAnalytics {
		return
	}

	data := AnonymousAnalyticsData{
		Command:  command,
		OsName:   runtime.GOOS + "-" + runtime.GOARCH,
		Version:  common.VERSION,
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
