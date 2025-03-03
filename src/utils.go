package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"

	"golang.org/x/crypto/pbkdf2"
)

type AnonymousErrorData struct {
	DbHost     string `json:"dbHost"`
	OsName     string `json:"osName"`
	Version    string `json:"version"`
	Error      string `json:"error"`
	StackTrace string `json:"stackTrace"`
}

func PanicIfError(err error, config *Config, message ...string) {
	if err != nil {
		if config != nil {
			sendAnonymousErrorReport(config, err)
		}
		if len(message) == 1 {
			panic(fmt.Errorf(message[0]+": %w", err))
		}

		panic(err)
	}
}

func CreateTemporaryFile(prefix string) (file *os.File, err error) {
	tempFile, err := os.CreateTemp("", prefix)
	PanicIfError(err, nil)

	return tempFile, nil
}

func DeleteTemporaryFile(file *os.File) {
	os.Remove(file.Name())
}

func IntToString(i int) string {
	return strconv.Itoa(i)
}

func StringToInt(s string) (int, error) {
	return strconv.Atoi(s)
}

func StringToScramSha256(password string) string {
	saltLength := 16
	digestLength := 32
	iterations := 4096
	clientKey := []byte("Client Key")
	serverKey := []byte("Server Key")

	salt := make([]byte, saltLength)
	_, err := rand.Read(salt)
	if err != nil {
		return ""
	}

	digestKey := pbkdf2.Key([]byte(password), salt, iterations, digestLength, sha256.New)
	clientKeyHash := hmacSha256Hash(digestKey, clientKey)
	serverKeyHash := hmacSha256Hash(digestKey, serverKey)
	storedKeyHash := sha256Hash(clientKeyHash)

	return fmt.Sprintf(
		"SCRAM-SHA-256$%d:%s$%s:%s",
		iterations,
		base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(storedKeyHash),
		base64.StdEncoding.EncodeToString(serverKeyHash),
	)
}

func StringDateToTime(str string) (time.Time, error) {
	// Golang's time.Parse() function does not support parsing dates with 5+ digit years
	// So we need to handle this case manually by parsing the year separately
	var nonStandardYear int
	var err error
	parts := strings.Split(str, "-")
	if len(parts) == 3 && len(parts[0]) > 4 {
		nonStandardYear, err = StringToInt(parts[0])
		if err != nil {
			return time.Time{}, errors.New("Invalid year: " + parts[0])
		}

		str = str[len(parts[0])-4:] // Remove the prefix from str leaving only the standard 10 characters (YYYY-MM-DD)
	}

	parsedTime, err := time.Parse("2006-01-02", str)

	// If the year is non-standard, add the year difference to the parsed time after parsing
	if err == nil && nonStandardYear != 0 {
		parsedTime = parsedTime.AddDate(nonStandardYear-parsedTime.Year(), 0, 0)
		return parsedTime, nil
	}

	return parsedTime, err
}

func StringContainsUpper(str string) bool {
	for _, char := range str {
		if unicode.IsUpper(char) {
			return true
		}
	}
	return false
}

func hmacSha256Hash(key []byte, message []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(message)
	return hash.Sum(nil)
}

func sha256Hash(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}

func sendAnonymousErrorReport(config *Config, err error) {
	if config.DisableAnonymousAnalytics {
		LogInfo(config, "Anonymous analytics is disabled")
		return
	}

	stack := make([]byte, 4096)
	n := runtime.Stack(stack, false)
	stackTrace := string(stack[:n])

	var dbHost string
	if config != nil && config.Pg.DatabaseUrl != "" {
		dbUrl, err := url.Parse(config.Pg.DatabaseUrl)
		if err != nil {
			return
		}

		hostname := dbUrl.Hostname()
		switch hostname {
		case "localhost", "127.0.0.1", "::1", "0.0.0.0":
			return
		default:
			dbHost = hostname
		}
	}

	data := AnonymousErrorData{
		DbHost:     dbHost,
		OsName:     runtime.GOOS + "-" + runtime.GOARCH,
		Version:    config.Version,
		Error:      err.Error(),
		StackTrace: stackTrace,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}

	client := http.Client{Timeout: 5 * time.Second}
	_, _ = client.Post("https://api.bemidb.com/api/errors", "application/json", bytes.NewBuffer(jsonData))
}
