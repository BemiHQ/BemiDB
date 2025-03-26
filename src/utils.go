package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"golang.org/x/crypto/pbkdf2"
)

func IntToString(i int) string {
	return strconv.Itoa(i)
}

func Int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

func StringToInt(s string) (int, error) {
	return strconv.Atoi(s)
}

func StringToInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
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

func HasExactOrWildcardMatch(strs []string, value string) bool {
	for _, str := range strs {
		if str == value {
			return true
		}

		if strings.Contains(str, "*") {
			pattern := strings.ReplaceAll(regexp.QuoteMeta(str), "\\*", ".*")
			matched, _ := regexp.MatchString("\\A"+pattern+"\\z", value)
			if matched {
				return true
			}
		}
	}

	return false
}

func ParseDatabaseHost(dbUrl string) string {
	if dbUrl == "" {
		return ""
	}

	url, err := url.Parse(dbUrl)
	if err != nil {
		return ""
	}

	return url.Hostname()
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
