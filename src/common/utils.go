package common

import (
	"strconv"
	"strings"
)

func IntToString(i int) string {
	return strconv.Itoa(i)
}

func IsLocalHost(host string) bool {
	return strings.HasPrefix(host, "127.0.0.1") || strings.HasPrefix(host, "localhost")
}
