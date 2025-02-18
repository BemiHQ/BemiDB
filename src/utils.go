package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"unicode"

	"golang.org/x/crypto/pbkdf2"
)

func PanicIfError(err error, message ...string) {
	if err != nil {
		if len(message) == 1 {
			panic(fmt.Errorf(message[0]+": %w", err))
		}

		panic(err)
	}
}

func CreateTemporaryFile(prefix string) (file *os.File, err error) {
	tempFile, err := os.CreateTemp("", prefix)
	PanicIfError(err)

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
