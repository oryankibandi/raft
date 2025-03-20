package utils

import (
	"math/rand"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

func RandomString(length int) string {
	rand.NewSource(time.Now().UnixNano()) // Seed to ensure randomness
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func GenerateElectionTimeoutDuration() (num int) {
	var n int
	n = rand.Intn(10)

	for n == 0 || n < 7 {
		n = rand.Intn(10)
	}
	return n
}

func GenerateHeartbeatDuration() (num int) {
	var n int

	n = rand.Intn(3)

	for n == 0 {
		n = rand.Intn(3)
	}
	return n
}
