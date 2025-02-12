package utils

import (
	"math/rand"
)

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
