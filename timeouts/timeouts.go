package timeouts

import (
	"fmt"
	"math/rand"
	"time"
)

type Timers struct {
	ElectionTimer  *time.Timer
	MaxElecTimeout time.Duration
	MinElecTimeout time.Duration
	rnd            *rand.Rand
}

var RaftTimeouts Timers

const (
	MAX_ELEC_TIMEOUT uint = 300
	MIN_ELEC_TIMEOUT uint = 150
)

func init() {
	RaftTimeouts = Timers{
		MaxElecTimeout: time.Duration(MAX_ELEC_TIMEOUT) * time.Millisecond,
		MinElecTimeout: time.Duration(MIN_ELEC_TIMEOUT) * time.Millisecond,
		rnd:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// RaftTimeouts.initElecTimer()
	fmt.Println("INITIALIZED RAFT TIMEOUTS")
}

func (t *Timers) initElecTimer() {
	if t.ElectionTimer == nil {
		t.ElectionTimer = time.NewTimer(t.GenerateElecTimeoutDuration())
	}
}

/**
* Starts a timeout after which, the server converts to candidate and sends RequestVoteRPC
 */
func (t *Timers) StartElectionTimeout(reset chan bool) {
	if t.ElectionTimer != nil {
		t.ElectionTimer.Stop()
		t.ElectionTimer.Reset(t.GenerateElecTimeoutDuration())

	} else {
		fmt.Println("TIMER IS NILL, INITIALIZING...")
		t.ElectionTimer = time.NewTimer(t.GenerateElecTimeoutDuration())
	}

	defer t.ElectionTimer.Stop()

	for {
		select {
		case l := <-reset:
			if l && t.ElectionTimer != nil {
				fmt.Println("Resetting election ticker...")
				t.ElectionTimer.Reset(t.GenerateElecTimeoutDuration())
			}
		case <-t.ElectionTimer.C:
			fmt.Printf("Election timout reached...\n")
			if t.ElectionTimer != nil {
				t.ElectionTimer.Stop()
			}
			reset <- true
			return
		}
	}
}

/**
* Resets election timer after receiving a heartbeat request from leader
 */
func (t *Timers) ResetElectionTimer() {
	fmt.Println("Resetting election timer...")
	if t.ElectionTimer != nil {
		t.ElectionTimer.Reset(t.GenerateElecTimeoutDuration())
	}
}

/*
* Cancels the election timer. This probably happens when the server is a leader
 */
func (t *Timers) CancelElectionTimer() {
	if t.ElectionTimer != nil {
		t.ElectionTimer.Stop()
		t.ElectionTimer = nil
	}
}

func (t *Timers) GenerateElecTimeoutDuration() (dur time.Duration) {
	diff := t.MaxElecTimeout - t.MinElecTimeout

	m := t.MinElecTimeout + time.Duration(t.rnd.Int63n(diff.Milliseconds()))

	fmt.Println("GENERATED RANDOME ELEC TIMEOUT => ", m)

	return m
}

func (t *Timers) GenerateHeartBeatDuration() (dur time.Duration) {
	return time.Duration(50) * time.Millisecond
}
