package timeouts

import (
	"fmt"
	"time"

	"raft/utils"
)

type Timers struct {
	ElectionTimer *time.Ticker
}

var RaftTimeouts Timers

/**
* Starts a timeout after which, the server converts to candidate and sends RequestVoteRPC
 */
func (t *Timers) StartElectionTimeout(reset chan bool) {
	if t.ElectionTimer == nil {
		t.ElectionTimer = time.NewTicker(time.Second * time.Duration(utils.GenerateElectionTimeoutDuration()))
	}

	for {
		select {
		case l := <-reset:
			if l && t.ElectionTimer != nil {
				fmt.Println("Resetting election ticker...")
				t.ElectionTimer.Reset(time.Second * time.Duration(utils.GenerateElectionTimeoutDuration()))
			}
		case <-t.ElectionTimer.C:
			fmt.Printf("Election timout reached...\n")
			if t.ElectionTimer != nil {
				t.ElectionTimer.Stop()
			}
			reset <- true
			break
		}
	}
}

/**
* Resets election timer after receiving a heartbeat request from leader
 */
func (t *Timers) ResetElectionTimer() {
	fmt.Println("Resetting election timer...")
	if t.ElectionTimer != nil {
		t.ElectionTimer.Reset(time.Second * time.Duration(utils.GenerateElectionTimeoutDuration()))
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
