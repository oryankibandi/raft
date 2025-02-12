package timeouts

import (
	"fmt"
	"time"

	"raft/utils"
)

var electionTimeoutTicker *time.Ticker

/**
* Starts a timeout after which, the server converts to candidate and sends RequestVoteRPC
 */
func StartElectionTimeout(reset chan bool) {
	electionTimeoutTicker = time.NewTicker(time.Second * time.Duration(utils.GenerateElectionTimeoutDuration()))

	for {
		select {
		case l := <-reset:
			if l && electionTimeoutTicker != nil {
				fmt.Println("Resetting election ticker...")
				electionTimeoutTicker.Reset(time.Second * time.Duration(utils.GenerateElectionTimeoutDuration()))
			}
		case <-electionTimeoutTicker.C:
			fmt.Printf("Election timout reached...\n")
			electionTimeoutTicker.Stop()
			reset <- true
			break
		}
	}
}

/**
* Resets election timer after receiving a heartbeat request from leader
 */
func ResetElectionTimer() {
	fmt.Println("Resetting election timer...")
	if electionTimeoutTicker != nil {
		electionTimeoutTicker.Reset(time.Second * time.Duration(utils.GenerateElectionTimeoutDuration()))
	}
}

/*
* Cancels the election timer. This probably happens when the server is a leader
 */
func CancelElectionTimer() {
	if electionTimeoutTicker != nil {
		electionTimeoutTicker.Stop()
		electionTimeoutTicker = nil
	}
}
