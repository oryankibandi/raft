package replication

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"time"

	"raft/membership"
	"raft/state"
	"raft/timeouts"
	"raft/utils"
)

type ReplicationRPC struct{}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          string
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []string
	LeaderCommitIndex int
}

type AppendEntriesRes struct {
	Success bool
	Term    int
}

var mu sync.Mutex
var wg sync.WaitGroup

var heartBeatTicker *time.Ticker
var revertToLeaderChan *chan bool
var resetElectionTimerChan *chan bool

func StartHeartbeatTimer(revert *chan bool, resetElecTimerChan *chan bool) {
	revertToLeaderChan = revert
	resetElectionTimerChan = resetElecTimerChan
	heartBeatTicker = time.NewTicker(time.Second * time.Duration(utils.GenerateHeartbeatDuration()))

	for {
		select {
		case <-heartBeatTicker.C:
			fmt.Printf("Heartbeat timeout reached...\n")

			if state.ServerState != "leader" {
				fmt.Println("Server not leader, terminating heartbeat timer...")
				heartBeatTicker.Stop()
				break
			}

			heartBeatTicker.Reset(time.Second * time.Duration(utils.GenerateHeartbeatDuration()))
			// Send AppendEntryRPC Requests
			go disperseHeartbeatRequests()
		case rev := <-*revertToLeaderChan:
			if rev {
				*revert <- true
				break
			}
		default:
			continue
		}
	}
}

func disperseHeartbeatRequests() {
	members := membership.GetClusterMembers()

	responses := 0
	// loop through members and send heartbeat requests in parallel
	for _, mem := range members {
		if mem != state.ServerAddress {
			wg.Add(1)
			go sendAppendEntriesRPC(mem, &responses, make([]string, 0))
		}
	}

	wg.Wait()

	// Check if the required quota of servers have responded successfully
	if responses+1 < int(float64(len(members))/float64(2)) {
		state.UpdateServerState("follower")
		*revertToLeaderChan <- true
	}

}

/**
* Replicates logs across followers
**/
func ReplicateLogs(entries []string) (err error) {
	members := membership.GetClusterMembers()

	responses := 0
	// loop through members and send heartbeat requests in parallel
	for _, mem := range members {
		if mem != state.ServerAddress {
			wg.Add(1)
			go sendAppendEntriesRPC(mem, &responses, entries)
		}
	}

	wg.Wait()

	// Check if the required quota of servers have responded successfully
	if responses+1 < int(float64(len(members))/float64(2)) {
		state.UpdateServerState("follower")
		*revertToLeaderChan <- true

		return errors.New("Unable to replicate")
	}

	return nil
}

/**
* Send's append entries RPC to specified address. For heartbeats entries slice is empty
 */
func sendAppendEntriesRPC(serverAddr string, succResponses *int, entries []string) {
	defer wg.Done()
	addr := fmt.Sprintf("localhost%s", serverAddr)
	client, err := rpc.Dial("tcp", addr)

	if err != nil {
		fmt.Println("ERR connecting -> ", err.Error())
		// log.Printf("dialing:", err)

		return
	}

	defer client.Close()

	fmt.Println("Sending Heartbeat to ADDR: ", serverAddr)

	args := &AppendEntriesArgs{Term: state.CurrentTerm, LeaderId: state.ServerAddress, PrevLogIndex: state.LogIndex, Entries: entries, LeaderCommitIndex: state.CommitIndex}
	res := &AppendEntriesRes{}

	err = client.Call("ReplicationRPC.AppendEntriesRPC", args, &res)

	if err != nil {
		log.Println("ReplicationRPC ERR -> ", err)
		// log.Fatal("RPC error:", err)
		return
	}

	// If a follower responds with a higher term, revert to follower
	if !res.Success && res.Term > state.CurrentTerm {
		heartBeatTicker.Stop()
		state.IncrementTerm(&res.Term)
		state.UpdateServerState("follower")
		*revertToLeaderChan <- true
		// election.InitElectionFlow()

		fmt.Println("Reverting to follower.")
		return
	}

	if res.Success {
		mu.Lock()
		*succResponses += 1
		mu.Unlock()
	}
}

/**
* Handles AppendEntriesRPC from leader which  includes heartbeats(empty AppendEntriesRPC)
 */
func (t *ReplicationRPC) AppendEntriesRPC(args *AppendEntriesArgs, appendRes *AppendEntriesRes) error {
	fmt.Printf("(%s) REPLICATION RPC FROM %s with term %d\n", state.ServerAddress, args.LeaderId, args.Term)
	fmt.Printf("(%s) CURRENT LEADER => %s WITH TERM => %d\n", state.ServerAddress, state.VotedFor, state.CurrentTerm)

	if args.LeaderId != state.VotedFor && args.Term <= state.CurrentTerm {
		fmt.Println("Received RPC from wrong leader...")
		appendRes.Success = false

		return nil
	}

	if args.Term < state.CurrentTerm {
		appendRes.Success = false

		return nil
	}

	// TODO:: Add checks for last log term and commit
	// Check term and update to leader's term if they don't match
	if args.Term > state.CurrentTerm && state.VotedFor == args.LeaderId {
		state.SetTerm(args.Term)
	} else if args.Term > state.CurrentTerm && state.VotedFor != args.LeaderId {
		fmt.Println("PREV LOG INDEX => ", args.PrevLogIndex)
		// if another node has a higher term
		if args.PrevLogIndex >= state.LogIndex {
			fmt.Println("leader log index > logIndex")
			state.SetVotedFor(args.LeaderId)
			state.SetTerm(args.Term)
		} else {
			fmt.Println("Leader log index < logIndex")
			return nil
		}
	}

	timeouts.ResetElectionTimer()
	appendRes.Term = state.CurrentTerm

	if len(args.Entries) <= 0 {
		appendRes.Success = true
		return nil
	}

	// TODO: Enter entries into log, increment logIndex, send back

	err, _ := state.WriteToLogs(args.Entries)

	if err != nil {
		appendRes.Success = false
		return nil
	}

	appendRes.Success = true
	return nil
}
