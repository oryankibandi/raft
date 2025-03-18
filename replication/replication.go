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
	Entries           [][]byte
	LeaderCommitIndex int
}

type AppendEntriesRes struct {
	Success              bool
	Term                 int
	FollowerLastLogIndex int
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

			if state.Node.Role != state.LEADER {
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
		if mem != state.Node.Ip {
			wg.Add(1)
			go sendAppendEntriesRPC(mem, &responses, make([][]byte, 0))
		}
	}

	wg.Wait()

	// Check if the required quota of servers have responded successfully
	if responses+1 < int(float64(len(members))/float64(2)) {
		state.Node.UpdateServerState(state.FOLLOWER)
		*revertToLeaderChan <- true
	}

}

/**
* Replicates logs across followers
**/
func ReplicateLogs(entries []string) (err error) {
	members := membership.GetClusterMembers()
	var entriesInBytes [][]byte = make([][]byte, 0)

	// convert string to bytes and append
	for _, entry := range entries {
		byteEntry := make([]byte, state.LOG_LENGTH-8)
		copy(byteEntry, entry)
		entriesInBytes = append(entriesInBytes, byteEntry)
	}

	responses := 0
	// loop through members and send heartbeat requests in parallel
	for _, mem := range members {
		if mem != state.Node.Ip {
			wg.Add(1)
			go sendAppendEntriesRPC(mem, &responses, entriesInBytes)
		}
	}

	wg.Wait()

	// Check if the required quota of servers have responded successfully
	if responses+1 < int(float64(len(members))/float64(2)) {
		state.Node.UpdateServerState(state.FOLLOWER)
		*revertToLeaderChan <- true

		return errors.New("Unable to replicate")
	}

	return nil
}

/**
* Send's append entries RPC to specified address. For heartbeats entries slice is empty
 */
func sendAppendEntriesRPC(serverAddr string, succResponses *int, entries [][]byte) {
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

	fmt.Println("APPENDENTRIES LENGTH => ", len(state.Node.Logs))
	// TODO: Switch PrevLogIndex with actual prev log index of follower node(minus no. of logs)

	args := &AppendEntriesArgs{Term: int(state.Node.Term), LeaderId: state.Node.Id, PrevLogIndex: max(int(membership.ClusterMembers.Members[addr])-1, 0), Entries: entries, LeaderCommitIndex: int(state.Node.CommitIndex), PrevLogTerm: state.Node.GetLastLogTerm(max(int(membership.ClusterMembers.Members[addr]-1), 0))}
	res := &AppendEntriesRes{}

	err = client.Call("ReplicationRPC.AppendEntriesRPC", args, &res)

	if err != nil {
		log.Println("ReplicationRPC ERR -> ", err)
		// log.Fatal("RPC error:", err)
		return
	}

	fmt.Println("FOLLOWER LAST LOG INDEX ==> ", res.FollowerLastLogIndex)

	// If a follower responds with a higher term, revert to follower
	if !res.Success && int64(res.Term) > state.Node.Term {
		heartBeatTicker.Stop()
		state.Node.IncrementTerm(&res.Term)
		state.Node.Role = state.FOLLOWER
		*revertToLeaderChan <- true
		// election.InitElectionFlow()

		fmt.Println("Reverting to follower.")
		return
	}

	if res.Success {
		mu.Lock()
		*succResponses += 1
		mu.Unlock()

		go membership.ClusterMembers.IncrementNodeNextIndex(addr, uint(len(entries)))
	}

	// If follower is missing logs, recalibrate and resend
	if !res.Success && res.FollowerLastLogIndex != 0 {
		fmt.Println("FOLLOWER LAST LOG INDEX DOES NOT MATCH ==>")
		if res.FollowerLastLogIndex < int(membership.ClusterMembers.Members[addr]) {
			// Reduce node nextIndex in state
			membership.ClusterMembers.DecrementNodeNextIndex(addr, uint(res.FollowerLastLogIndex))

		} else {
			membership.ClusterMembers.SetNodeNextIndex(addr, uint(res.FollowerLastLogIndex))
		}
		// Get logs from the new index onwards

		k := make([][]byte, 0)
		for _, v := range state.Node.Logs[res.FollowerLastLogIndex:] {
			k = append(k, v.Command)
		}

		// Resend Append Entry response
		sendAppendEntriesRPC(addr, succResponses, k)

		return
	}
}

/**
* Handles AppendEntriesRPC from leader which  includes heartbeats(empty AppendEntriesRPC)
 */
func (t *ReplicationRPC) AppendEntriesRPC(args *AppendEntriesArgs, appendRes *AppendEntriesRes) error {
	lenOfLogs := len(state.Node.Logs)
	fmt.Printf("(%s) REPLICATION RPC FROM %s with term %d\n", state.Node.Ip, args.LeaderId, args.Term)
	fmt.Printf("(%s) CURRENT LEADER => %s WITH TERM => %d\n", state.Node.Ip, state.Node.VotedFor, state.Node.Term)

	if args.Term < int(state.Node.Term) {
		fmt.Println("LEADER TERM IS LESS THAN MY TERM...")
		appendRes.Success = false

		return nil
	}

	// Log Matching check
	if args.PrevLogIndex > 0 && (lenOfLogs-1 < args.PrevLogIndex || state.Node.Logs[args.PrevLogIndex].Term != int64(args.PrevLogTerm)) {
		fmt.Println("ENTRY AT PREVLOGINDEX DOES NOT MATCH...")
		fmt.Printf("PREVLOGINDEX: => %d \t\t LOGLEN => %d\n", args.PrevLogIndex, lenOfLogs)

		if args.PrevLogIndex > lenOfLogs-1 {
			// Missing data. This follower is not up to date
			// Send back last log Index
			appendRes.FollowerLastLogIndex = lenOfLogs - 1

			appendRes.Success = false
			return nil
		}

		if lenOfLogs-1 > args.PrevLogIndex {
			fmt.Printf("LASTLOGTERM => %d \t\t LEADERLASTLOGTERM => %d\n", state.Node.Logs[args.PrevLogIndex].Term, args.PrevLogTerm)

			fmt.Println("CONFLICTING ENTRY => ", state.Node.Logs[args.PrevLogIndex])

			// panic("CONFLICTING ENTRIES")
			appendRes.Success = false
			appendRes.FollowerLastLogIndex = lenOfLogs - 1

		}
		appendRes.Success = false

		return nil
	}

	// Check term and update to leader's term if they don't match
	if int64(args.Term) > state.Node.Term {
		state.Node.SetTerm(args.Term)

		if args.LeaderId != state.Node.VotedFor {
			state.Node.SetVotedFor(args.LeaderId)
		}
	}

	timeouts.ResetElectionTimer()
	appendRes.Term = int(state.Node.Term)

	if len(args.Entries) <= 0 {
		appendRes.Success = true
		return nil
	}

	// TODO: Enter entries into log, increment logIndex, send back

	// err, _ := state.WriteToLogs(args.Entries)
	state.Node.AddEntries(args.Term, args.PrevLogIndex, args.Entries, args.LeaderCommitIndex)

	appendRes.Success = true
	return nil
}
