package election

import (
	"fmt"
	"log"
	"math"
	"net/rpc"
	"sync"

	"raft/membership"
	"raft/replication"
	"raft/state"
	"raft/timeouts"
)

// Arithmetic provides methods for RPC
type ElectionRPC struct{}

type RequestVoteArgs struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

var Wg sync.WaitGroup
var mu sync.Mutex

/**
* Start election flow after startup
 */
func InitElectionFlow() {
	fmt.Println("Initializing election flow...")
	electionTimeoutChann := make(chan bool)

	go timeouts.StartElectionTimeout(electionTimeoutChann)

	for {
		select {
		case l := <-electionTimeoutChann:
			fmt.Println("Received election timout signal..")
			if l {
				// If election timeout, increment term and send RequestVotRPC to all servers

				// Start election in another goroutine
				go StartElection()
				// Close channel
				break
			}
		default:
			continue
		}

	}

}

func voteForSelf(votes *int) {
	if votes == nil {
		log.Fatal("votes pointer is required")
		return
	}

	state.SetVotedFor(state.ServerAddress)
	*votes++
}

/**
* Sends RPC Requests to other servers requesting for votes
**/
func StartElection() {
	// 1. Increment term and change state
	// 2. Read from file, the IPs of the other servers
	// 3. Vote for self, then, for each IP, send a RequestVote RPC
	// 4. If result is true, increment a vote counter
	// 5. Once all servers have responded, if quota is reached convert to leader else revert back to follower

	var votes int = 0

	state.IncrementTerm(nil)
	state.UpdateServerState("candidate")

	voteForSelf(&votes)
	members := membership.GetClusterMembers()

	// loop through members and send requests in parallel
	for _, mem := range members {
		if mem != state.ServerAddress {
			Wg.Add(1)
			go requestVote(mem, &votes)
		}
		fmt.Println("Started REQ VOTE RPCs..")
	}

	Wg.Wait()

	var result float64 = float64(len(members)) / float64(2)

	if votes >= int(math.Round(result)) {
		state.UpdateServerState("leader")
		timeouts.CancelElectionTimer()
		go initLeaderFlow()
	} else {
		state.DecrementTerm()
		state.UpdateServerState("follower")
		go InitElectionFlow()
	}

}

func requestVote(serverAddr string, votes *int) {
	defer Wg.Done()

	addr := fmt.Sprintf("localhost%s", serverAddr)
	client, err := rpc.Dial("tcp", addr)

	if err != nil {
		log.Println("ERR connecting -> ", err.Error())

		fmt.Println("EXITING...")
		return
	}

	defer client.Close()

	fmt.Println("NOT EXITED")

	fmt.Println("Client Connected...")

	fmt.Println("Sending RequestVoteRPC to ADDR: ", serverAddr)

	args := &RequestVoteArgs{Term: state.CurrentTerm, CandidateId: state.ServerAddress, LastLogIndex: state.CommitIndex, LastLogTerm: state.CurrentTerm}
	res := &RequestVoteResponse{}

	err = client.Call("ElectionRPC.RequestVoteRPC", args, &res)

	if err != nil {
		log.Println("Unable to send RequestVoteRPC: ", err)
		return
	}

	// Mock fxn to grant or deny vote. Ideally should do a consistency check

	if res.VoteGranted {
		mu.Lock()
		*votes += 1
		mu.Unlock()
	}
}

func (t *ElectionRPC) RequestVoteRPC(args *RequestVoteArgs, reqVoteRes *RequestVoteResponse) error {
	// If candidate term is less or equal, deny vote
	if args.Term < state.CurrentTerm {
		reqVoteRes.VoteGranted = false
		return nil
	}

	if args.LastLogIndex >= state.CommitIndex {
		timeouts.ResetElectionTimer()
		reqVoteRes.VoteGranted = true
		state.SetVotedFor(args.CandidateId)

		fmt.Println("Voted For ===> ", args.CandidateId)
		return nil
	}

	fmt.Printf("(%s) Did not vote for %s", state.ServerAddress, args.CandidateId)

	reqVoteRes.VoteGranted = false
	return nil
}

/**
* Resets election timer after receiving a heartbeat request from leader
 */
//func ResetElectionTimer() {
//	if electionTimeoutTicker != nil {
//		electionTimeoutTicker.Reset(time.Second * time.Duration(utils.GenerateElectionTimeoutDuration()))
//	}
//}

func initLeaderFlow() {
	leaderFlowChan := make(chan bool)
	resetElecChan := make(chan bool)
	go replication.StartHeartbeatTimer(&leaderFlowChan, &resetElecChan)

	// start loop and listem to both  channels
	for {
		select {
		case l := <-leaderFlowChan:
			if l {
				// electionTimeoutTicker = nil
				InitElectionFlow()
				break
			}
			// reset election timer handler
		case k := <-resetElecChan:
			if k {
				timeouts.ResetElectionTimer()
			}
		default:
			continue
		}

	}
}
