package client

import (
	"fmt"

	"raft/replication"
	"raft/state"
)

type ClientRPC struct{}

type ClientRequestRPC struct {
	Entries []string
}

type ClientResponse struct {
	Success       bool
	CurrentLeader string
}

/*
* RPC method that handles replication request from client. Client communicate via RPC and call this method with entries that need to be added.
 */
func (t *ClientRPC) ClientReplicationRequest(args ClientRequestRPC, clientRes *ClientResponse) error {
	// TODO: Check if is leader,if isn't redirect to leader

	if state.Node.Role != state.LEADER {
		clientRes.CurrentLeader = state.Node.VotedFor
		clientRes.Success = false

		return nil
	}

	if len(args.Entries) <= 0 {
		clientRes.Success = true
		return nil
	}

	byteEntr := make([][]byte, 0)

	for _, v := range args.Entries {
		k := make([]byte, state.LOG_LENGTH-8)
		copy(k, v)
		byteEntr = append(byteEntr, k)
	}

	// Write to leader's logs
	err := state.Node.AppendLeaderEntry(byteEntr)

	if err != nil {
		clientRes.Success = false
		return nil
	}

	// Send to followers
	err = replication.ReplicateLogs(args.Entries)

	if err != nil {
		fmt.Println("Unable to replicate => ", err.Error())

		clientRes.Success = false
		return nil
	}

	clientRes.Success = true

	return nil
}
