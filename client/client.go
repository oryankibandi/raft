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

	if state.ServerState != "leader" {
		clientRes.CurrentLeader = state.VotedFor
		clientRes.Success = false

		return nil
	}

	if len(args.Entries) <= 0 {
		clientRes.Success = true
		return nil
	}

	// Write to leader's logs
	err, _ := state.WriteToLogs(args.Entries)

	if err != nil {
		clientRes.Success = false
		return nil
	}

	// Send to followers
	errr := replication.ReplicateLogs(args.Entries)

	if errr != nil {
		fmt.Println("Unable to replicate => ", err.Error())

		clientRes.Success = false
		return nil
	}

	clientRes.Success = true

	return nil
}
