package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"

	"raft/client"
	"raft/election"
	"raft/replication"
	"raft/state"
)

type Arithmetic struct{}

type Args struct {
	A, B int
}

var CurrentTerm int = 0
var VotedFor string
var Wg sync.WaitGroup

func main() {

	// set address
	if len(os.Args) < 2 {
		log.Fatal("Please provide server address")
		return
	}

	state.SetServerAdd(os.Args[1])
	formattedAddr := fmt.Sprintf("localhost%s", os.Args[1])
	fmt.Println("ADDR => ", formattedAddr)
	// commitedIndex := 0
	// lastApplied := 0

	// Read persistent state for current term and Voted for values
	Wg.Add(1)
	go state.InitializeState(&Wg)
	Wg.Wait()
	// Initialize election timeout
	go election.InitElectionFlow()

	// open RPC connections
	election := new(election.ElectionRPC)
	replicationRPC := new(replication.ReplicationRPC)
	clientRPC := new(client.ClientRPC)

	rpc.Register(election)
	rpc.Register(replicationRPC)
	rpc.Register(clientRPC)

	// Start listening on a specific port
	listener, err := net.Listen("tcp", state.ServerAddress)
	if err != nil {
		fmt.Println("Error starting listener:", err)

	}

	defer listener.Close()

	fmt.Printf("Listening on port %s\n\n", os.Args[1])
	for {

		conn, err := listener.Accept()

		if err != nil {
			log.Fatal(err)
			continue
		}

		fmt.Println("Received Connection")
		// Serve request in goroutine
		go rpc.ServeConn(conn)
	}

}
