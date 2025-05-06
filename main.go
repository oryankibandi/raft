package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"time"

	"raft/client"
	"raft/election"
	kvstore "raft/kv_store"
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

func init() {
	if len(os.Args) < 3 {
		log.Fatal("Please provide server address and key value store api address")
		return
	}

	Wg.Add(1)
	go state.InitializeState(&Wg, os.Args[1])
	Wg.Wait()
	fmt.Println("Initialized state")

}

func main() {

	formattedAddr := fmt.Sprintf("localhost%s", os.Args[1])
	fmt.Println("ADDR => ", formattedAddr)
	// open RPC connections
	elec := new(election.ElectionRPC)
	replicationRPC := new(replication.ReplicationRPC)
	clientRPC := new(client.ClientRPC)

	rpc.Register(elec)
	rpc.Register(replicationRPC)
	rpc.Register(clientRPC)

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	// Start listening on a specific port
	fmt.Println("POrt => ", state.Node.Ip)
	listener, err := net.Listen("tcp", state.Node.Ip)
	if err != nil {
		fmt.Println("Error starting listener:", err)

	}

	// defer listener.Close()

	// Initialize key value store
	// kvstore.InitiateKVState()

	go kvstore.InitializeApi(os.Args[2])

	fmt.Printf("Listening on port %s\n\n", os.Args[1])
	// Initialize election timeout
	go election.InitElectionFlow()

	go func() {
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
	}()

	<-stop
	fmt.Println("Shuting down Raft...")
	state.Node.Fd.Close()

	listener.Close()

	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

}
