package state

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
)

type PersistentState struct {
	CurrentTerm int    `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
	CommitIndex int    `json:"commitIndex"`
}

const (
	ServerId = "4c888b1c70009953"
)

var ServerAddress string
var CurrentTerm int = 0
var VotedFor string
var ServerState string = "follower"
var mu sync.Mutex
var CommitIndex int = 0 // index of the highest log entry known to be committed
var LastApplied int = 0 // index of the highest log entry applied to state machine i.e. Not index of log or committed index but logs that have been applied to state machine
var LogIndex int = 0

func SetServerAdd(addr string) {
	ServerAddress = addr
}

/**
*  Scan the log and get the index of last log entry
 */
func getLogIndex() {
	var logIdx int = 0

	f, err := os.OpenFile("raft.log", os.O_CREATE|os.O_RDONLY, 0644)

	if err != nil {
		log.Printf("Unable to open file => ")
		fmt.Println(err.Error())
		return
	}

	defer f.Close()
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		logIdx++
	}

	fmt.Println("LogIndex => ", logIdx)

	if logIdx-1 >= 0 {
		LogIndex = logIdx - 1
	} else {
		LogIndex = logIdx
	}
}

func InitializeState(wg *sync.WaitGroup) {
	go getLogIndex()

	persData, err := ReadPersistentState(wg)

	if err != nil {
		fmt.Println("Unable to read persistent state")
		log.Fatal(err.Error())
	} else {

		// set variables CommitedIndex and VotedDor
		VotedFor = persData.VotedFor
		CurrentTerm = persData.CurrentTerm
		CommitIndex = persData.CommitIndex
	}

}

func IncrementTerm(newTerm *int) {
	mu.Lock()

	if newTerm != nil {
		CurrentTerm = *newTerm
	} else {
		CurrentTerm++
	}

	mu.Unlock()
	WriteToPersistentState(CurrentTerm, &VotedFor, CommitIndex)
}

func DecrementTerm() {
	mu.Lock()
	CurrentTerm = CurrentTerm - 1
	mu.Unlock()
	WriteToPersistentState(CurrentTerm, &VotedFor, CommitIndex)
}

func SetVotedFor(candidateId string) {
	mu.Lock()
	VotedFor = candidateId
	mu.Unlock()

	WriteToPersistentState(CurrentTerm, &candidateId, CommitIndex)
}

/**
*  Sets a new term to state(volatile and persisent)
 */
func SetTerm(newTerm int) {
	mu.Lock()

	if newTerm != 0 {
		CurrentTerm = newTerm
	} else {
		CurrentTerm++
	}

	mu.Unlock()
	WriteToPersistentState(CurrentTerm, &VotedFor, CommitIndex)

}

/**
*  Sets a new CommitIndex to state(volatile and persisent)
 */
//func SetTerm(commIdx int) {
//	WriteToPersistentState(CurrentTerm, nil, CommitIndex)

//}

func ReadPersistentState(wg *sync.WaitGroup) (persData PersistentState, err error) {
	defer wg.Done()

	var state PersistentState

	f, err := os.OpenFile("server_data.json", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	stat, err := f.Stat()

	if err != nil {
		log.Fatal(err.Error())
		return state, err
	}

	fmt.Println("FILE ENGTH => %d", stat.Size())

	// if file is empty, initialize values to zero
	if stat.Size() <= 0 {
		WriteToPersistentState(0, &VotedFor, 0)
	}

	j := json.NewDecoder(f)
	j.Decode(&state)

	return state, nil
}

func UpdateServerState(newState string) {
	defer mu.Unlock()
	if newState != "candidate" && newState != "follower" && newState != "leader" {
		log.Fatal("State can only be `candidate`, `leader` or `follower`")
		return
	}

	mu.Lock()
	ServerState = newState

	// If reverting to follower, clear votedFor update persistent state
	// if newState == "follower" {
	//		state.VotedFor = ""
	//		WriteToPersistentState(state.CurrentTerm, state.VotedFor, state.CommitIndex)
	//	}

}

func WriteToPersistentState(term int, votedFor *string, commitIndex int) {
	newState := PersistentState{}

	if term != 0 {
		newState.CurrentTerm = term
	}

	if votedFor != nil {
		newState.VotedFor = *votedFor
	}

	if commitIndex != 0 {
		newState.CommitIndex = commitIndex
	}

	f, err := os.OpenFile("./server_data.json", os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatal(err)

		return
	}

	defer f.Close()

	fmt.Println("PersistentState STRUCT => %q", newState)

	// Encode the Go struct to JSON and write to the file
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ") // For pretty-printing
	err = encoder.Encode(newState)
	if err != nil {
		fmt.Println("Error encoding JSON:", err)
		return
	}

}

/**
* Writes logs from AppendEntryRPC to log file
 */
func WriteToLogs(logs []string) (err error, success bool) {
	f, err := os.OpenFile("raft.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend)

	if err != nil {
		fmt.Println(err.Error())
		return err, false
	}

	defer f.Close()

	for _, log := range logs {
		newLog, err := addMetadataToLog(log)

		if err != nil {
			fmt.Println("Invalid log => ", log)

			return err, false
		}

		if _, err := f.Write([]byte(newLog)); err != nil {
			fmt.Println("Unable to write log => ", err)
			f.Close()
			break
		}

		// Update commit index
		mu.Lock()
		CommitIndex++
		LogIndex++
		mu.Unlock()
	}

	// Ensure commit index is updated:WriteToLogs
	WriteToPersistentState(CurrentTerm, &VotedFor, CommitIndex)

	return nil, true
}

/**
* Prepends term and index to log
 */
func addMetadataToLog(entry string) (newEntry string, err error) {
	if entry == "" {
		return "", errors.New("Invalid entry provided")
	}

	newLog := fmt.Sprintf("<%d , %d, %s>\n", LogIndex+1, CurrentTerm, entry)

	return newLog, nil

}
