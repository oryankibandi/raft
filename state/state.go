package state

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"raft/utils"
	"sync"
)

type PersistentState struct {
	CurrentTerm int    `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
	CommitIndex int    `json:"commitIndex"`
}

const (
	LOG_LENGTH          = 64
	FOLLOWER   NodeRole = "follower"
	LEADER     NodeRole = "leader"
	CANDIDATE  NodeRole = "candidate"
)

type NodeRole string

var mu sync.Mutex
var LogIndex int = 0
var Node *Server

// Entry Struct
type Entry struct {
	Term    int64
	Command []byte
}

// Server Struct
type Server struct {
	Id          string
	Ip          string
	Role        NodeRole
	CommitIndex int64
	Term        int64
	Logs        []Entry
	Mu          sync.Mutex
	Fd          *os.File
	VotedFor    string
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

func InitializeState(wg *sync.WaitGroup, ip string) {
	defer wg.Done()
	//	go getLogIndex()

	//	persData, err := ReadPersistentState(wg)
	//
	//	if err != nil {
	//		fmt.Println("Unable to read persistent state")
	//		log.Fatal(err.Error())
	//	} else {
	//
	//		// set variables CommitedIndex and VotedDor
	//		VotedFor = persData.VotedFor
	//		CurrentTerm = persData.CurrentTerm
	//		CommitIndex = persData.CommitIndex
	//	}

	// Initialize server struct
	// restore from persistent storage
	f, err := os.OpenFile("raft.log", os.O_CREATE|os.O_RDWR, 0666)

	if err != nil {
		fmt.Println("ERR => ", err)
		panic("Unable to open or create log file")
	}

	node := Server{
		Id:          utils.RandomString(8),
		Fd:          f,
		Role:        FOLLOWER,
		Ip:          ip,
		CommitIndex: 0,
		Term:        0,
		VotedFor:    "",
	}

	Node = &node

	Node.restore()
	fmt.Println("NODE DATA => ", Node)
}

func (s *Server) AppendLeaderEntry(entries [][]byte) (err error) {
	fmt.Println("APPEDNING ELADER ENTRY ...")
	for _, entry := range entries {
		n := Entry{
			Term:    s.Term,
			Command: entry,
		}

		s.Logs = append(s.Logs, n)
	}

	// Persist
	err, _ = s.Persist(len(entries), false, false)

	if err != nil {
		return err
	}

	return nil
}

/**
* Adds entries to Logs (volatile state)
 */
func (s *Server) AddEntries(term int, prevLogIndex int, command [][]byte, leaderCommit int) (err error, n int) {
	fmt.Println("prevlogindex in AddEntries => ", prevLogIndex)
	entriesAdded := 0
	overwrittenLogs := false
	updatedMetadata := false

	if leaderCommit > int(s.CommitIndex) {
		s.CommitIndex = int64(min(leaderCommit, len(s.Logs)))
		updatedMetadata = true
	}

	for idx, comm := range command {
		if len(s.Logs) > 0 && s.entryExists(prevLogIndex+1+idx) {
			fmt.Println("ENTRY EXISTS ==> ")

			if s.entryConflicts(term, prevLogIndex+1+idx) {
				fmt.Println("ENTRY CONFLICTS =>")

				s.clearConflictingEntries(prevLogIndex + 1 + idx)

				en := Entry{
					Term:    int64(term),
					Command: comm,
				}

				s.Mu.Lock()
				s.Logs = append(s.Logs, en)
				s.Mu.Unlock()

				overwrittenLogs = true
				entriesAdded++

			}
		} else {
			fmt.Println("NO DUPLICATE OR CONFLICTING ENTRY ==>")

			en := Entry{
				Term:    int64(term),
				Command: comm,
			}

			s.Mu.Lock()
			s.Logs = append(s.Logs, en)
			s.Mu.Unlock()

			entriesAdded++
		}
	}

	return s.Persist(entriesAdded, updatedMetadata, overwrittenLogs)
}

/*
Checks if an entry exists at newEntryIndex and compares if the terms match
*/
func (s *Server) entryConflicts(newEntryTerm int, newEntryIndex int) (conflict bool) {

	return s.Logs[newEntryIndex].Term != int64(newEntryTerm)
}

/*
Checks if an entry already exists at given index
*/
func (s *Server) entryExists(newEntryIndex int) (exists bool) {
	return len(s.Logs)-1 >= newEntryIndex
}

func (s *Server) clearConflictingEntries(index int) {
	// Clear entry from current index to the end
	s.Logs = s.Logs[:index]
}

/*
Stores Node details (logs, currentTerm, VotedFor) in persistent storage
*/
func (s *Server) Persist(numOfEntries int, updateMetadata bool, truncate bool) (err error, n int) {

	// offset af which we begin writing the new logs, should replace existing logs from that offset if any
	offset := 16 + ((len(s.Logs) - numOfEntries) * LOG_LENGTH)

	if updateMetadata {
		off, err := s.Fd.Seek(0, 0)

		fmt.Println("OFF => ", off)

		if err != nil {
			panic("Unable to seek to 0 in Persist")
		}
	}

	// File
	// 0 - 8 bytes - CommitIndex
	// 8 - 16 bytes - VotedFor
	// 16 - N bytes Logs
	bw := bufio.NewWriter(s.Fd)

	// Update CommitIndex and VotedFor if value has changed
	if updateMetadata {
		metadata := make([]byte, 16)

		binary.LittleEndian.PutUint64(metadata[:8], uint64(s.CommitIndex))
		copy(metadata[8:16], s.VotedFor)

		if _, err := bw.Write(metadata[:]); err != nil {
			fmt.Println("ERR => ", err)
			panic("Unable to write METADATA")
		}

		if err := bw.Flush(); err != nil {
			panic("Unable too flush data in MEtADATA => ")
		} else {
			fmt.Println("Flushed Metadata contents of buffer to io.Writer...")
		}

	}
	// Get file size
	fileStat, err := s.Fd.Stat()

	if err != nil {
		fmt.Println("ERR => ", err)
		panic("Unable to get file stats")
	}

	size := fileStat.Size()

	// set offset to end of logs to append logs
	// First 16 bytes CommitIndex and VotedFor
	// From 16 - N bytes contains Logs
	offs, err := s.Fd.Seek(size, 0)

	if err != nil {
		fmt.Println("Offset ERR => ", err)
	}

	fmt.Println("OFFSET => ", offs)

	// Get new logs added
	var k []Entry

	if numOfEntries > 0 {
		k = s.Logs[(len(s.Logs) - numOfEntries):]
	} else {
		k = make([]Entry, 0)
	}

	fmt.Println("LENGTH OF K => ", len(k))

	logs := make([]byte, LOG_LENGTH*numOfEntries)

	for _, v := range k {
		newLog := make([]byte, 64)
		binary.LittleEndian.PutUint64(newLog[:8], uint64(v.Term))
		copy(newLog[8:], v.Command)

		// copy new log to Logs
		logs = append(logs, newLog...)
	}

	n, err = bw.Write(logs[:])

	if err != nil {
		panic("Unable to call bw.Write() ON Log writes...")
	}

	if err = bw.Flush(); err != nil {
		panic("Unable too flush log data in Logs => ")
	} else {
		fmt.Println("Flushed contents of buffer to io.Writer...")
	}

	fmt.Printf("Wrote data of length %d to buffer\n", n)

	// Write to persistent storage
	if err = s.Fd.Sync(); err != nil {
		fmt.Println("Unable to sync to stable storage => ", err)
		panic("Error syncing to stable storage")
	}

	// If entries have been overwritten, truncate to eliminate any old data past the new offset
	if truncate {
		if err = os.Truncate("raft.log", int64(offset+(numOfEntries*LOG_LENGTH))); err != nil {
			fmt.Println("Unable to truncate file => ", err)
		}
	}

	return nil, numOfEntries
}

/**
* Restore state from status
 */
func (s *Server) restore() {
	br := bufio.NewReader(s.Fd)

	fileStat, err := s.Fd.Stat()

	if err != nil {
		panic("Could not get file stats")
	}

	size := fileStat.Size()

	if size <= 0 {
		s.initializeServer()
		return
	}

	var offset int64 = 0
	_, err = s.Fd.Seek(0, 0)

	if err != nil {
		fmt.Println("Error setting offset => ", err)
	}

	log := make([]byte, 64)

	// Read CommitIndex
	commitIndex := make([]byte, 8)
	br.Read(commitIndex)
	s.CommitIndex = int64(binary.LittleEndian.Uint64(commitIndex))
	offset += 8

	// Read VotedFor
	votedFor := make([]byte, 8)
	br.Read(votedFor)
	s.VotedFor = string(votedFor)
	offset += 8

	// store Term
	var preliminaryTerm int64 = 0
	// Read the log entries and append to Server struct
	for offset < size {
		br.Read(log)

		// Add log to struct
		newEntry := Entry{
			Term:    int64(binary.LittleEndian.Uint64(log[:8])),
			Command: log[16:],
		}

		s.Logs = append(s.Logs, newEntry)

		// update Term
		if newEntry.Term > int64(preliminaryTerm) {
			preliminaryTerm = newEntry.Term
		}

		if offset+64 > size {
			offset += size - offset
		} else {
			offset += 64
		}
		if offset == size {
			break
		}

		fmt.Println("READ LOG TERM => ", newEntry.Term)
	}

	s.Term = preliminaryTerm

	fmt.Println("FINAL RESTORED TERM => ", s.Term)
}

/*
Updates VotedFor and stores in persistent state
*/
func (s *Server) SetVotedFor(serverId string) {
	fmt.Println("Setting Voted For...")
	s.VotedFor = serverId

	s.Persist(0, true, false)
}

/**
* Initialize commit index
 */
func (s *Server) initializeServer() {
	fmt.Println("Initializing....")
	bw := bufio.NewWriter(s.Fd)

	index := make([]byte, 8)

	binary.LittleEndian.PutUint64(index, uint64(0))

	n, err := bw.Write(index)

	if err != nil {
		panic("Unable to write...")
	}

	fmt.Printf("Wrote %d bytes\n", n)

	if err = bw.Flush(); err != nil {
		fmt.Println("ERR on flush => ", err)
		panic("Unable to flush")
	}

	if err = s.Fd.Sync(); err != nil {
		panic("Unable to Sync while initializing...")
	}
}

func (s *Server) IncrementTerm(newTerm *int) {
	s.Mu.Lock()

	if newTerm != nil {
		s.Term = int64(*newTerm)
	} else {
		s.Term += 1
	}

	s.Mu.Unlock()
}

func (s *Server) DecrementTerm() {
	if s.Term <= 0 {
		return
	}

	s.Mu.Lock()
	s.Term = s.Term - 1
	s.Mu.Unlock()
	// WriteToPersistentState(CurrentTerm, &VotedFor, CommitIndex)
}

/*
Get's the last Logs term
*/
func (s *Server) GetLastLogTerm(newEntriesCount int) (term int) {
	logLen := len(s.Logs)
	if logLen <= 0 {
		return 0
	}

	if newEntriesCount <= 0 {
		// No New Entries
		return int(s.Logs[logLen-1].Term)

	} else if logLen == newEntriesCount {
		// No entries before the new Entries
		return 0
	} else {
		return int(s.Logs[logLen-(1+newEntriesCount)].Term)
	}
}

//func SetVotedFor(candidateId string) {
//	mu.Lock()
//	VotedFor = candidateId
//	mu.Unlock()
//
//	WriteToPersistentState(CurrentTerm, &candidateId, CommitIndex)
//}

/**
*  Sets a new term to state(volatile and persisent)
 */
func (s *Server) SetTerm(newTerm int) {
	fmt.Println("Setting Term...")
	s.Mu.Lock()

	if newTerm != 0 {
		s.Term = int64(newTerm)
	} else {
		s.Term += 1
	}

	s.Mu.Unlock()
	s.Persist(0, true, false)
}

/**
*  Sets a new CommitIndex to state(volatile and persisent)
 */
//func SetTerm(commIdx int) {
//	WriteToPersistentState(CurrentTerm, nil, CommitIndex)

//}

//	func ReadPersistentState(wg *sync.WaitGroup) (persData PersistentState, err error) {
//		defer wg.Done()
//
//		var state PersistentState
//
//		f, err := os.OpenFile("server_data.json", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
//
//		stat, err := f.Stat()
//
//		if err != nil {
//			log.Fatal(err.Error())
//			return state, err
//		}
//
//		fmt.Println("FILE ENGTH => %d", stat.Size())
//
//		// if file is empty, initialize values to zero
//		if stat.Size() <= 0 {
//			WriteToPersistentState(0, &VotedFor, 0)
//		}
//
//		j := json.NewDecoder(f)
//		j.Decode(&state)
//
//		return state, nil
//	}
func (s *Server) UpdateServerState(newState NodeRole) {
	defer s.Mu.Unlock()
	if newState != CANDIDATE && newState != FOLLOWER && newState != LEADER {
		log.Fatal("State can only be `candidate`, `leader` or `follower`")
		return
	}

	s.Mu.Lock()
	s.Role = newState

	// If reverting to follower, clear votedFor update persistent state
	if newState == FOLLOWER {
		s.VotedFor = ""
		//s.Persist(0, true, false)
	}

}

// func WriteToPersistentState(term int, votedFor *string, commitIndex int) {
// 	newState := PersistentState{}
//
// 	if term != 0 {
// 		newState.CurrentTerm = term
// 	}
//
// 	if votedFor != nil {
// 		newState.VotedFor = *votedFor
// 	}
//
// 	if commitIndex != 0 {
// 		newState.CommitIndex = commitIndex
// 	}
//
// 	f, err := os.OpenFile("./server_data.json", os.O_CREATE|os.O_WRONLY, 0644)
//
// 	if err != nil {
// 		log.Fatal(err)
//
// 		return
// 	}
//
// 	defer f.Close()
//
// 	fmt.Println("PersistentState STRUCT => %q", newState)
//
// 	// Encode the Go struct to JSON and write to the file
// 	encoder := json.NewEncoder(f)
// 	encoder.SetIndent("", "  ") // For pretty-printing
// 	err = encoder.Encode(newState)
// 	if err != nil {
// 		fmt.Println("Error encoding JSON:", err)
// 		return
// 	}
//
// }
//
// /**
// * Writes logs from AppendEntryRPC to log file
//  */
// func WriteToLogs(logs []string) (err error, success bool) {
// 	f, err := os.OpenFile("raft.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend)
//
// 	if err != nil {
// 		fmt.Println(err.Error())
// 		return err, false
// 	}
//
// 	defer f.Close()
//
// 	for _, log := range logs {
// 		newLog, err := addMetadataToLog(log)
//
// 		if err != nil {
// 			fmt.Println("Invalid log => ", log)
//
// 			return err, false
// 		}
//
// 		if _, err := f.Write([]byte(newLog)); err != nil {
// 			fmt.Println("Unable to write log => ", err)
// 			f.Close()
// 			break
// 		}
//
// 		// Update commit index
// 		mu.Lock()
// 		CommitIndex++
// 		LogIndex++
// 		mu.Unlock()
// 	}
//
// 	// Ensure commit index is updated:WriteToLogs
// 	WriteToPersistentState(CurrentTerm, &VotedFor, CommitIndex)
//
// 	return nil, true
// }
//
// /**
// * Prepends term and index to log
//  */
// func addMetadataToLog(entry string) (newEntry string, err error) {
// 	if entry == "" {
// 		return "", errors.New("Invalid entry provided")
// 	}
//
// 	newLog := fmt.Sprintf("<%d , %d, %s>\n", LogIndex+1, CurrentTerm, entry)
//
// 	return newLog, nil
//
// }
