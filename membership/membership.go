package membership

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
)

type Members struct {
	Members []string `json:"members"`
}

type ClusterMemberStruct struct {
	Members map[string]uint
	Mu      sync.Mutex
}

var ClusterMembers *ClusterMemberStruct

func init() {
	members := readClusterMembers()

	initializedMembers := make(map[string]uint)

	for _, v := range members {
		initializedMembers[v] = 0
	}

	ClusterMembers = &ClusterMemberStruct{
		Members: initializedMembers,
		Mu:      sync.Mutex{},
	}

	// ClusterMembers = &mems
}

/**
* Retrieves cluster member  IPs and ports stored in a config file
 */
func readClusterMembers() []string {
	var members Members

	f, err := os.OpenFile("members.json", os.O_RDONLY, 0644)

	if err != nil {
		log.Fatal("Unable to read config file")
		return members.Members
	}

	j := json.NewDecoder(f)
	j.Decode(&members)

	fmt.Println("Members RETRIEVED => ", members.Members)
	return members.Members
}

func (m *ClusterMemberStruct) GetClusterMembers() []string {
	mems := make([]string, 0, len(m.Members))
	if len(m.Members) <= 0 {
		return make([]string, 0)
	}

	for k, _ := range m.Members {
		mems = append(mems, k)
	}

	return mems
}

func (m *ClusterMemberStruct) UpdateClusterMembers(nextIndex uint) {
	for k, _ := range m.Members {
		m.Members[k] = nextIndex
	}

	return
}

func (m *ClusterMemberStruct) IncrementNodeNextIndex(address string, newEntryCount uint, currLogLen int) {
	defer m.Mu.Unlock()

	if (ClusterMembers.Members[address] + newEntryCount) > uint(currLogLen) {
		m.Mu.Lock()
		ClusterMembers.Members[address] = uint(currLogLen)

		return
	}

	m.Mu.Lock()
	ClusterMembers.Members[address] = ClusterMembers.Members[address] + newEntryCount
	fmt.Printf("DONE INCREMENTING:: %s, NEW COUNT => %d\n", address, ClusterMembers.Members[address])
}

func (m *ClusterMemberStruct) SetNodeNextIndex(address string, newIndex uint, currLogLen int) {
	defer m.Mu.Unlock()

	if newIndex > uint(currLogLen) {
		m.Mu.Lock()
		ClusterMembers.Members[address] = uint(currLogLen)

		return
	}
	m.Mu.Lock()

	ClusterMembers.Members[address] = newIndex
	fmt.Printf("DONE SETTING NEXT INDEX:: %s, NEW COUNT => %d\n", address, ClusterMembers.Members[address])

}
