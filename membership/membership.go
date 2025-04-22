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

/**
* Retrieves cluster member  IPs and ports stored in a config file
 */
func GetClusterMembers() []string {
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

func InitializeClusterMembers(nextIndex uint) {
	members := GetClusterMembers()

	initializedMembers := make(map[string]uint)

	for _, v := range members {
		initializedMembers[v] = nextIndex
	}

	mems := ClusterMemberStruct{
		Members: initializedMembers,
		Mu:      sync.Mutex{},
	}

	ClusterMembers = &mems
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
