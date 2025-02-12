package membership

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

type Members struct {
	Members []string `json:"members"`
}

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
