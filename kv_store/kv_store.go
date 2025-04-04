package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type kvStore struct {
	data *sync.Map
	Fd   *os.File
}

type Entry struct {
	Key   string
	Value string
}

type ReaderCloser struct {
	reader io.ReadCloser
}

var Entries kvStore

var m sync.Map

func (r *kvStore) readFromPersistentState() (d []byte) {
	n, err := os.ReadFile("kv_store.json")

	fmt.Printf("READ %d BYTES\n", n)

	if err != nil {
		log.Fatal("Unable to read from file")
	}

	fmt.Println("read bytes => ", n)

	return n
}

func (s *kvStore) StoreVals(v Entry) (err error) {
	if v.Key == "" || v.Value == "" {
		fmt.Println("Entry is missing key or value")
		return errors.New("Entry is missing key or value")
	}

	s.data.Store(v.Key, v.Value)

	go s.persistVals()
	return nil
}

func (s *kvStore) GetVal(k string) (err error, value *Entry) {
	if k == "" {
		fmt.Println("key is required")
		return errors.New("Key is required"), &Entry{}
	}

	val, ok := s.data.Load(k)

	if !ok || val == nil {

		return errors.New("Value not found"), &Entry{}
	}

	return nil, &Entry{
		Key:   k,
		Value: val.(string),
	}
}

func (s *kvStore) persistVals() {
	normalMap := make(map[string]string)

	s.data.Range(func(key, value any) bool {
		normalMap[key.(string)] = value.(string)

		return true
	})

	// Encode to JSON
	jsonData, err := json.Marshal(normalMap)

	if err != nil {
		fmt.Println("ERR => ", err)
		log.Fatal("Unable to unmarshal")

	}

	// Move cursor to the beginning of the file to overwrite existing content
	_, err = s.Fd.Seek(0, 0)
	if err != nil {
		fmt.Println("Error seeking file:", err)
		return
	}

	_, err = s.Fd.Write(jsonData)

	if err != nil {
		fmt.Println("ERR => ", err)
		log.Fatal("Unable to write to persistent state")
	}
}

func InitiateKVState() {
	f, err := os.OpenFile("kv_store.json", os.O_CREATE|os.O_RDWR, 0644)

	Entries = kvStore{
		data: &m,
		Fd:   f,
	}

	k := make([]byte, 0)

	if err != nil {
		fmt.Println("ERR ==> ", err)
		log.Fatal("Unable to open kv_store file")
	}

	k = Entries.readFromPersistentState()

	fmt.Println("READ STATE => ", len(k))

	if len(k) > 0 {
		data := make(map[string]string)
		err = json.Unmarshal(k, &data)

		if err != nil {
			fmt.Println("ERR => ", err)
			log.Fatal("Unable to Unmarshall")
		}

		fmt.Println("JSON ==> ", data)

		for k, v := range data {
			entr := Entry{
				Key:   k,
				Value: v,
			}

			Entries.StoreVals(entr)
		}
	}
}
