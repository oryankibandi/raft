package kvstore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"
)

type KVServer struct {
	Port string
}

type EntryReqBody struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GetValueReqBody struct {
	Key string `json:"key"`
}

type ResponseBody struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

var KVServ KVServer

func handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	res := make(map[string]string)
	res["message"] = "Hello world"

	json.NewEncoder(w).Encode(res)

	return
}

func handleAddKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var newData EntryReqBody

	err := json.NewDecoder(r.Body).Decode(&newData)

	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	newEntry := Entry{
		Key:   newData.Key,
		Value: newData.Value,
	}

	Entries.StoreVals(newEntry)
	// Set Content-Type header
	w.Header().Set("Content-Type", "application/json")

	// Encode struct to JSON and write response
	json.NewEncoder(w).Encode(newData)

	return
}

func handleGetKey(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var reqBody GetValueReqBody
	err := json.NewDecoder(r.Body).Decode(&reqBody)

	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	err, val := Entries.GetVal(reqBody.Key)

	if err != nil {
		errResp := ResponseBody{
			Success: false,
			Message: "could not get value",
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)

		json.NewEncoder(w).Encode(errResp)

		return
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(val)

	return

}

func InitializeApi(port string) {

	KVServ = KVServer{
		Port: port,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/", handler)
	mux.HandleFunc("/api/add", handleAddKey)
	mux.HandleFunc("/api/getval", handleGetKey)

	server := http.Server{
		Addr:    fmt.Sprintf(":%s", port),
		Handler: mux,
	}

	// Channel to listen for OS interrupt signals (Ctrl+C, termination signals)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	go func() {
		fmt.Println("Server running on port ", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("ListenAndServe error: %v\n", err)
		}
	}()

	<-stop // Wait for termination signal
	fmt.Println("\nShutting down server...")

	// Close file descriptor
	Entries.Fd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("Server shutdown error: %v\n", err)
	}

	fmt.Println("Server gracefully stopped.")

}
