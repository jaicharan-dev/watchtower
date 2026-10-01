package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// Metric represents a single data point from our applications.
type Metric struct {
	Name      string    `json:"name"`
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// main is the entry point of our Go application.
func main() {
	http.HandleFunc("/ingest", handleIngest)

	fmt.Println("Watchtower Ingestion API starting on port 8080...")
	err := http.ListenAndServe(":8080", nil)
	
	if err != nil {
		log.Fatal("Server failed to start:", err)
	}
}

// handleIngest processes incoming metric data.
func handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are allowed", http.StatusMethodNotAllowed)
		return
	}

	// Create an empty Metric object to hold the incoming data
	var m Metric

	// Decode the JSON from the request body into our Metric struct
	err := json.NewDecoder(r.Body).Decode(&m)
	if err != nil {
		http.Error(w, "Invalid JSON data", http.StatusBadRequest)
		return
	}

	// For now, print the parsed metric to the console
	fmt.Printf("Received Metric - Name: %s, Value: %f, Time: %s\n", m.Name, m.Value, m.Timestamp)

	// Send a success response back to the client
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Metric '%s' received successfully!\n", m.Name)
}
