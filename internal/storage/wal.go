package storage

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// LogEntry represents a single operation in our database
type LogEntry struct {
	Operation string `json:"operation"` // "PUT" or "DELETE"
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
}

// WAL represents the Write-Ahead Log
type WAL struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
}

// NewWAL creates or opens a WAL file for appending
func NewWAL(filename string) (*WAL, error) {
	// Open the file in Append mode, create it if it doesn't exist
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
		enc:  json.NewEncoder(file),
	}, nil
}

// Append writes a new entry to the log securely
func (w *WAL) Append(entry LogEntry) error {
	w.mu.Lock() // Ensure only one write to the file happens at a time
	defer w.mu.Unlock()
	return w.enc.Encode(entry)
}

// Close gracefully closes the log file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// ReplayReads the WAL line by line and rebuilds the map state
func (w *WAL) Replay(store map[string][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Go back to the beginning of the file
	w.file.Seek(0, 0)
	scanner := bufio.NewScanner(w.file)

	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return err
		}

		// Re-apply the operation to the map
		if entry.Operation == "PUT" {
			store[entry.Key] = entry.Value
		} else if entry.Operation == "DELETE" {
			delete(store, entry.Key)
		}
	}

	return scanner.Err()
}
