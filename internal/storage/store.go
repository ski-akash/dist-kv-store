package storage

import "sync"

// KVStore represents our thread-safe in-memory database
type KVStore struct {
	mu    sync.RWMutex
	store map[string][]byte
}

// NewKVStore initializes and returns a ready-to-use store
func NewKVStore() *KVStore {
	return &KVStore{
		store: make(map[string][]byte),
	}
}

// Put safely adds or updates a key-value pair
func (s *KVStore) Put(key string, value []byte) {
	s.mu.Lock() // Exclusive lock for writing
	defer s.mu.Unlock()

	s.store[key] = value
}

// Get safely retrieves a value. It returns the value and a boolean indicating if it was found.
func (s *KVStore) Get(key string) ([]byte, bool) {
	s.mu.RLock() // Shared lock for reading
	defer s.mu.RUnlock()

	value, found := s.store[key]
	return value, found
}

// Delete safely removes a key from the store
func (s *KVStore) Delete(key string) {
	s.mu.Lock() // Exclusive lock for writing
	defer s.mu.Unlock()

	delete(s.store, key)
}

// RecoverFromWAL loads the saved data from the WAL into the in-memory store
func (s *KVStore) RecoverFromWAL(wal *WAL) error {
	// Because both files are in the 'storage' packae, we can access s.store
	return wal.Replay(s.store)
}
