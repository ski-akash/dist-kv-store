package hashing

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// HashRing manages the consistent hashing ring
type HashRing struct {
	mu       sync.RWMutex
	replicas int               // Number of virtual nodes per physical server
	keys     []uint32          // Sorted list of node hashes on the ring
	hashMap  map[uint32]string // Maps the hash back to the physical server address
}

// NewHashRing initializes a new consistent hash ring
func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		replicas: replicas,
		hashMap:  make(map[uint32]string),
	}
}

// AddNode adds a new physical server to the ring
func (r *HashRing) AddNode(nodeAddress string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Create virtual nodes for better data distribution
	for i := 0; i < r.replicas; i++ {
		// Generate a unique name for the virtual node
		virtualNodeName := strconv.Itoa(i) + ":" + nodeAddress
		hash := r.hashKey(virtualNodeName)

		r.keys = append(r.keys, hash)
		r.hashMap[hash] = nodeAddress
	}

	// Sort the ring so we can easily search it later
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

// RemoveNode safely removes a server if it crashes
func (r *HashRing) RemoveNode(nodeAddress string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		virtualNodeName := strconv.Itoa(i) + ":" + nodeAddress
		hash := r.hashKey(virtualNodeName)

		delete(r.hashMap, hash)

		// Find and remove the hash from our sorted slice
		for j := 0; j < len(r.keys); j++ {
			if r.keys[j] == hash {
				r.keys = append(r.keys[:j], r.keys[j+1:]...)
				break
			}
		}
	}
}

// GetNode routes a key to its correct server
func (r *HashRing) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return ""
	}

	hash := r.hashKey(key)

	// Binary search to find the first server whose hash is >= the key's hash
	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= hash
	})

	// If we reach the end of the ring, wrap around to the very first server
	if idx == len(r.keys) {
		idx = 0
	}

	return r.hashMap[r.keys[idx]]
}

// hashKey generates a CRC32 checksum for fast, reliable hashing
func (r *HashRing) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}