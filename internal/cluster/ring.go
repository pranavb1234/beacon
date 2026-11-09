// Package cluster provides consistent hash ring routing and cluster membership.
// The ring determines which node owns a given topic (primary) and which node
// holds its replica (the next distinct node clockwise on the ring).
package cluster

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// HashFunc maps bytes to a 32-bit hash value.
type HashFunc func(data []byte) uint32

// Ring is a thread-safe consistent hash ring with virtual nodes.
//
// Virtual nodes: each real node is represented by V virtual nodes spread around
// the ring. This smooths out clustering and ensures a balanced distribution of keys.
type Ring struct {
	mu           sync.RWMutex
	vnodes       int               // virtual nodes per real node
	hash         HashFunc          // hash function (defaults to CRC32 IEEE)
	vnodeMap     map[uint32]string // ring position -> real node ID
	nodeAddrs    map[string]string // real node ID -> address
	sortedHashes []uint32          // sorted hashes for binary-search lookup
}

// New creates a Ring with the given number of virtual nodes per real node.
// vnodes=150 is the standard default providing balanced distribution within ±20% of the mean.
func New(vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = 150
	}
	return &Ring{
		vnodes:    vnodes,
		hash:      crc32.ChecksumIEEE,
		vnodeMap:  make(map[uint32]string),
		nodeAddrs: make(map[string]string),
	}
}

// SetHashFunc allows configuring an alternative hash function.
func (r *Ring) SetHashFunc(fn HashFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hash = fn
	r.rebuildLocked()
}

// AddNode inserts a physical node into the ring with V virtual nodes.
// If the node already exists, it is updated idempotently.
func (r *Ring) AddNode(id string) {
	r.Add(id, "")
}

// Add inserts a physical node with its network address and all its virtual replicas.
// Safe for concurrent use.
func (r *Ring) Add(id, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If node already exists, update address and return
	if _, exists := r.nodeAddrs[id]; exists {
		r.nodeAddrs[id] = addr
		return
	}

	r.nodeAddrs[id] = addr

	// Insert virtual nodes
	for i := 0; i < r.vnodes; i++ {
		h := r.hash([]byte(fmt.Sprintf("%s#%d", id, i)))
		r.vnodeMap[h] = id
		r.sortedHashes = append(r.sortedHashes, h)
	}

	sort.Slice(r.sortedHashes, func(i, j int) bool {
		return r.sortedHashes[i] < r.sortedHashes[j]
	})
}

// RemoveNode deletes a node and all its virtual replicas from the ring.
// Implements rebuild-free removal: filters sortedHashes in-place in a single O(N) pass,
// preserving sorted order without invoking sort.Slice.
func (r *Ring) RemoveNode(id string) {
	r.Remove(id)
}

// Remove deletes a node and all its virtual replicas from the ring.
func (r *Ring) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodeAddrs[id]; !exists {
		return
	}
	delete(r.nodeAddrs, id)

	toRemove := make(map[uint32]struct{}, r.vnodes)
	for i := 0; i < r.vnodes; i++ {
		h := r.hash([]byte(fmt.Sprintf("%s#%d", id, i)))
		toRemove[h] = struct{}{}
		delete(r.vnodeMap, h)
	}

	// Rebuild-free in-place filtering: preserves existing sort order
	newHashes := make([]uint32, 0, len(r.sortedHashes)-len(toRemove))
	for _, h := range r.sortedHashes {
		if _, removed := toRemove[h]; !removed {
			newHashes = append(newHashes, h)
		}
	}
	r.sortedHashes = newHashes
}

// GetOwner returns the primary node ID that owns the key.
// Returns an empty string if the ring contains no nodes.
func (r *Ring) GetOwner(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getOwnerLocked(key)
}

func (r *Ring) getOwnerLocked(key string) string {
	if len(r.sortedHashes) == 0 {
		return ""
	}
	idx := r.search(r.hash([]byte(key)))
	return r.vnodeMap[r.sortedHashes[idx]]
}

// GetReplica returns the next DISTINCT physical node clockwise from the owner.
// When 2 or more nodes exist, the replica is guaranteed to differ from the owner.
// If only one node exists, returns that node. Returns empty string if the ring is empty.
func (r *Ring) GetReplica(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return ""
	}

	owner := r.getOwnerLocked(key)
	if len(r.nodeAddrs) <= 1 {
		return owner
	}

	startIdx := r.search(r.hash([]byte(key)))
	n := len(r.sortedHashes)

	// Walk clockwise until we find a distinct physical node ID
	for i := 1; i < n; i++ {
		candidate := r.vnodeMap[r.sortedHashes[(startIdx+i)%n]]
		if candidate != owner {
			return candidate
		}
	}

	return owner
}

// Owner returns the primary owner node ID and its address.
// ok is false if the ring is empty.
func (r *Ring) Owner(key string) (id, addr string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	owner := r.getOwnerLocked(key)
	if owner == "" {
		return "", "", false
	}
	return owner, r.nodeAddrs[owner], true
}

// Replica returns the replica node ID and its address.
// ok is false if the ring is empty.
func (r *Ring) Replica(key string) (id, addr string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	replica := r.GetReplica(key)
	if replica == "" {
		return "", "", false
	}
	return replica, r.nodeAddrs[replica], true
}

// Nodes returns all distinct node IDs currently in the ring in sorted order.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.nodeAddrs))
	for id := range r.nodeAddrs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// search returns the index of the first hash >= h, wrapping around to 0 if h
// exceeds all hashes in the ring.
// Must be called with at least r.mu.RLock held.
func (r *Ring) search(h uint32) int {
	n := len(r.sortedHashes)
	idx := sort.Search(n, func(i int) bool {
		return r.sortedHashes[i] >= h
	})
	return idx % n
}

// rebuildLocked reconstructs sortedHashes from vnodeMap.
// Must be called with r.mu.Lock held.
func (r *Ring) rebuildLocked() {
	r.vnodeMap = make(map[uint32]string)
	r.sortedHashes = make([]uint32, 0, len(r.nodeAddrs)*r.vnodes)
	for id := range r.nodeAddrs {
		for i := 0; i < r.vnodes; i++ {
			h := r.hash([]byte(fmt.Sprintf("%s#%d", id, i)))
			r.vnodeMap[h] = id
			r.sortedHashes = append(r.sortedHashes, h)
		}
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool {
		return r.sortedHashes[i] < r.sortedHashes[j]
	})
}
