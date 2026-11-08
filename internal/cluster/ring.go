// Package cluster provides consistent hash ring routing and cluster membership.
// The ring determines which node owns a given topic (primary) and which node
// holds its replica (the next distinct node clockwise on the ring).
package cluster

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// Ring is a thread-safe consistent hash ring with virtual nodes.
//
// Virtual nodes: each real node is represented by vnodes phantom entries
// spread around the ring. This smooths out the natural clustering that
// occurs when only 3 real nodes sit on a ring of 2^32 positions.
//
// mu protects vnodeMap and sortedHashes.
type Ring struct {
	mu           sync.RWMutex
	vnodes       int             // virtual nodes per real node
	vnodeMap     map[uint32]node // ring position → real node
	sortedHashes []uint32        // sorted for binary-search lookup
}

// node is an entry on the ring representing a real broker.
type node struct {
	id   string
	addr string
}

// New creates a Ring with the given number of virtual nodes per real node.
// vnodes=150 is a good default for clusters of ≤ 10 nodes; it keeps
// max/min load ratio below 1.5 with high probability.
func New(vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = 150
	}
	return &Ring{
		vnodes:   vnodes,
		vnodeMap: make(map[uint32]node),
	}
}

// Add inserts a node and all its virtual replicas into the ring.
// Safe for concurrent use; callers do not need external locking.
func (r *Ring) Add(id, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.vnodes; i++ {
		// Label format is "<id>#<i>" — collision probability with FNV-32a is
		// negligible for vnodes ≤ 500 and cluster sizes ≤ 50.
		h := hashKey(fmt.Sprintf("%s#%d", id, i))
		r.vnodeMap[h] = node{id: id, addr: addr}
	}
	r.rebuild()
}

// Remove deletes a node and all its virtual replicas from the ring.
// Keys that were on the removed node will fall to the next live node.
func (r *Ring) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.vnodes; i++ {
		h := hashKey(fmt.Sprintf("%s#%d", id, i))
		delete(r.vnodeMap, h)
	}
	r.rebuild()
}

// Owner returns the primary owner of key (the first node clockwise from hash(key)).
// Returns ok=false only when the ring is empty.
func (r *Ring) Owner(key string) (id, addr string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return "", "", false
	}
	idx := r.search(hashKey(key))
	n := r.vnodeMap[r.sortedHashes[idx]]
	return n.id, n.addr, true
}

// Replica returns the first node after Owner that has a different node ID.
// This is the node that holds the in-memory replica of the topic.
// If the ring contains only one distinct node, Replica returns that same node.
func (r *Ring) Replica(key string) (id, addr string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return "", "", false
	}

	h := hashKey(key)
	startIdx := r.search(h)
	owner := r.vnodeMap[r.sortedHashes[startIdx]]

	// Walk clockwise until we find a distinct node ID.
	n := len(r.sortedHashes)
	for i := 1; i < n; i++ {
		candidate := r.vnodeMap[r.sortedHashes[(startIdx+i)%n]]
		if candidate.id != owner.id {
			return candidate.id, candidate.addr, true
		}
	}

	// Only one distinct node in the ring — replica == primary.
	return owner.id, owner.addr, true
}

// Nodes returns all distinct node IDs currently in the ring.
// Order is not guaranteed.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]struct{}, 8)
	for _, n := range r.vnodeMap {
		seen[n.id] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}

// rebuild re-sorts sortedHashes after any mutation.
// Must be called with r.mu held for writing.
func (r *Ring) rebuild() {
	r.sortedHashes = make([]uint32, 0, len(r.vnodeMap))
	for h := range r.vnodeMap {
		r.sortedHashes = append(r.sortedHashes, h)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool {
		return r.sortedHashes[i] < r.sortedHashes[j]
	})
}

// search returns the index of the first hash ≥ h, wrapping around to 0 if h
// is greater than all hashes (virtual ring semantics).
// Must be called with r.mu held for reading.
func (r *Ring) search(h uint32) int {
	n := len(r.sortedHashes)
	idx := sort.Search(n, func(i int) bool {
		return r.sortedHashes[i] >= h
	})
	return idx % n // wrap: if h > max, we go to index 0
}

// hashKey hashes s with FNV-32a.
// FNV-32a is not cryptographic, but it is fast, deterministic, and has good
// avalanche behaviour for the short strings we produce ("<id>#<vnode>").
func hashKey(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
