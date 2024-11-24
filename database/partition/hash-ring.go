package partition

import (
	"log"
	"sort"
	"strconv"
	"sync"

	"github.com/spaolacci/murmur3"
)

func hashKey(key []byte) uint64 {
	h64 := murmur3.New64WithSeed(4007)

	if _, err := h64.Write(key); err != nil {
		panic(err)
	}

	return h64.Sum64()
}

type HashRing struct {
	nodes      []*Node
	vNodes     map[uint64]*Node
	slots      []uint64
	fillFactor int
	mu         sync.RWMutex
}

func NewHashRing(fillFactor int) *HashRing {
	return &HashRing{
		nodes:      make([]*Node, 0),
		vNodes:     make(map[uint64]*Node),
		slots:      make([]uint64, 0),
		fillFactor: fillFactor,
	}
}

func (hr *HashRing) AddNode(node *Node) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	for i := 0; i < hr.fillFactor; i++ {
		vNode := node.Id + strconv.Itoa(i)
		hash := hashKey([]byte(vNode))
		hr.vNodes[hash] = node
		hr.slots = append(hr.slots, hash)
	}

	sort.Slice(hr.slots, func(i, j int) bool {
		return hr.slots[i] < hr.slots[j]
	})
}

// todo: change to return X unique nodes
func (hr *HashRing) GetNodes(key []byte, count int) []*Node {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	nodes := make([]*Node, 0)

	if len(hr.vNodes) == 0 {
		return nodes
	}

	hash := hashKey(key)

	idx := sort.Search(len(hr.slots), func(i int) bool {
		return hr.slots[i] >= hash
	})

	if idx == len(hr.slots) {
		idx = 0
	}

	for i := 0; i < count; i++ {
		nodeHash := hr.slots[(idx+i)%len(hr.slots)]
		nodes = append(nodes, hr.vNodes[nodeHash])
	}

	return nodes
}

func (hr *HashRing) Print() {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	for slot, node := range hr.vNodes {
		log.Printf("Slot %d node %s", slot, node.Id)
	}
}
