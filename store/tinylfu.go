package store

import (
	"hash/fnv"
	"sync"
)

type CountMinSketch struct {
	width uint64
	depth uint64
	// 一维连续数组，采用uint32节省内存和提升缓存亲和力
	table      []uint32
	seeds      []uint64
	itemsAdded uint64
	windowSize uint64
}

type Node struct {
	key  string
	val  Value
	prev *Node
	next *Node
}

func (n *Node) size() uint64 {
	if n == nil || n.val == nil {
		return 0
	}
	return uint64(len(n.key) + n.val.Len())
}

type LRUCache struct {
	cache     map[string]*Node
	maxBytes  uint64
	usedBytes uint64
	head      *Node
	tail      *Node
}

func NewNode(key string, val Value) *Node {
	node := &Node{
		key: key,
		val: val,
	}
	return node
}

func NewCountMinSketch(width, depth int) *CountMinSketch {
	seeds := make([]uint64, depth)
	for i := range depth {
		seeds[i] = uint64(i*1315423911 + 1)
	}

	return &CountMinSketch{
		width:      uint64(width),
		depth:      uint64(depth),
		table:      make([]uint32, width*depth),
		seeds:      seeds,
		itemsAdded: 0,
		windowSize: uint64(10 * width * depth),
	}
}

// 提取基础哈希计算，避免循环内重复创建对象和计算哈希
func (cms *CountMinSketch) baseHash(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func (cms *CountMinSketch) Insert(key string) {
	h := cms.baseHash(key)
	for i := range cms.depth {
		index := uint64(i)*cms.width + ((h ^ cms.seeds[i]) % cms.width)
		cms.table[index]++
	}
	cms.itemsAdded++
	if cms.itemsAdded == cms.windowSize {
		for i := 0; i < int(cms.width*cms.depth); i++ {
			cms.table[i] >>= 1
		}
		cms.itemsAdded >>= 1
	}
}

func (cms *CountMinSketch) Estimate(key string) uint64 {
	h := cms.baseHash(key)
	Min := uint32(^uint32(0))
	for i := range cms.depth {
		index := uint64(i)*cms.width + ((h ^ cms.seeds[i]) % cms.width)
		Min = min(Min, cms.table[index])
	}
	return uint64(Min)
}

func Constructor(maxBytes uint64) *LRUCache {
	head := &Node{}
	tail := &Node{}

	head.next = tail
	tail.prev = head
	return &LRUCache{
		cache:    make(map[string]*Node),
		maxBytes: uint64(maxBytes),
		head:     head,
		tail:     tail,
	}
}

func (lru *LRUCache) removeNode(node *Node) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (lru *LRUCache) addToHead(node *Node) {
	node.next = lru.head.next
	node.prev = lru.head

	lru.head.next.prev = node
	lru.head.next = node
}

func (lru *LRUCache) moveToHead(node *Node) {
	lru.removeNode(node)
	lru.addToHead(node)
}

type TinyLFU struct {
	mu  sync.Mutex
	lru *LRUCache
	cms *CountMinSketch
}

func NewTinyLFU(capacity int, cmsWidth, cmsDepth int) *TinyLFU {
	return &TinyLFU{
		lru: Constructor(uint64(capacity)),
		cms: NewCountMinSketch(cmsWidth, cmsDepth),
	}
}

func (t *TinyLFU) Get(key string) (Value, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cms.Insert(key) // 关键附加动作：每次读取都增加频率

	if node, ok := t.lru.cache[key]; ok {
		t.lru.moveToHead(node)
		return node.val, true
	}
	return nil, false
}

func (t *TinyLFU) canEvictWithEstimatedSize(node *Node) (int, bool) {
	// 0 表示无限容量
	if t.lru.maxBytes == 0 {
		return 0, true
	}
	needToEvictedCount := 0
	nodeSize := node.size()
	candidateFreq := t.cms.Estimate(node.key)
	tempUsedByts := t.lru.usedBytes
	if nodeSize > t.lru.maxBytes {
		return -1, false
	}
	cur := t.lru.tail.prev
	for tempUsedByts+nodeSize > t.lru.maxBytes {
		if cur == t.lru.head {
			return -1, false
		}
		curFreq := t.cms.Estimate(cur.key)
		if candidateFreq > curFreq {
			needToEvictedCount++
			tempUsedByts -= cur.size()
		} else {
			return -1, false
		}
		cur = cur.prev
	}
	return needToEvictedCount, true
}

func (t *TinyLFU) Insert(key string, val Value) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if node, ok := t.lru.cache[key]; ok {
		// 更新已有节点的值，并调整内存容量
		t.lru.usedBytes += uint64(val.Len()) - uint64(node.val.Len())
		node.val = val
		t.lru.moveToHead(node)
		t.cms.Insert(key)
	} else {
		// 新节点
		node := NewNode(key, val)
		nodeSize := node.size()
		if needToEvictedCount, ok := t.canEvictWithEstimatedSize(node); ok {
			// 尝试腾出空间
			for range needToEvictedCount {
				victim := t.lru.tail.prev
				t.lru.usedBytes -= victim.size()
				t.lru.removeNode(victim)
				delete(t.lru.cache, victim.key)
			}

			// 如果腾出空间后，有足够的内存放置新节点，或者 maxBytes 本身为 0 (无限制)
			t.lru.addToHead(node)
			t.lru.cache[key] = node
			t.lru.usedBytes += nodeSize
			t.cms.Insert(key)
		} else {
			return
		}
	}
}
