package store

import (
	"hash/fnv"
	"sync"
	"time"
)

type CMSOptions struct {
	cmsWidth uint64
	cmsDepth uint64
}

func NewCMSOptions() CMSOptions {
	return CMSOptions{
		cmsWidth: 1000,
		cmsDepth: 5,
	}
}

type CountMinSketch struct {
	width uint64
	depth uint64
	// 一维连续数组，采用uint32节省内存和提升缓存亲和力
	table      []uint32
	seeds      []uint64
	itemsAdded uint64
	windowSize uint64
}

func NewCountMinSketch(opts CMSOptions) *CountMinSketch {
	seeds := make([]uint64, opts.cmsDepth)
	for i := range opts.cmsDepth {
		seeds[i] = uint64(i*1315423911 + 1)
	}

	return &CountMinSketch{
		width:      uint64(opts.cmsWidth),
		depth:      uint64(opts.cmsDepth),
		table:      make([]uint32, opts.cmsWidth*opts.cmsDepth),
		seeds:      seeds,
		itemsAdded: 0,
		windowSize: uint64(10 * opts.cmsWidth * opts.cmsDepth),
	}
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
	cache           map[string]*Node
	maxBytes        uint64
	usedBytes       uint64
	head            *Node
	tail            *Node
	expires         map[string]time.Time // 过期时间映射
	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
	closeCh         chan struct{}
}

func NewLRUCache(opts LRUOptions) *LRUCache {
	cleanupInterval := opts.cleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
	}
	head := &Node{}
	tail := &Node{}

	head.next = tail
	tail.prev = head
	return &LRUCache{
		cache:           make(map[string]*Node),
		expires:         make(map[string]time.Time),
		maxBytes:        opts.maxBytes,
		head:            head,
		tail:            tail,
		cleanupInterval: cleanupInterval,
		cleanupTicker:   time.NewTicker(cleanupInterval),
		closeCh:         make(chan struct{}),
	}
}

func NewNode(key string, val Value) *Node {
	node := &Node{
		key: key,
		val: val,
	}
	return node
}

// 提取基础哈希计算，避免循环内重复创建对象和计算哈希
func (cms *CountMinSketch) baseHash(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func (cms *CountMinSketch) insert(key string) {
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

func (cms *CountMinSketch) estimate(key string) uint64 {
	h := cms.baseHash(key)
	Min := uint32(^uint32(0))
	for i := range cms.depth {
		index := uint64(i)*cms.width + ((h ^ cms.seeds[i]) % cms.width)
		Min = min(Min, cms.table[index])
	}
	return uint64(Min)
}

type LRUOptions struct {
	maxBytes        uint64
	cleanupInterval time.Duration
}

func NewLRUOptions() LRUOptions {
	return LRUOptions{
		maxBytes:        8192,
		cleanupInterval: 5,
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

func newTinyLFU(opts Options) *TinyLFU {
	t := &TinyLFU{
		lru: NewLRUCache(LRUOptions{
			maxBytes:        uint64(opts.MaxBytes),
			cleanupInterval: opts.CleanupInterval,
		}),
		cms: NewCountMinSketch(CMSOptions{
			cmsWidth: uint64(opts.CMSWidth),
			cmsDepth: uint64(opts.CMSDepth),
		}),
	}
	go t.cleanupLoop()
	return t
}

func (t *TinyLFU) Get(key string) (Value, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 惰性删除检查（否则会读到已过期但还未被后台协程清理的数据）
	if expTime, ok := t.lru.expires[key]; ok && time.Now().After(expTime) {
		if node, exists := t.lru.cache[key]; exists {
			t.lru.removeNode(node)
			t.lru.usedBytes -= node.size()
			delete(t.lru.cache, node.key)
		}
		delete(t.lru.expires, key)
		return nil, false
	}

	t.cms.insert(key) // 关键附加动作：每次读取都增加频率

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
	candidateFreq := t.cms.estimate(node.key)
	tempUsedByts := t.lru.usedBytes
	if nodeSize > t.lru.maxBytes {
		return -1, false
	}
	cur := t.lru.tail.prev
	for tempUsedByts+nodeSize > t.lru.maxBytes {
		if cur == t.lru.head {
			return -1, false
		}
		curFreq := t.cms.estimate(cur.key)
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

func (t *TinyLFU) SetWithExpiration(key string, val Value, expiration time.Duration) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var expTime time.Time
	if expiration > 0 {
		expTime = time.Now().Add(expiration)
		t.lru.expires[key] = expTime
	} else {
		delete(t.lru.expires, key)
	}

	if node, ok := t.lru.cache[key]; ok {
		t.lru.usedBytes = t.lru.usedBytes - uint64(node.val.Len()) + uint64(val.Len())
		node.val = val
		t.lru.moveToHead(node)
		t.cms.insert(key)
	} else {
		node := NewNode(key, val)
		if count, ok := t.canEvictWithEstimatedSize(node); ok {
			for range count {
				victim := t.lru.tail.prev
				t.lru.usedBytes -= victim.size()
				t.lru.removeNode(victim)
				delete(t.lru.cache, victim.key)
			}
			t.lru.addToHead(node)
			t.lru.usedBytes += node.size()
			t.lru.cache[key] = node
			t.cms.insert(key)
		} else {
			return nil
		}
	}
	return nil
}

func (t *TinyLFU) Set(key string, val Value) error {
	return t.SetWithExpiration(key, val, -1)
}

func (t *TinyLFU) Delete(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.lru.expires, key)
	if node, ok := t.lru.cache[key]; ok {
		t.lru.removeNode(node)
		t.lru.usedBytes -= node.size()
		delete(t.lru.cache, key)
		return true
	}
	return false
}

func (t *TinyLFU) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.cms.table)
	t.cms.itemsAdded = 0

	clear(t.lru.cache)
	clear(t.lru.expires)
	t.lru.head.next = t.lru.tail
	t.lru.tail.prev = t.lru.head
	t.lru.usedBytes = 0
}

func (t *TinyLFU) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.lru.cache)
}

func (t *TinyLFU) removeExpiredEntries() {
	now := time.Now()
	for key, expTime := range t.lru.expires {
		if now.After(expTime) {
			if node, ok := t.lru.cache[key]; ok {
				t.lru.removeNode(node)
				t.lru.usedBytes -= node.size()
				delete(t.lru.cache, node.key)
			}
			delete(t.lru.expires, key)
		}
	}
}

func (t *TinyLFU) cleanupLoop() {
	for {
		select {
		case <-t.lru.cleanupTicker.C:
			t.mu.Lock()
			t.removeExpiredEntries()
			t.mu.Unlock()
		case <-t.lru.closeCh:
			return
		}
	}
}

func (t *TinyLFU) Close() {
	if t.lru != nil && t.lru.cleanupTicker != nil {
		t.lru.cleanupTicker.Stop()
		close(t.lru.closeCh)
	}
}
