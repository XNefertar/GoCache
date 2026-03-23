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

type WTinyLFU struct {
	mu           sync.Mutex
	windowLRU    *LRUCache
	probationLRU *LRUCache
	protectedLRU *LRUCache
	cms          *CountMinSketch
}

func newTinyLFU(opts Options) *WTinyLFU {
	t := &WTinyLFU{
		windowLRU: NewLRUCache(LRUOptions{
			maxBytes:        uint64(float64(opts.MaxBytes) * 0.01),
			cleanupInterval: opts.CleanupInterval,
		}),
		probationLRU: NewLRUCache(LRUOptions{
			maxBytes:        uint64(float64(opts.MaxBytes) * 0.99 * 0.8),
			cleanupInterval: opts.CleanupInterval,
		}),
		protectedLRU: NewLRUCache(LRUOptions{
			maxBytes:        uint64(float64(opts.MaxBytes) * 0.99 * 0.2),
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

func (t *WTinyLFU) addToProtectedWithEnoughSpace(node *Node, expiration time.Duration) {
	t.protectedLRU.addToHead(node)
	t.protectedLRU.cache[node.key] = node
	t.protectedLRU.usedBytes += node.size()
	if expiration > 0 {
		var expTime time.Time
		expTime = time.Now().Add(expiration)
		t.protectedLRU.expires[node.key] = expTime
	}
}

func (t *WTinyLFU) evictFromProbationIfFull(neededBytes uint64) {
	remainBytes := t.probationLRU.maxBytes - t.probationLRU.usedBytes
	for remainBytes < neededBytes {
		victim := t.probationLRU.tail.prev
		remainBytes += victim.size()
		t.probationLRU.removeNode(victim)
		delete(t.probationLRU.cache, victim.key)
		delete(t.probationLRU.expires, victim.key)
		t.probationLRU.usedBytes -= victim.size()
	}
}

func (t *WTinyLFU) evictFromProtectedToProbation(neededBytes uint64) {
	if t.probationLRU.maxBytes < neededBytes {
		return
	}
	remainBytes := t.protectedLRU.maxBytes - t.protectedLRU.usedBytes
	for remainBytes < neededBytes {
		victim := t.protectedLRU.tail.prev
		if victim == t.protectedLRU.head {
			break
		}
		remainBytes += victim.size()
		t.protectedLRU.removeNode(victim)
		delete(t.protectedLRU.cache, victim.key)
		victimExpTime, victimHasExpTime := t.protectedLRU.expires[victim.key]
		delete(t.protectedLRU.expires, victim.key)
		t.protectedLRU.usedBytes -= victim.size()

		t.evictFromProbationIfFull(victim.size())
		t.probationLRU.addToHead(victim)
		t.probationLRU.cache[victim.key] = victim
		if victimHasExpTime {
			t.probationLRU.expires[victim.key] = victimExpTime
		}
		t.probationLRU.usedBytes += victim.size()
	}
}

func (t *WTinyLFU) addToProtected(node *Node, expiration time.Duration) {
	remainBytes := t.protectedLRU.maxBytes - t.protectedLRU.usedBytes
	if remainBytes >= node.size() {
		t.addToProtectedWithEnoughSpace(node, expiration)
	} else {
		t.evictFromProtectedToProbation(node.size())
		t.addToProtectedWithEnoughSpace(node, expiration)
	}
}

func (lru *LRUCache) renew(node *Node, key string, val Value, expiration time.Duration) {
	lru.usedBytes = lru.usedBytes - (uint64(node.val.Len()) - uint64(val.Len()))
	node.val = val
	lru.moveToHead(node)
	var expTime time.Time
	if expiration > 0 {
		expTime = time.Now().Add(expiration)
		lru.expires[key] = expTime
	} else {
		delete(lru.expires, key)
	}
}

func (lru *LRUCache) remove(key string) {
	if node, ok := lru.cache[key]; ok {
		lru.removeNode(node)
		lru.usedBytes -= node.size()
		delete(lru.cache, key)
		delete(lru.expires, key)
	}
}

func (lru *LRUCache) get(key string) (*Node, bool) {
	node, ok := lru.cache[key]
	if !ok {
		return nil, false
	}

	if expTime, exists := lru.expires[key]; exists && time.Now().After(expTime) {
		lru.remove(key)
		return nil, false
	}
	lru.moveToHead(node)
	return node, true
}

func (t *WTinyLFU) SetWithExpiration(key string, val Value, expiration time.Duration) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if node, ok := t.windowLRU.cache[key]; ok {
		t.windowLRU.renew(node, key, val, expiration)
		return nil
	}
	if node, ok := t.probationLRU.cache[key]; ok {
		t.probationLRU.removeNode(node)
		t.probationLRU.usedBytes -= node.size()
		delete(t.probationLRU.cache, key)
		delete(t.probationLRU.expires, key)

		node.val = val
		t.addToProtected(node, expiration)
	}
	if node, ok := t.protectedLRU.cache[key]; ok {
		t.protectedLRU.renew(node, key, val, expiration)
		return nil
	}
	t.cms.insert(key)

	// insert 逻辑
	node := NewNode(key, val)
	if node.size() > t.windowLRU.maxBytes {
		return nil
	}
	windowLRURemainBytes := t.windowLRU.maxBytes - t.windowLRU.usedBytes
	for windowLRURemainBytes < node.size() {
		victim := t.windowLRU.tail.prev
		if victim == t.windowLRU.head {
			break
		}
		windowLRURemainBytes += victim.size()
		t.windowLRU.usedBytes -= victim.size()
		delete(t.windowLRU.cache, victim.key)
		delete(t.windowLRU.expires, victim.key)

		if count, ok := t.canEvictWithEstimatedSize(victim); ok {
			for range count {
				cur := t.probationLRU.tail.prev
				t.probationLRU.removeNode(cur)
				delete(t.probationLRU.cache, cur.key)
				delete(t.probationLRU.expires, cur.key)
				t.probationLRU.usedBytes -= cur.size()
			}
			t.probationLRU.addToHead(victim)
			t.probationLRU.cache[victim.key] = victim
			if expiration > 0 {
				var expTime time.Time
				expTime = time.Now().Add(expiration)
				t.probationLRU.expires[victim.key] = expTime
			}
			t.probationLRU.usedBytes += victim.size()
		}
	}
	t.windowLRU.addToHead(node)
	t.windowLRU.cache[key] = node
	if expiration > 0 {
		var expTime time.Time
		expTime = time.Now().Add(expiration)
		t.windowLRU.expires[key] = expTime
	}
	t.windowLRU.usedBytes += node.size()
	return nil

}

func (t *WTinyLFU) Set(key string, val Value) error {
	return t.SetWithExpiration(key, val, -1)
}

func (t *WTinyLFU) Get(key string) (Value, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, lru := range []*LRUCache{t.windowLRU, t.probationLRU, t.protectedLRU} {
		if node, ok := lru.get(key); ok {
			// 晋升机制
			if lru == t.probationLRU {
				lru.removeNode(node)
				expTime, hasExp := lru.expires[key]
				delete(lru.cache, key)
				delete(lru.expires, key)
				lru.usedBytes -= node.size()

				var duration time.Duration = -1
				if hasExp {
					duration = time.Until(expTime)
				}
				t.addToProtected(node, duration)
			}
			t.cms.insert(key)
			return node.val, true
		}
	}
	return nil, false
}

func (t *WTinyLFU) canEvictWithEstimatedSize(node *Node) (int, bool) {
	if node.size() > t.probationLRU.maxBytes {
		return -1, false
	}
	candidateFreq := t.cms.estimate(node.key)
	remainBytes := t.probationLRU.maxBytes - t.probationLRU.usedBytes
	var needToEvictedCount uint64
	cur := t.probationLRU.tail.prev
	for remainBytes < node.size() {
		if cur == t.probationLRU.head {
			return -1, false
		}
		victimFreq := t.cms.estimate(cur.key)
		if victimFreq >= candidateFreq {
			return -1, false
		}
		needToEvictedCount++
		remainBytes += cur.size()
		cur = cur.prev
	}
	return int(needToEvictedCount), true
}

func (lru *LRUCache) delete(key string) bool {
	delete(lru.expires, key)
	if node, ok := lru.cache[key]; ok {
		lru.removeNode(node)
		lru.usedBytes -= node.size()
		delete(lru.cache, key)
		return true
	}
	return false
}

func (t *WTinyLFU) Delete(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, lru := range []*LRUCache{t.windowLRU, t.probationLRU, t.protectedLRU} {
		if lru.delete(key) {
			return true
		}
	}
	return false
}

func (t *WTinyLFU) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.cms.table)
	t.cms.itemsAdded = 0

	for _, lru := range []*LRUCache{t.windowLRU, t.probationLRU, t.protectedLRU} {
		clear(lru.cache)
		clear(lru.expires)
		lru.head.next = lru.tail
		lru.tail.prev = lru.head
		lru.usedBytes = 0
	}
}

func (t *WTinyLFU) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.windowLRU.cache) + len(t.probationLRU.cache) + len(t.protectedLRU.cache)
}

func (t *WTinyLFU) removeExpiredEntries() {
	for _, lru := range []*LRUCache{t.windowLRU, t.probationLRU, t.protectedLRU} {
		now := time.Now()
		for key, expTime := range lru.expires {
			if now.After(expTime) {
				lru.delete(key)
			}
		}
	}
}

func (t *WTinyLFU) cleanupLoop() {
	// 只需要用其中一个 ticker 来驱动，因为它们的清理周期是一样的
	for {
		select {
		case <-t.windowLRU.cleanupTicker.C:
			t.mu.Lock()
			t.removeExpiredEntries()
			t.mu.Unlock()
		case <-t.windowLRU.closeCh:
			return
		}
	}
}

func (t *WTinyLFU) Close() {
	// 遍历并关闭所有 LRU 分区的 ticker 和 channel，防止内存泄漏
	for _, lru := range []*LRUCache{t.windowLRU, t.probationLRU, t.protectedLRU} {
		if lru != nil && lru.cleanupTicker != nil {
			lru.cleanupTicker.Stop()
			close(lru.closeCh)
		}
	}
}
