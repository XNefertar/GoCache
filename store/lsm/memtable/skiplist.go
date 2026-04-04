package memtable

import (
	"math/rand"
	"sync"
)

const (
	MaxLevel    = 32
	Probability = 0.25
)

type Node struct {
	key   string
	value []byte
	next  []*Node
}

type SkipList struct {
	head  *Node        // 头节点
	level int          // 当前跳表的最高层数
	size  int          // 元素个数
	mu    sync.RWMutex // 读写锁
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:  &Node{next: make([]*Node, MaxLevel)},
		level: 1,
	}
}

func (sl *SkipList) randomLevel() int {
	level := 1
	for rand.Float64() < Probability && level < MaxLevel {
		level += 1
	}
	return level
}

func (sl *SkipList) Put(key string, value []byte) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*Node, MaxLevel)
	cur := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for cur.next[i] != nil && cur.next[i].key < key {
			cur = cur.next[i]
		}
		update[i] = cur
	}

	if cur.next[0] != nil && cur.next[0].key == key {
		cur.next[0].value = value
		return
	}

	randomLevel := sl.randomLevel()
	if randomLevel > sl.level {
		for i := sl.level; i < randomLevel; i++ {
			update[i] = sl.head
		}
		sl.level = randomLevel
	}

	newNode := &Node{
		key:   key,
		value: value,
		next:  make([]*Node, randomLevel),
	}

	for i := 0; i < randomLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}
	sl.size++
}

func (sl *SkipList) Get(key string) ([]byte, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	cur := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for cur.next[i] != nil && cur.next[i].key < key {
			cur = cur.next[i]
		}
	}
	cur = cur.next[0]
	if cur != nil && cur.key == key {
		return cur.value, true
	}
	return nil, false
}

type Iterator struct {
	cur *Node
}

func (sl *SkipList) NewIterator() *Iterator {
	return &Iterator{cur: sl.head.next[0]}
}

func (it *Iterator) Valid() bool {
	return it.cur != nil
}

func (it *Iterator) Next() {
	it.cur = it.cur.next[0]
}

func (it *Iterator) Key() string {
	return it.cur.key
}

func (it *Iterator) Value() []byte {
	return it.cur.value
}
