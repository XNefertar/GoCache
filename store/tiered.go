package store

import (
	"hash/crc32"
	"log"
	"sync"
	"time"
)

type TieredStore struct {
	L1    Store              // 内存层
	L2    Store              // 磁盘层
	locks [256]*sync.RWMutex // 分段锁
}

func (ts *TieredStore) getLock(key string) *sync.RWMutex {
	hash := crc32.ChecksumIEEE([]byte(key))
	index := hash & 255
	return ts.locks[index]
}

func NewTieredStore(l1 Store, l2 Store) *TieredStore {
	ts := &TieredStore{
		L1: l1,
		L2: l2,
	}

	for i := range 256 {
		ts.locks[i] = &sync.RWMutex{}
	}
	return ts
}

func (ts *TieredStore) Get(key string) (Value, bool) {
	mu := ts.getLock(key)
	mu.RLock()
	if val, ok := ts.L1.Get(key); ok {
		mu.RUnlock()
		return val, true
	}

	mu.Lock()
	defer mu.Unlock()

	// double check
	if val, ok := ts.L1.Get(key); ok {
		return val, true
	}

	val, ok := ts.L2.Get(key)
	if !ok {
		return nil, false
	}
	ts.L1.Set(key, val)
	return val, true
}

func (ts *TieredStore) Set(key string, value Value) error {
	mu := ts.getLock(key)
	mu.Lock()
	defer mu.Unlock()

	if err := ts.L2.Set(key, value); err != nil {
		return err
	}
	ts.L1.Set(key, value)
	return nil
}

func (ts *TieredStore) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	mu := ts.getLock(key)
	mu.Lock()
	defer mu.Unlock()

	if err := ts.L2.SetWithExpiration(key, value, expiration); err != nil {
		return err
	}
	ts.L1.SetWithExpiration(key, value, expiration)
	return nil
}

func (ts *TieredStore) Delete(key string) bool {
	mu := ts.getLock(key)
	mu.Lock()
	defer mu.Unlock()

	ok2 := ts.L2.Delete(key)
	if !ok2 {
		log.Printf("L2 Delete failed for key: %s, cluster consistency might be affected", key)
		return false
	}
	ts.L1.Delete(key)
	return true
}

func (ts *TieredStore) Clear() {
	ts.L1.Clear()
	ts.L2.Clear()
}

func (ts *TieredStore) Len() int {
	// 注意：由于底层 L2 采用 LSM 树架构，其写入和删除为追加写（Append）和墓碑（Tombstone）机制。
	// 在后台 Compaction 尚未触发前，此处的 Len 可能包含历史版本和墓碑，返回值是一个近似虚高值 (Approximate Size)。
	// 为了不破坏 LSM 的极速写性能（避免 Set 时前置检查），我们接受这种最终一致的统计偏差。
	return ts.L2.Len()
}

func (ts *TieredStore) Close() {
	ts.L1.Close()
	ts.L2.Close()
}
