package lsm

import (
	"time"

	"github.com/XNefertar/GoCache/store"
)

// DB 是 LSM 引擎的主结构体
type DB struct {
	// 这里未来会包含 memtable, wal, sstables 等组件
}

// Ensure DB implements store.Store
var _ store.Store = (*DB)(nil)

func init() {
	store.RegisterLSMFactory(NewLSM)
}

func NewLSM(opts store.Options) store.Store {
	return &DB{}
}

func (db *DB) Get(key string) (store.Value, bool) {
	// TODO: 从 MemTable -> Immutable MemTable -> SSTables 查找
	return nil, false
}

func (db *DB) Set(key string, value store.Value) error {
	// TODO: 写 WAL -> 写 MemTable
	return nil
}

func (db *DB) SetWithExpiration(key string, value store.Value, expiration time.Duration) error {
	// TODO: 实现带过期时间的写入
	return nil
}

func (db *DB) Delete(key string) bool {
	// TODO: 写入 Tombstone 标记
	return false
}

func (db *DB) Clear() {
	// TODO: 清理所有数据文件
}

func (db *DB) Len() int {
	// TODO: 返回近似数据量
	return 0
}

func (db *DB) Close() {
	// TODO: 优雅关闭，刷盘及释放资源
}
