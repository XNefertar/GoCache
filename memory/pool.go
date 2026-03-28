package memory

import (
	"math/bits"
	"sync"
)

var pools [11]*sync.Pool

const (
	MinPoolSize = 64
	MaxPoolSize = 65536
)

func init() {
	for i := range len(pools) {
		size := MinPoolSize << i
		pools[i] = &sync.Pool{
			New: func(s int) func() any {
				return func() any {
					b := make([]byte, s)
					return &b
				}
			}(size),
		}
	}
}

func calcPoolIdx(size int) int {
	if size <= MinPoolSize {
		return 0
	}
	// 6 -> 2^6 -> 64byte
	return bits.Len(uint(size-1)) - 6
}

func AllocByte(size int) []byte {
	if size <= 0 {
		return nil
	}
	if size > MaxPoolSize {
		return make([]byte, size)
	}
	idx := calcPoolIdx(size)
	ptr := pools[idx].Get().(*[]byte)
	b := *ptr
	// 调整切片长度适配用户需求
	return b[:size]
}

func FreeByte(b []byte) {
	c := cap(b)
	if c == 0 || c > MaxPoolSize {
		return
	}
	idx := calcPoolIdx(c)
	b = b[:0]
	pools[idx].Put(&b)
}
