package kamacache

import (
	"github.com/youngyangyang04/KamaCache-Go/memory"
)

// ByteView 只读的字节视图，用于缓存数据
type ByteView struct {
	b []byte
}

func (b ByteView) Len() int {
	return len(b.b)
}

func (b ByteView) ByteSlice() []byte {
	return cloneBytes(b.b)
}

// ByteSlicePool 使用内存池借用临时切片，调用方必须确保使用后释放 (memory.FreeByte)
func (b ByteView) ByteSlicePool() []byte {
	c := memory.AllocByte(len(b.b))
	copy(c, b.b)
	return c
}

func (b ByteView) String() string {
	return string(b.b)
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
