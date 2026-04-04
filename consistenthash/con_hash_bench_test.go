package consistenthash

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/XNefertar/GoCache/memory"
)

// 模拟旧代码：完全依赖 fmt.Sprintf 和强转 []byte 的产生大量垃圾的写法
func BenchmarkHashVirtualNode_OldSprintf(b *testing.B) {
	node := "192.168.1.100:8080"

	// 在真正执行时间测试前，重置计时器
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// 这是我们在代码里改掉之前的老写法
		// fmt.Sprintf 拼接，并强制转换成 []byte
		keyByte := []byte(fmt.Sprintf("%s-%d", node, i%100))
		_ = keyByte // 模拟发送给 hash 函数计算
	}
}

var globalResult []byte // 防止编译器优化

func BenchmarkHashVirtualNode_NewMemoryPool(b *testing.B) {
	node := "192.168.1.100:8080"
	b.ReportAllocs() // 开启内存分配统计
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// 1. 预估大小
		size := len(node) + 1 + 3
		buf := memory.AllocByte(size)

		// 2. 组装
		n := copy(buf, node)
		buf[n] = '-'

		// 3. 写入数字
		// 确保传给 AppendInt 的切片拥有完整的 cap，防止其触发重新分配
		finalBuf := strconv.AppendInt(buf[:n+1], int64(i%100), 10)

		// 4. 模拟使用：赋值给全局变量防止优化
		globalResult = finalBuf

		// 5. 归还内存
		memory.FreeByte(buf)
	}
}
