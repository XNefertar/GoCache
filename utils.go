package kamacache

import (
	"strings"

	"github.com/XNefertar/GoCache/memory"
)

// JoinAddr 优化字符串拼接
func JoinAddr(host, port string) string {
	size := len(host) + 1 + len(port)
	buf := memory.AllocByte(size)
	defer memory.FreeByte(buf)

	n := copy(buf, host)
	buf[n] = ':'
	copy(buf[n+1:], port)

	return string(buf)
}

func ValidPeerAddr(addr string) bool {
	idx := strings.IndexByte(addr, ':')
	if idx == -1 || idx == len(addr)-1 {
		return false
	}
	host := addr[:idx]

	if host != "localhost" {
		dots := 0
		for i := 0; i < len(host); i++ {
			if host[i] == '.' {
				dots++
			}
		}
		if dots != 3 {
			return false
		}
	}
	return true
}
