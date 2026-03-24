package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
)

func main() {
	// 启动 pprof 性能分析服务器 (后台运行)
	// 在浏览器访问 http://localhost:6060/debug/pprof/ 查看
	go func() {
		fmt.Println("Starting pprof server on :6060")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			fmt.Printf("pprof server failed: %v\n", err)
		}
	}()

	// 定义子命令
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run example/*.go <command>")
		fmt.Println("Commands:")
		fmt.Println("  bench    - Run single node cache performance benchmark")
		fmt.Println("  dist     - Run distributed cache cluster test")
		os.Exit(1)
	}

	cmd := os.Args[1]

	newArgs := append([]string{os.Args[0]}, os.Args[2:]...)
	os.Args = newArgs

	switch cmd {
	case "bench":
		fmt.Println("=== Starting Benchmark Mode ===")
		runBenchmarkMode()
	case "dist":
		fmt.Println("=== Starting Distributed Mode ===")
		runDistributedTest()
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		os.Exit(1)
	}
}
