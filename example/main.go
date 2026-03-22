package main

import (
	"fmt"
	"os"
)

func main() {
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
