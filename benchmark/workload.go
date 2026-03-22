package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	kamacache "github.com/youngyangyang04/KamaCache-Go"
)

type RequestGenMode int

const (
	Zipfian RequestGenMode = iota
	Scan
)

func modeString(mode RequestGenMode) string {
	switch mode {
	case Zipfian:
		return "Zipfian"
	case Scan:
		return "Scan"
	default:
		return "Unknown"
	}
}

type Request string

func GenerateWorkload(mode RequestGenMode, keySpace int, totalRequests int) []Request {
	var requests []Request

	// Initialize Zipf generator if needed
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	zipfGenerator := rand.NewZipf(r, 1.2, 10, uint64(keySpace-1))

	switch mode {
	case Zipfian:
		for i := 0; i < totalRequests; i++ {
			keyId := zipfGenerator.Uint64()
			requests = append(requests, Request(fmt.Sprintf("key-%d", keyId%uint64(keySpace))))
		}
	case Scan:
		for i := 0; i < totalRequests; i++ {
			requests = append(requests, Request(fmt.Sprintf("key-%d", i%keySpace)))
		}
	}
	return requests
}

func splitRequest(requests []Request, level int) [][]Request {
	if level <= 0 || len(requests) == 0 {
		return nil
	}
	requestLists := make([][]Request, level)
	totalSize := len(requests)
	for i := 0; i < totalSize; i++ {
		idx := i % level
		requestLists[idx] = append(requestLists[idx], requests[i])
	}
	return requestLists
}

type BenchResult struct {
	hit  int
	miss int
}

func aggregateAndPrint(results chan BenchResult, totalRequests int, totalDuration time.Duration, mode string) {
	totalHits := 0
	totalMisses := 0

	for res := range results {
		totalHits += res.hit
		totalMisses += res.miss
	}

	hitRate := float64(totalHits) / float64(totalHits+totalMisses) * 100
	qps := float64(totalRequests) / totalDuration.Seconds()

	fmt.Printf("=== Benchmark Report ===\n")
	fmt.Printf("Traffic Mode:   %s\n", mode)
	fmt.Printf("Total Requests: %d\n", totalRequests)
	fmt.Printf("Total Duration: %v\n", totalDuration)
	fmt.Printf("Throughput (QPS): %.2f ops/s\n", qps)
	fmt.Printf("Hit Rate:       %.2f%% (%d hits / %d misses)\n", hitRate, totalHits, totalMisses)
	fmt.Printf("========================\n\n")
}

func RunBenchmark(mode RequestGenMode, keySpace int, totalRequests int, level int, cacheGroup *kamacache.Group) {
	fmt.Printf("Generating %s workload data (Total Req: %d, KeySpace: %d)...\n", modeString(mode), totalRequests, keySpace)
	allRequests := GenerateWorkload(mode, keySpace, totalRequests)
	requestLists := splitRequest(allRequests, level)

	var wg sync.WaitGroup
	results := make(chan BenchResult, level)

	fmt.Printf("Starting benchmark with %d concurrent workers...\n", level)
	startTime := time.Now()

	for i := 0; i < level; i++ {
		wg.Add(1)
		go func(requests []Request) {
			defer wg.Done()
			hitCount, missCount := 0, 0
			// 为了极致性能，使用同一个context
			ctx := context.Background()

			for _, req := range requests {
				view, err := cacheGroup.Get(ctx, string(req))
				// 在 KamaCache 中，如果没有底层数据会返回 error
				// 由于我们在 Group 已经通过 Getter 解决了 Miss 时的回源写入
				// 我们需要检查是否有合法的真实响应 (比如我们造假数据能返回真实的字节)
				if err == nil && view.Len() > 0 {
					hitCount++
				} else {
					missCount++
				}
			}
			results <- BenchResult{hit: hitCount, miss: missCount}
		}(requestLists[i])
	}

	wg.Wait()
	close(results)
	totalDuration := time.Since(startTime)

	aggregateAndPrint(results, totalRequests, totalDuration, modeString(mode))
}
