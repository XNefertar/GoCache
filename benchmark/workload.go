package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	kamacache "github.com/XNefertar/GoCache"
	"github.com/XNefertar/GoCache/memory"
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

func fastKey(prefix string, id uint64) string {
	b := memory.AllocByte(len(prefix) + 20)
	defer memory.FreeByte(b)

	n := copy(b, prefix)
	// strconv.AppendUint 会把转换后的字节追加到我们借来的 slice 的已用长度后面
	// 所以我们需要对 b 进行正确的 slice 操作来传给 AppendUint
	res := strconv.AppendUint(b[:n], id, 10)
	return string(res)
}

func GenerateWorkload(mode RequestGenMode, keySpace int, totalRequests int) []Request {
	var requests []Request

	// Initialize Zipf generator if needed
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	zipfGenerator := rand.NewZipf(r, 1.2, 10, uint64(keySpace-1))

	switch mode {
	case Zipfian:
		for range totalRequests {
			keyId := zipfGenerator.Uint64()
			requests = append(requests, Request(fastKey("key-", keyId%uint64(keySpace))))
		}
	case Scan:
		for i := range totalRequests {
			requests = append(requests, Request(fastKey("key-", uint64(i%keySpace))))
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
	hist *hdrhistogram.Histogram // 用于记录延迟分布
}

func aggregateAndPrint(results chan BenchResult, totalRequests int, totalDuration time.Duration, mode string) {
	totalHits := 0
	totalMisses := 0
	mergedHist := hdrhistogram.New(1, 1000000000, 3) // 1ns to 1s, 3 sig figs

	for res := range results {
		totalHits += res.hit
		totalMisses += res.miss
		// 合并各个协程的柱状图数据
		if res.hist != nil {
			mergedHist.Merge(res.hist)
		}
	}

	hitRate := float64(totalHits) / float64(totalHits+totalMisses) * 100
	qps := float64(totalRequests) / totalDuration.Seconds()

	fmt.Printf("=== Benchmark Report ===\n")
	fmt.Printf("Traffic Mode:   %s\n", mode)
	fmt.Printf("Total Requests: %d\n", totalRequests)
	fmt.Printf("Total Duration: %v\n", totalDuration)
	fmt.Printf("Throughput (QPS): %.2f ops/s\n", qps)
	fmt.Printf("Hit Rate:       %.2f%% (%d hits / %d misses)\n", hitRate, totalHits, totalMisses)

	// 打印延迟分布信息（将纳秒转换为毫秒）
	fmt.Printf("Latency (P50):  %.3f ms\n", float64(mergedHist.ValueAtQuantile(50))/1e6)
	fmt.Printf("Latency (P90):  %.3f ms\n", float64(mergedHist.ValueAtQuantile(90))/1e6)
	fmt.Printf("Latency (P99):  %.3f ms\n", float64(mergedHist.ValueAtQuantile(99))/1e6)
	fmt.Printf("Latency (P99.9):%.3f ms\n", float64(mergedHist.ValueAtQuantile(99.9))/1e6)
	fmt.Printf("Latency (Max):  %.3f ms\n", float64(mergedHist.Max())/1e6)
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

			// 记录 1纳秒到 1000秒之间的分布，精度为 3 位有效数字
			localHist := hdrhistogram.New(1, 1000000000000, 3)

			// 为了极致性能，使用同一个context
			ctx := context.Background()

			for _, req := range requests {
				reqStart := time.Now()
				view, err := cacheGroup.Get(ctx, string(req))

				// 记录该次请求的延迟耗时（微秒级误差通过hdr合并解决）
				if errRecord := localHist.RecordValue(time.Since(reqStart).Nanoseconds()); errRecord != nil {
					// 处理超过上界的异常用例（一般不会发生）
				}

				// 在 KamaCache 中，如果没有底层数据会返回 error
				// 由于我们在 Group 已经通过 Getter 解决了 Miss 时的回源写入
				// 我们需要检查是否有合法的真实响应 (比如我们造假数据能返回真实的字节)
				if err == nil && view.Len() > 0 {
					hitCount++
				} else {
					missCount++
				}
			}
			results <- BenchResult{hit: hitCount, miss: missCount, hist: localHist}
		}(requestLists[i])
	}

	wg.Wait()
	close(results)
	totalDuration := time.Since(startTime)

	aggregateAndPrint(results, totalRequests, totalDuration, modeString(mode))
}
