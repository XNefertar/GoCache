package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	kamacache "github.com/youngyangyang04/KamaCache-Go"
	"github.com/youngyangyang04/KamaCache-Go/benchmark"
)

func runBenchmarkMode() {
	logrus.SetLevel(logrus.ErrorLevel) // 关掉一些日志以免刷屏
	var dbMissCount atomic.Int64

	// 创建一个模拟数据库，延迟为一定时间，返回固定长度的值（例如 1KB）
	mockDBGetter := kamacache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		dbMissCount.Add(1)
		// 模拟返回 1KB 数据
		return make([]byte, 1024), nil
	})

	fmt.Println("Initializing KamaCache Group (Capacity: 10MB)")
	opts := kamacache.DefaultCacheOptions()
	opts.MaxBytes = 10 * 1024 * 1024 // 10MB

	cacheGroup := kamacache.NewGroup("benchmark-group", opts.MaxBytes, mockDBGetter, kamacache.WithCacheOptions(opts))

	// 参数设定
	keySpace := 50000       // 总数据量 5万
	totalRequests := 500000 // 50万次请求
	concurrency := 16       // 并发

	// 1. 测试 Zipfian 模式
	fmt.Println("\n>>> Testing Zipfian Mode (Hotspot)")
	dbMissCount.Store(0)
	benchmark.RunBenchmark(benchmark.Zipfian, keySpace, totalRequests, concurrency, cacheGroup)

	misses := dbMissCount.Load()
	realHitRate := float64(totalRequests-int(misses)) / float64(totalRequests) * 100
	fmt.Printf("[Real Metrics via DB] Hit Rate: %.2f%% (%d Misses / %d requests)\n", realHitRate, misses, totalRequests)

	// ==========================================

	// 重新创建组来测试
	cacheGroup2 := kamacache.NewGroup("benchmark-group-2", opts.MaxBytes, mockDBGetter, kamacache.WithCacheOptions(opts))

	// 2. 测试 Scan 模式
	fmt.Println("\n>>> Testing Scan Mode (Full Table Scan)")
	dbMissCount.Store(0)
	benchmark.RunBenchmark(benchmark.Scan, keySpace, totalRequests, concurrency, cacheGroup2)

	misses2 := dbMissCount.Load()
	realHitRate2 := float64(totalRequests-int(misses2)) / float64(totalRequests) * 100
	fmt.Printf("[Real Metrics via DB] Hit Rate: %.2f%% (%d Misses / %d requests)\n\n", realHitRate2, misses2, totalRequests)
}
