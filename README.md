# KamaCache-Go

KamaCache-Go 是一个高可用、可扩展的分布式内存缓存系统，使用 Go 语言开发。它采用了类似 groupcache 的无中心化架构设计，通过一致性哈希和 gRPC 通信实现节点间的动态请求路由和负载均衡。

## 🌟 核心特性

- **分布式架构与高可用**：无单点故障，节点之间平等通信。支持水平扩展。
- **服务注册与发现**：内置集成 Etcd 进行自动化的节点心跳保活和动态服务发现，实现去中心化的平滑扩缩容。
- **一致性哈希路由**：内置一致性哈希算法，结合虚拟节点机制，使得集群节点增删时最大程度减小缓存失效带来的雪崩影响。具备动态负载均衡能力，可根据请求分布自动调整虚拟节点。
- **防止缓存击穿**：内置 `singleflight` 机制。对于海量并发访问同一个未命中缓存的 Key，只允许一个请求去回源获取数据，其他请求阻塞等待共享结果，有效防止底层数据库被压垮。
- **多策略内存管理 (Eviction)**：抽象出底层的 `Store` 接口，内置支持多种缓存淘汰策略（如 `LRU`, `LRU-2`, `TinyLFU` 等），能根据场景需求灵活配置，从而避免偶发性遍历导致热点数据被淘汰。
- **命名空间隔离 (Group)**：支持在一个进程中创建多个隔离的缓存 Group，每个 Group 拥有独立的回源逻辑 (`GetterFunc`) 和内存上限。
- **gRPC 通信层**：节点之间的通信基于高效的 gRPC 框架，保证网络传输的高效与可靠。
- **数据一致性机制**：分布式读写同步，本地无数据自动通过 Peer 查找，Set 和 Delete 操作会在非同步请求的场景下自动广播至其他节点，保证多节点间状态协调。

## 🏗️ 架构概览

KamaCache-Go 的主要模块包括：

- **节点网络路由** (`server.go`, `client.go`, `pb/`)：封装了底层的通信细节，对外提供标准 API。
- **分组管理** (`group.go`, `cache.go`)：处理查询、并发控制、本地缓存与远程拉取的协同逻辑。
- **一致性哈希** (`consistenthash/`)：负责 Key 到节点的映射逻辑和动态哈希环维护。
- **防击穿控制** (`singleflight/`)：并发请求结果合并。
- **注册中心** (`registry/`)：通过 Etcd 处理节点的租约与发现。
- **内存存储引擎** (`store/`)：负责真正的数据存取和空间淘汰。

## 🚀 快速开始

### 依赖环境

- Go 1.18+
- Etcd 服务端 (推荐 3.x)

### 安装

```bash
go get github.com/.../KamaCache-Go
```

### 使用示例

以下是一个简单的示例，展示如何启动一个 KamaCache 节点并配置回源逻辑：

```go
package main

import (
"context"
"fmt"
"log"

lcache "github.com/.../KamaCache-Go"
)

func main() {
addr := "127.0.0.1:8001"

// 1. 创建和启动节点 Server，指定 Etcd 地址
node, err := lcache.NewServer(addr, "kama-cache", lcache.WithEtcdEndpoints([]string{"127.0.0.1:2379"}))
if err != nil {
log.Fatalf("启动 server 失败: %v", err)
}
go node.Start()

// 2. 创建一个 Group 并配置回源逻辑 GetterFunc 和容量上限
// 当所有节点的缓存中都不存在时，会调用此函数从备用数据源获取数据
group := lcache.NewGroup("scores", 2<<20, lcache.GetterFunc(
func(ctx context.Context, key string) ([]byte, error) {
log.Printf("[缓慢的数据库查询] 正在获取 Key: %s", key)
return []byte("Data From DB for " + key), nil
}))

// 3. 创建 PeerPicker (节点选择器) 并注册到 Group
picker, err := lcache.NewClientPicker(addr)
if err != nil {
log.Fatalf("创建客户端失败: %v", err)
}
group.RegisterPeers(picker)

// 4. 进行业务查询及操作
ctx := context.Background()

// 写入缓存 (自动同步)
group.Set(ctx, "tom", []byte("95"))

// 查询缓存 (自动进行哈希路由或回源)
val, err := group.Get(ctx, "tom")
if err != nil {
log.Printf("获取失败: %v", err)
} else {
fmt.Printf("Get 结果: %s\n", string(val))
}
}
```

## 📈 运维与监控

KamaCache 在内部记录了各项关键指标的统计数据包（Hit Rate, Miss Rate, Load Time 等），开发者可以根据需求扩展 Prometheus/Metrics 导出模块，对集群的健康情况和缓存命中率进行大盘监控。

## 📜 许可证

开源协议: [MIT License](LICENSE)
