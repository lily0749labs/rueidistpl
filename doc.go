// Package rueidistpl 在 Rueidis、gocache 和 rueidislock 之上提供面向业务的轻量封装。
//
// 包内统一处理 Redis 连接配置、key 前缀、JSON 编解码和常用数据结构，包括 Bitmap、Geo、
// HyperLogLog、Redis 8 的 JSON/TimeSeries/Vector Set/概率结构，以及 Redis 8.8+ 原生稀疏
// Array，同时保留 Redis.Client 原生出口。连接管理、客户端缓存和分布式锁续期仍由成熟的
// 上游库负责，本包不重复实现 Redis 协议、连接池或锁算法。
package rueidistpl
