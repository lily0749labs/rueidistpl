# Changelog

本项目的重要变更都会记录在此文件中。版本号遵循 [Semantic Versioning](https://semver.org/)。

## [Unreleased]

### Fixed

- `Cache.GetWithTTL` 原子返回对象和 Redis key 的毫秒 TTL，不再返回客户端缓存 TTL。
- `Array.LastItems` 跳过空槽，保留合法零值和 JSON null。
- 修复命名空间首个 hash tag 为空 `{}` 时组合 key 跨 Slot 的问题；普通命名空间的键名保持不变。

### Changed

- Go 最低版本升级到 1.26.0，更新直接及间接依赖。
- `DeleteEach`/`ExistsEach` 按最多 512 个命令分块执行，减少中间结果分配，并在 context 取消后停止发送剩余分块。

### Added

- 排行榜 `Page` 接口，单页最多 1000 项，校验偏移、页大小及下标溢出。
- 缓存 TTL、数组空槽、组合 key、排行榜分页和批量操作边界的回归测试。

## [0.1.1] - 2026-08-23

### Changed

- 优化客户端配置与依赖，整理发布文档。

## [0.1.0] - 2026-08-23

### Added

- 首次公开发布。
- 提供 Rueidis 客户端、统一 key 前缀、JSON 编解码、客户端缓存和分布式锁封装。
- 支持常用 Redis 数据结构、Stream、Pub/Sub、幂等控制和轻量延迟队列。
- 支持 Redis 8 的 JSON、TimeSeries、Vector Set、概率结构，以及 Redis 8.8+ Array。
- 提供跨 Cluster Slot 的逐 key 删除和存在性检查，以及组合结构的 hash tag 管理。

[Unreleased]: https://github.com/lily0749labs/rueidistpl/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/lily0749labs/rueidistpl/releases/tag/v0.1.1
[0.1.0]: https://github.com/lily0749labs/rueidistpl/releases/tag/v0.1.0
