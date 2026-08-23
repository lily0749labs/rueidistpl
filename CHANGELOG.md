# Changelog

本项目的重要变更都会记录在此文件中。版本号遵循 [Semantic Versioning](https://semver.org/)。

## [Unreleased]

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
