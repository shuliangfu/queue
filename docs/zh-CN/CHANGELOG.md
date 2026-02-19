# 变更日志

[English](../en-US/CHANGELOG.md) | 中文 (Chinese)

本文档记录 @dreamer/queue 的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

---

## [1.0.1] - 2026-02-19

### 新增

- **文档**：文档结构调整为 `docs/en-US` 与
  `docs/zh-CN`；CHANGELOG、TEST_REPORT、中文 README 迁至 docs
  下，并增加语言切换链接。
- **文档**：TEST_REPORT 完整中文版（无删减）。
- **i18n**：适配器与管理器错误文案国际化（en-US、zh-CN）；提供 `$tr`、
  `setQueueLocale`，语言从环境变量自动检测；新增依赖 `@dreamer/i18n`。

### 变更

- **测试报告**：总体统计更新为 113 项测试（含生命周期钩子），执行时间约 2 分 54
  秒（Deno）；各文件用例数与 `deno test` 输出一致。

---

## [1.0.0] - 2026-02-07

### 新增

- **稳定版发布**：首枚稳定版本，API 稳定

- **多队列支持**：
  - 多个独立的队列实例
  - 任务隔离，互不阻塞
  - 每个队列有独立的处理循环和并发控制

- **队列适配器**：
  - MemoryQueueAdapter - 内存适配器，仅用于开发/测试
  - RedisQueueAdapter - 基于 Redis 的持久化（推荐）
  - MemcachedQueueAdapter - 基于 Memcached 的内存缓存
  - MongoDBQueueAdapter - 基于 MongoDB 的持久化
  - RabbitMQQueueAdapter - 基于 RabbitMQ 的持久化（企业级）
  - 统一 QueueAdapter 接口

- **任务队列**：
  - FIFO 队列（先进先出）
  - 优先级队列（low、normal、high、urgent）
  - 延迟队列（延迟执行）

- **任务调度**：
  - 定时任务（支持 5 字段和 6 字段 Cron 表达式，UTC）
  - 延迟任务（指定时间后执行）
  - 周期性任务（间隔执行）

- **任务管理**：
  - 任务重试（可配置最大重试次数）
  - 任务状态追踪（pending、processing、completed、failed）
  - 任务优先级
  - 任务超时
  - 超时处理中任务的自动恢复

- **并发控制**：
  - 每个队列独立的并发控制
  - 可配置每个队列的最大并发数

- **性能优化**：
  - Redis MGET 批量获取大量任务场景
  - MongoDB 聚合管道获取最高优先级任务
  - 动态延迟轮询（有任务时短延迟，无任务时递增）

- **服务容器集成**：
  - createQueueManager 工厂函数
  - QueueManager.fromContainer 静态方法
  - 命名管理器支持
  - @dreamer/service 依赖注入

### 兼容性

- Deno 2.6+
- Bun 1.3.5+
- Redis（Redis 适配器）
- Memcached（Memcached 适配器）
- MongoDB（MongoDB 适配器）
- RabbitMQ（RabbitMQ 适配器）
