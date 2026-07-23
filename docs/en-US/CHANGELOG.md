# Changelog

English | [中文 (Chinese)](../zh-CN/CHANGELOG.md)

All notable changes to @dreamer/queue are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.1.0] - 2026-07-23

### Added

- **Node.js 22+ compatibility**: Full cross-runtime support (Deno, Bun, Node.js).
  Unit tests pass on all three runtimes (57 tests each on Bun/Node, 64 on Deno).
- **CI**: 9-job matrix (Deno/Bun/Node × Linux/macOS/Windows) running unit tests
  on every push/PR to `dev`.
- **Config**: `tsconfig.json`, `package.json` with `test:node` script
  (`tsx --test --test-force-exit`), `engines.node >= 22`, `minimumDependencyAge`.
- **Docs**: README (en + zh-CN) now documents Node.js 22+ installation and
  compatibility.

### Changed

- **Lazy npm imports**: Redis, MongoDB, and RabbitMQ adapters now dynamically
  `import()` their npm packages inside `connect()` instead of top-level static
  imports. This prevents eager loading of `mongodb`→`bson` / `redis` /
  `amqplib` when only `MemoryQueueAdapter` is used, fixing Bun module-load
  failures and improving startup performance. (Memcached adapter already used
  this pattern.)
- **Test split**: Unit tests (MemoryQueueAdapter, no external services) are
  separated from integration tests (memcached/mongodb/rabbitmq/redis, requiring
  Docker services). CI runs unit tests only; `deno task test:integration` runs
  the external-service tests locally.
- **Dependencies**: Upgraded `@dreamer/i18n` ^1.1.2, `@dreamer/test` ^1.2.3,
  `@dreamer/service` ^1.1.0, `@dreamer/runtime-adapter` ^1.2.2.
- **npm install**: Uses `--force` flag to bypass `@redis/test-utils` 404 (a
  non-existent devDependency of `@redis/client`; not needed at runtime).

### Compatibility

- Deno 2.9+
- Bun 1.3+
- Node.js 22+ (since v1.1.0)
- Redis (for Redis adapter), Memcached (for Memcached adapter), MongoDB (for
  MongoDB adapter), RabbitMQ (for RabbitMQ adapter)

---

## [1.0.1] - 2026-02-19

### Added

- **Docs**: Restructure documentation into `docs/en-US` and `docs/zh-CN`; move
  CHANGELOG, TEST_REPORT, and Chinese README under docs with language switcher
  links.
- **Docs**: Full Chinese translation of TEST_REPORT (no content reduction).
- **i18n**: Adapter and manager error messages localized (en-US, zh-CN); `$tr`,
  `setQueueLocale`, locale auto-detect from env; `@dreamer/i18n` dependency.

### Changed

- **Test report**: Updated overall statistics to 113 tests (including lifecycle
  hooks), execution time ~2m54s (Deno); per-file counts aligned with `deno test`
  output.

---

## [1.0.0] - 2026-02-07

### Added

- **Stable release**: First stable version with stable API

- **Multi-queue support**:
  - Multiple independent queue instances
  - Task isolation with no cross-blocking
  - Each queue has its own processing loop and concurrency control

- **Queue adapters**:
  - MemoryQueueAdapter - In-memory, dev/test only
  - RedisQueueAdapter - Redis-based persistence (recommended)
  - MemcachedQueueAdapter - Memcached in-memory cache
  - MongoDBQueueAdapter - MongoDB persistence
  - RabbitMQQueueAdapter - RabbitMQ persistence (enterprise)
  - Unified QueueAdapter interface

- **Task queues**:
  - FIFO queue (first-in first-out)
  - Priority queue (low, normal, high, urgent)
  - Delayed queue (scheduled execution)

- **Task scheduling**:
  - Cron jobs (5-field and 6-field Cron expressions, UTC)
  - Delayed jobs (execute after specified time)
  - Recurring jobs (interval execution)

- **Task management**:
  - Job retry (configurable max attempts)
  - Job status tracking (pending, processing, completed, failed)
  - Job priority
  - Job timeout
  - Auto recovery for timed-out processing jobs

- **Concurrency control**:
  - Independent concurrency per queue
  - Configurable max concurrency per queue

- **Performance optimizations**:
  - Redis MGET batch get for large job scenarios
  - MongoDB aggregation pipeline for priority fetching
  - Dynamic delay polling (short delay when jobs exist, increasing when empty)

- **Service container integration**:
  - createQueueManager factory function
  - QueueManager.fromContainer static method
  - Named manager support
  - @dreamer/service dependency injection

### Compatibility

- Deno 2.6+
- Bun 1.3.5+
- Redis (for Redis adapter)
- Memcached (for Memcached adapter)
- MongoDB (for MongoDB adapter)
- RabbitMQ (for RabbitMQ adapter)
