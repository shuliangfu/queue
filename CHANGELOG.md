# Changelog

All notable changes to @dreamer/queue are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
