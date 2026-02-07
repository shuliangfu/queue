# @dreamer/queue

> A queue and task scheduling library compatible with Deno and Bun, providing task queues, scheduling, concurrency control, and more

English | [中文 (Chinese)](./README-zh.md)

[![JSR](https://jsr.io/badges/@dreamer/queue)](https://jsr.io/@dreamer/queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE.md)
[![Tests](https://img.shields.io/badge/tests-100%20passed-brightgreen)](./TEST_REPORT.md)

---

## 🎯 Features

Queue and task scheduling library for async task processing, cron jobs, batch processing, and more.

---

## 📦 Installation

```bash
deno add jsr:@dreamer/queue
```

---

## 🌍 Environment Compatibility

| Environment | Support | Notes                           |
| ----------- | ------- | ------------------------------- |
| Deno        | ✅      | Deno 2.6+                       |
| Bun         | ✅      | Bun 1.3.5+                      |
| Server      | ✅      | Compatible with Deno and Bun    |
| Client      | ❌      | Browser cannot run task queues  |

**Dependencies**:

- **Redis adapter**: Requires `npm:redis` or `npm:ioredis`
- **Memcached adapter**: Requires `npm:memcache-client`
- **RabbitMQ adapter**: Requires `npm:amqplib`
- **MongoDB adapter**: Requires `npm:mongodb`
- **Memory adapter**: No extra dependencies, dev/test only
- **Service container**: `jsr:@dreamer/service` (optional, for dependency injection)

---

## ✨ Characteristics

- **Multi-queue support** (task isolation, no cross-blocking):
  - Multiple independent queue instances
  - Each task type can use a separate queue
  - Queues are fully isolated
  - Each queue has its own processing loop and concurrency control
- **Task queues**:
  - FIFO queue (first-in first-out)
  - Priority queue (higher priority first)
  - Delayed queue (scheduled execution)
- **Task scheduling**:
  - Cron jobs (Cron expression support)
  - Delayed jobs (execute after specified time)
  - Recurring jobs (interval execution)
- **Concurrency control**:
  - Independent concurrency per queue
  - Configurable max concurrency per queue
- **Task management**:
  - Job retry (configurable retry count)
  - Job status tracking (pending, processing, completed, failed)
  - Job priority
  - Job timeout
- **Persistence** (recommended):
  - **Redis adapter**: Redis-based persistence (recommended, high performance)
  - **Memcached adapter**: Memcached in-memory cache
  - **RabbitMQ adapter**: RabbitMQ persistence (enterprise)
  - **MongoDB adapter**: MongoDB persistence
  - **Memory adapter**: Dev/test only
- **Service container integration**: Supports dependency injection and service container

---

## 🎯 Use Cases

- Async task processing (email sending, image processing, etc.)
- Cron jobs (data sync, report generation, etc.)
- Batch processing (bulk import, bulk export, etc.)
- Background job processing
- Parallel processing of multiple task types

---

## 🚀 Quick Start

### Create Queue Manager

**⚠️ Important**: Production must use a persistent adapter (Redis, Memcached, RabbitMQ, or MongoDB).

#### Using Redis Adapter (Recommended)

```typescript
import { QueueManager, RedisQueueAdapter } from "jsr:@dreamer/queue";
import { createClient } from "npm:redis";

// Create Redis client
const redisClient = createClient({ url: "redis://localhost:6379" });
await redisClient.connect();

// Create queue manager
const queueManager = new QueueManager({
  adapter: new RedisQueueAdapter({ client: redisClient }),
  autoRecover: true,
  recoverTimeout: 30000,
});
```

#### Using MongoDB Adapter

```typescript
import { MongoDBQueueAdapter, QueueManager } from "jsr:@dreamer/queue";

const adapter = new MongoDBQueueAdapter({
  connection: {
    url: "mongodb://localhost:27017",
    database: "queue",
  },
});
await adapter.connect();

const queueManager = new QueueManager({ adapter, autoRecover: true });
```

### Create and Use Queue

```typescript
// Create queue
const emailQueue = queueManager.createQueue("email", {
  concurrency: 5,
  retry: 3,
  timeout: 60000,
});

// Add job
await emailQueue.add("send-welcome", { userId: 123 });

// Process jobs
emailQueue.process(async (job) => {
  console.log("Processing email job:", job.data);
});
```

### Cron Jobs

```typescript
// Add cron job (runs daily at midnight)
queueManager.schedule("daily-report", "0 0 * * *", async (data) => {
  console.log("Running daily report");
});

// Remove cron job
queueManager.unschedule("daily-report");
```

---

## 📚 API Reference

### QueueManager

#### `new QueueManager(options)`

Create a queue manager.

| Parameter     | Type          | Default | Description                        |
| -------------- | ------------- | ------- | ---------------------------------- |
| adapter        | QueueAdapter  | -       | Queue adapter (required)           |
| autoRecover    | boolean       | true    | Auto recover incomplete jobs       |
| recoverTimeout | number        | 30000   | Timeout for recovery (ms)          |
| name           | string        | default | Manager name (for service container) |

#### Methods

| Method                                  | Returns           | Description       |
| --------------------------------------- | ----------------- | ----------------- |
| `createQueue(name, options?)`           | Queue             | Create queue      |
| `getQueue(name)`                        | Queue \| undefined | Get queue         |
| `schedule(name, cron, handler, opts?)`   | void              | Add cron job      |
| `unschedule(name)`                      | void              | Remove cron job   |
| `close()`                               | Promise\<void\>   | Close manager     |
| `getName()`                             | string            | Get manager name  |
| `setContainer(container)`               | this              | Set service container |
| `getContainer()`                        | ServiceContainer  | Get service container |
| `static fromContainer(container, name)` | QueueManager      | Get manager from container |

### Queue

#### `add(name, data, options?)`

Add job to queue.

| Parameter   | Type        | Description                       |
| ----------- | ----------- | --------------------------------- |
| name        | string      | Job name                          |
| data        | JobData     | Job data                          |
| priority    | JobPriority | Priority (low/normal/high/urgent)  |
| delay       | number      | Delay before execution (ms)       |
| maxAttempts | number      | Max retry count                   |
| timeout     | number      | Timeout (ms)                      |

#### Methods

| Method                 | Returns           | Description     |
| ---------------------- | ----------------- | --------------- |
| `process(processor)`   | void              | Process jobs    |
| `getJob(jobId)`        | Promise\<Job\>    | Get job         |
| `getJobs()`            | Promise\<Job[]\>  | Get all jobs    |
| `getStats()`           | Promise\<Stats\>  | Get statistics  |
| `clear()`              | Promise\<void\>   | Clear queue     |
| `stop()`               | void              | Stop processing |

### createQueueManager Factory

```typescript
import { createQueueManager } from "jsr:@dreamer/queue";
import { ServiceContainer } from "jsr:@dreamer/service";

const container = new ServiceContainer();
const queueManager = createQueueManager({ adapter, name: "main" }, container);

// Get from container
const manager = QueueManager.fromContainer(container, "main");
```

### Type Definitions

```typescript
// Job status
type JobStatus = "pending" | "processing" | "completed" | "failed";

// Job priority
type JobPriority = "low" | "normal" | "high" | "urgent";

// Job interface
interface Job {
  id: string;
  name: string;
  data: JobData;
  status: JobStatus;
  priority: JobPriority;
  createdAt: number;
  attempts: number;
  maxAttempts: number;
  error?: string;
  delay?: number;
  timeout?: number;
}
```

---

## 🔧 Advanced Configuration

### Adapter Configuration

#### Redis Adapter

```typescript
const adapter = new RedisQueueAdapter({
  connection: {
    url: "redis://localhost:6379",
    password: "your-password",
    db: 0,
  },
  keyPrefix: "queue",
});
```

#### RabbitMQ Adapter

```typescript
const adapter = new RabbitMQQueueAdapter({
  connection: {
    url: "amqp://guest:guest@localhost:5672",
  },
  queueOptions: { durable: true },
});
```

#### Memcached Adapter

```typescript
const adapter = new MemcachedQueueAdapter({
  connection: {
    host: "127.0.0.1",
    port: 11211,
    timeout: 5000,
  },
});
```

### Cron Expressions

Cron jobs use UTC. Supports standard Cron expressions:

| Expression    | Description       |
| ------------- | ----------------- |
| `* * * * *`   | Every minute      |
| `0 * * * *`   | Every hour        |
| `0 0 * * *`   | Daily             |
| `*/5 * * * *` | Every 5 minutes    |
| `*/30 * * * * *` | Every 30 seconds |

### Multi-Queue Isolation

The library supports multiple independent queue instances, each fully isolated:

1. **Independent processing loops**: Each queue has its own `processLoop()` async loop
2. **Task isolation**: Each queue only processes its own jobs
3. **Independent concurrency**: Each queue has its own concurrency limit
4. **Blocking isolation**: Slow jobs in one queue do not affect others

---

## 📋 Changelog

**v1.0.0** (2026-02-07) - First stable release with multi-queue support, adapters (Redis, MongoDB, RabbitMQ, Memcached, Memory), cron jobs, and service container integration.

See [CHANGELOG.md](./CHANGELOG.md) for full details.

---

## 📊 Test Report

| Item       | Result      |
| ---------- | ----------- |
| Total tests| 100         |
| Passed     | 100 ✅      |
| Failed     | 0           |
| Pass rate  | 100%        |
| Test date  | 2026-01-30  |

See [TEST_REPORT.md](./TEST_REPORT.md) for details.

---

## 📝 Notes

- **Persistent adapter**: Production must use a persistent adapter (Redis, Memcached, RabbitMQ, or MongoDB)
- **Memory adapter**: Dev/test only; jobs are lost on app restart
- **Memcached**: Data lost on service restart; use Redis or MongoDB for true persistence
- **Concurrency**: Each queue has independent concurrency control
- **Retry**: Jobs retry automatically until max attempts
- **Timeout**: Jobs are marked failed on execution timeout
- **Cron**: Uses UTC; supports 5-field and 6-field Cron expressions

---

## 🤝 Contributing

Issues and Pull Requests welcome!

---

## 📄 License

MIT License - see [LICENSE.md](./LICENSE.md)

---

<div align="center">

**Made with ❤️ by Dreamer Team**

</div>
