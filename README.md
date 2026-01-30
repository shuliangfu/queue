# @dreamer/queue

> 一个兼容 Deno 和 Bun 的队列和任务调度库，提供任务队列、任务调度、并发控制等功能

[![JSR](https://jsr.io/badges/@dreamer/queue)](https://jsr.io/@dreamer/queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE.md)
[![Tests](https://img.shields.io/badge/tests-100%20passed-brightgreen)](./TEST_REPORT.md)

---

## 🎯 功能

队列和任务调度库，用于异步任务处理、定时任务、批量处理等。

---

## 📦 安装

```bash
deno add jsr:@dreamer/queue
```

---

## 🌍 环境兼容性

| 环境       | 支持情况 | 说明                              |
| ---------- | -------- | --------------------------------- |
| Deno       | ✅       | Deno 2.6+                         |
| Bun        | ✅       | Bun 1.3.5+                        |
| 服务端     | ✅       | 兼容 Deno 和 Bun 运行时           |
| 客户端     | ❌       | 浏览器环境无法运行任务队列        |

**依赖说明**：

- **Redis 适配器**：需要 `npm:redis` 或 `npm:ioredis`
- **Memcached 适配器**：需要 `npm:memcache-client`
- **RabbitMQ 适配器**：需要 `npm:amqplib`
- **MongoDB 适配器**：需要 `npm:mongodb`
- **内存适配器**：无需额外依赖，仅用于开发和测试
- **服务容器**：`jsr:@dreamer/service`（可选，用于依赖注入）

---

## ✨ 特性

- **多队列支持**（任务隔离，不会互相阻塞）：
  - 支持多个独立的队列实例
  - 每个任务类型可以使用独立的队列
  - 不同队列之间完全隔离，互不影响
  - 每个队列有独立的处理循环和并发控制
- **任务队列**：
  - FIFO 队列（先进先出）
  - 优先级队列（高优先级任务优先执行）
  - 延迟队列（延迟执行）
- **任务调度**：
  - 定时任务（Cron 表达式支持）
  - 延迟任务（指定时间后执行）
  - 周期性任务（间隔执行）
- **并发控制**：
  - 每个队列独立的并发控制
  - 可配置每个队列的最大并发数
- **任务管理**：
  - 任务重试（可配置重试次数）
  - 任务状态追踪（pending、processing、completed、failed）
  - 任务优先级设置
  - 任务超时控制
- **持久化支持**（推荐使用）：
  - **Redis 适配器**：基于 Redis 的持久化存储（推荐，高性能）
  - **Memcached 适配器**：基于 Memcached 的内存缓存存储
  - **RabbitMQ 适配器**：基于 RabbitMQ 的持久化存储（企业级）
  - **MongoDB 适配器**：基于 MongoDB 的持久化存储
  - **内存适配器**：仅用于开发和测试
- **服务容器集成**：支持依赖注入和服务容器管理

---

## 🎯 使用场景

- 异步任务处理（邮件发送、图片处理等）
- 定时任务（数据同步、报表生成等）
- 批量处理（批量导入、批量导出等）
- 后台任务处理
- 多任务类型并行处理

---

## 🚀 快速开始

### 创建队列管理器

**⚠️ 重要**：生产环境必须使用持久化适配器（Redis、Memcached、RabbitMQ 或 MongoDB）。

#### 使用 Redis 适配器（推荐）

```typescript
import { QueueManager, RedisQueueAdapter } from "jsr:@dreamer/queue";
import { createClient } from "npm:redis";

// 创建 Redis 客户端
const redisClient = createClient({ url: "redis://localhost:6379" });
await redisClient.connect();

// 创建队列管理器
const queueManager = new QueueManager({
  adapter: new RedisQueueAdapter({ client: redisClient }),
  autoRecover: true,
  recoverTimeout: 30000,
});
```

#### 使用 MongoDB 适配器

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

### 创建和使用队列

```typescript
// 创建队列
const emailQueue = queueManager.createQueue("email", {
  concurrency: 5,
  retry: 3,
  timeout: 60000,
});

// 添加任务
await emailQueue.add("send-welcome", { userId: 123 });

// 处理任务
emailQueue.process(async (job) => {
  console.log("处理邮件任务:", job.data);
});
```

### 定时任务

```typescript
// 添加定时任务（每天凌晨执行）
queueManager.schedule("daily-report", "0 0 * * *", async (data) => {
  console.log("执行每日报表生成");
});

// 移除定时任务
queueManager.unschedule("daily-report");
```

---

## 📚 API 文档

### QueueManager

#### `new QueueManager(options)`

创建队列管理器。

| 参数            | 类型           | 默认值  | 说明                         |
| --------------- | -------------- | ------- | ---------------------------- |
| adapter         | QueueAdapter   | -       | 队列适配器（必需）           |
| autoRecover     | boolean        | true    | 是否自动恢复未完成的任务     |
| recoverTimeout  | number         | 30000   | 恢复超时任务的时间（毫秒）   |
| name            | string         | default | 管理器名称（用于服务容器）   |

#### 方法

| 方法                                    | 返回值                | 说明                       |
| --------------------------------------- | --------------------- | -------------------------- |
| `createQueue(name, options?)`           | Queue                 | 创建队列                   |
| `getQueue(name)`                        | Queue \| undefined    | 获取队列                   |
| `schedule(name, cron, handler, opts?)`  | void                  | 添加定时任务               |
| `unschedule(name)`                      | void                  | 移除定时任务               |
| `close()`                               | Promise\<void\>       | 关闭管理器                 |
| `getName()`                             | string                | 获取管理器名称             |
| `setContainer(container)`               | this                  | 设置服务容器               |
| `getContainer()`                        | ServiceContainer      | 获取服务容器               |
| `static fromContainer(container, name)` | QueueManager          | 从容器获取管理器           |

### Queue

#### `add(name, data, options?)`

添加任务到队列。

| 参数        | 类型        | 说明                                     |
| ----------- | ----------- | ---------------------------------------- |
| name        | string      | 任务名称                                 |
| data        | JobData     | 任务数据                                 |
| priority    | JobPriority | 优先级（low/normal/high/urgent）         |
| delay       | number      | 延迟执行时间（毫秒）                     |
| maxAttempts | number      | 最大重试次数                             |
| timeout     | number      | 超时时间（毫秒）                         |

#### 方法

| 方法                 | 返回值             | 说明             |
| -------------------- | ------------------ | ---------------- |
| `process(processor)` | void               | 处理任务         |
| `getJob(jobId)`      | Promise\<Job\>     | 获取任务         |
| `getJobs()`          | Promise\<Job[]\>   | 获取所有任务     |
| `getStats()`         | Promise\<Stats\>   | 获取统计信息     |
| `clear()`            | Promise\<void\>    | 清空队列         |
| `stop()`             | void               | 停止处理任务     |

### createQueueManager 工厂函数

```typescript
import { createQueueManager } from "jsr:@dreamer/queue";
import { ServiceContainer } from "jsr:@dreamer/service";

const container = new ServiceContainer();
const queueManager = createQueueManager({ adapter, name: "main" }, container);

// 从容器获取
const manager = QueueManager.fromContainer(container, "main");
```

### 类型定义

```typescript
// 任务状态
type JobStatus = "pending" | "processing" | "completed" | "failed";

// 任务优先级
type JobPriority = "low" | "normal" | "high" | "urgent";

// 任务接口
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

## 🔧 高级配置

### 适配器配置

#### Redis 适配器

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

#### RabbitMQ 适配器

```typescript
const adapter = new RabbitMQQueueAdapter({
  connection: {
    url: "amqp://guest:guest@localhost:5672",
  },
  queueOptions: { durable: true },
});
```

#### Memcached 适配器

```typescript
const adapter = new MemcachedQueueAdapter({
  connection: {
    host: "127.0.0.1",
    port: 11211,
    timeout: 5000,
  },
});
```

### Cron 表达式

定时任务使用 UTC 时区，支持标准的 Cron 表达式：

| 表达式            | 说明             |
| ----------------- | ---------------- |
| `* * * * *`       | 每分钟执行       |
| `0 * * * *`       | 每小时执行       |
| `0 0 * * *`       | 每天执行         |
| `*/5 * * * *`     | 每5分钟执行      |
| `*/30 * * * * *`  | 每30秒执行       |

### 多队列隔离

队列库支持多个独立的队列实例，每个队列完全隔离，互不阻塞：

1. **独立的处理循环**：每个队列有独立的 `processLoop()` 异步循环
2. **任务隔离**：每个队列只处理自己队列中的任务
3. **独立的并发控制**：每个队列有独立的并发限制
4. **阻塞隔离**：一个队列的慢任务不会影响其他队列

---

## 📊 测试报告

| 项目     | 结果       |
| -------- | ---------- |
| 测试总数 | 100        |
| 通过     | 100 ✅     |
| 失败     | 0          |
| 通过率   | 100%       |
| 测试时间 | 2026-01-30 |

详细测试报告请查看 [TEST_REPORT.md](./TEST_REPORT.md)。

---

## 📝 注意事项

- **持久化适配器**：生产环境必须使用持久化适配器（Redis、Memcached、RabbitMQ 或 MongoDB）
- **内存适配器**：仅用于开发和测试，应用重启后任务会丢失
- **Memcached 注意**：服务重启后数据会丢失，如需真正持久化请使用 Redis 或 MongoDB
- **并发控制**：每个队列独立的并发控制，互不影响
- **任务重试**：任务失败后会自动重试，直到达到最大重试次数
- **任务超时**：任务执行超时后会被标记为失败
- **定时任务**：使用 UTC 时区，支持 5 字段和 6 字段 Cron 表达式

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

## 📄 许可证

MIT License - 详见 [LICENSE.md](./LICENSE.md)

---

<div align="center">

**Made with ❤️ by Dreamer Team**

</div>
