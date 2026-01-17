# @dreamer/queue

> 一个兼容 Deno 和 Bun 的队列和任务调度库，提供任务队列、任务调度、并发控制等功能

[![JSR](https://jsr.io/badges/@dreamer/queue)](https://jsr.io/@dreamer/queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 🎯 功能

队列和任务调度库，用于异步任务处理、定时任务、批量处理等。

## 特性

- **多队列支持**（任务隔离，不会互相阻塞）：
  - 支持多个独立的队列实例
  - 每个任务类型可以使用独立的队列
  - 不同队列之间完全隔离，互不影响
  - 每个队列有独立的处理循环和并发控制
  - 一个队列的慢任务不会阻塞其他队列
  - 可以为不同任务类型配置不同的并发数
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
- **任务隔离**：
  - 不同任务类型使用不同队列，互不阻塞
  - 一个任务类型的阻塞不会影响其他任务类型
- **持久化支持**（推荐使用）：
  - **Redis 适配器**：基于 Redis 的持久化存储（推荐，高性能）
  - **Memcached 适配器**：基于 Memcached 的内存缓存存储（高性能，只要服务不重启数据不丢失）
  - **RabbitMQ 适配器**：基于 RabbitMQ 的持久化存储（企业级，支持高级特性）
  - **MongoDB 适配器**：基于 MongoDB 的持久化存储（文档数据库，适合复杂查询）
  - **内存适配器**：仅用于开发和测试，不支持持久化
  - **故障恢复**：自动恢复超时的处理中任务
  - ⚠️ **重要**：生产环境必须使用持久化适配器，应用重启后任务不会丢失

## 使用场景

- 异步任务处理（邮件发送、图片处理等）
- 定时任务（数据同步、报表生成等）
- 批量处理（批量导入、批量导出等）
- 后台任务处理
- 多任务类型并行处理

## 安装

```bash
deno add jsr:@dreamer/queue
```

## 环境兼容性

- **运行时要求**：Deno 2.6+ 或 Bun 1.3.5
- **服务端**：✅ 支持（兼容 Deno 和 Bun 运行时）
- **客户端**：❌ 不支持（浏览器环境无法运行任务队列）
- **依赖**：根据使用的适配器需要相应的客户端库
  - **Redis 适配器**：需要 Redis 客户端库（如 `npm:redis` 或 `npm:ioredis`）
  - **Memcached 适配器**：需要 Memcached 客户端库（如 `npm:memcache-client`）
  - **RabbitMQ 适配器**：需要 RabbitMQ 客户端库（如 `npm:amqplib`）
  - **MongoDB 适配器**：需要 MongoDB 客户端库（如 `npm:mongodb`）
  - **内存适配器**：无需额外依赖，但仅用于开发和测试
  - ⚠️ **重要**：生产环境必须使用持久化适配器，内存适配器不支持持久化（会丢失数据）

## 基本使用

### 创建队列管理器

**⚠️ 重要**：生产环境必须使用持久化适配器（Redis、Memcached、RabbitMQ 或 MongoDB）。内存适配器仅用于开发和测试。

#### 使用 Redis 适配器（推荐）

**方式1：使用已创建的 Redis 客户端**

```typescript
import { QueueManager, RedisQueueAdapter } from "jsr:@dreamer/queue";
import { createClient } from "npm:redis";

// 创建 Redis 客户端
const redisClient = createClient({
  url: "redis://localhost:6379",
  // password: "your-password",
});

await redisClient.connect();

// 创建队列管理器（使用 Redis 持久化）
const queueManager = new QueueManager({
  adapter: new RedisQueueAdapter({
    client: redisClient,
    // 可选：自定义键前缀（默认：queue）
    // keyPrefix: "queue",
  }),
  autoRecover: true, // 自动恢复未完成的任务
  recoverTimeout: 30000, // 30秒后恢复超时任务
});
```

**方式2：使用连接配置（适配器内部创建连接）**

```typescript
import { QueueManager, RedisQueueAdapter } from "jsr:@dreamer/queue";

// 创建适配器（使用连接配置）
const adapter = new RedisQueueAdapter({
  connection: {
    url: "redis://localhost:6379",
    // 或者使用详细配置：
    // host: "127.0.0.1",
    // port: 6379,
    // password: "your-password",
    // db: 0,
    // socket: {
    //   keepAlive: false,
    //   connectTimeout: 5000,
    // },
  },
  // 可选：自定义键前缀（默认：queue）
  // keyPrefix: "queue",
});
await adapter.connect();

const queueManager = new QueueManager({
  adapter,
  autoRecover: true,
  recoverTimeout: 30000,
});
```

#### 使用 RabbitMQ 适配器

**方式1：使用已创建的 RabbitMQ 连接**

```typescript
import { QueueManager, RabbitMQQueueAdapter } from "jsr:@dreamer/queue";
import amqp from "npm:amqplib";

// 连接 RabbitMQ
const connection = await amqp.connect("amqp://localhost");

// 创建队列管理器（使用 RabbitMQ 持久化）
const queueManager = new QueueManager({
  adapter: new RabbitMQQueueAdapter({
    connectionObject: connection,
    // 队列选项
    queueOptions: {
      durable: true, // 启用持久化
    },
  }),
  autoRecover: true,
  recoverTimeout: 30000,
});
```

**方式2：使用连接配置（适配器内部创建连接）**

```typescript
import { QueueManager, RabbitMQQueueAdapter } from "jsr:@dreamer/queue";

// 创建适配器（使用连接配置）
const adapter = new RabbitMQQueueAdapter({
  connection: {
    url: "amqp://guest:guest@localhost:5672",
    // 或者使用详细配置：
    // hostname: "127.0.0.1",
    // port: 5672,
    // username: "guest",
    // password: "guest",
    // vhost: "/",
  },
  // 队列选项
  queueOptions: {
    durable: true, // 启用持久化
  },
});
await adapter.connect();

const queueManager = new QueueManager({
  adapter,
  autoRecover: true,
  recoverTimeout: 30000,
});
```

#### 使用 Memcached 适配器

**方式1：使用连接配置（推荐）**

```typescript
import { QueueManager, MemcachedQueueAdapter } from "jsr:@dreamer/queue";

const adapter = new MemcachedQueueAdapter({
  connection: {
    host: "127.0.0.1",
    port: 11211,
    timeout: 5000,
    compress: false,
    maxConnections: 10,
  },
});

await adapter.connect();

// 创建队列管理器（使用 Memcached 持久化）
const queueManager = new QueueManager({
  adapter,
  autoRecover: true,
  recoverTimeout: 30000,
});
```

**方式2：使用已创建的客户端**

```typescript
import { QueueManager, MemcachedQueueAdapter } from "jsr:@dreamer/queue";
import { MemcacheClient } from "npm:memcache-client";

// 创建 Memcached 客户端
const memcachedClient = new MemcacheClient({
  server: "127.0.0.1:11211",
});

// 创建队列管理器（使用 Memcached 持久化）
const queueManager = new QueueManager({
  adapter: new MemcachedQueueAdapter({ client: memcachedClient }),
  autoRecover: true,
  recoverTimeout: 30000,
});
```

**Memcached 适配器说明**：
- Memcached 是内存缓存系统，数据存储在内存中
- 只要 Memcached 服务不重启，数据不会丢失
- 但服务重启后数据会丢失，如果需要真正的持久化（服务重启后数据不丢失），请使用 Redis 或 MongoDB 适配器
- Memcached 适配器性能高，适合单机或小规模分布式场景
- 支持批量获取优化（getMulti），提高性能

#### 使用 MongoDB 适配器

```typescript
import { QueueManager, MongoDBQueueAdapter } from "jsr:@dreamer/queue";

// 创建队列管理器（使用 MongoDB 持久化）
const adapter = new MongoDBQueueAdapter({
  connection: {
    url: "mongodb://localhost:27017",
    database: "queue",
    // 可选配置：
    // host: "127.0.0.1",
    // port: 27017,
    // username: "user",
    // password: "password",
    // authSource: "admin",
    // options: {
    //   connectTimeoutMS: 5000,
    //   socketTimeoutMS: 0,
    //   maxPoolSize: 10,
    //   minPoolSize: 1,
    // },
  },
  // 可选：自定义集合名称（默认：queues）
  // collectionPrefix: "queues",
  // 可选：自定义数据库名称（默认：queue）
  // databaseName: "queue",
});
await adapter.connect();

const queueManager = new QueueManager({
  adapter,
  autoRecover: true,
  recoverTimeout: 30000,
});
```

**MongoDB 适配器说明**：
- 所有队列的任务都存储在同一个集合中（默认：`queues`），通过 `queueName` 字段区分不同队列
- 这种设计简化了表管理，避免了为每个队列创建单独的表
- 适配器会自动创建索引以优化查询性能

### 创建队列

```typescript
// 创建不同类型的队列（互不阻塞）
const emailQueue = queueManager.createQueue("email", {
  concurrency: 5, // 邮件队列最多5个并发
  retry: 3, // 失败重试3次
  timeout: 60000, // 任务超时时间（60秒）
});

const imageQueue = queueManager.createQueue("image", {
  concurrency: 3, // 图片处理队列最多3个并发
  retry: 2,
  priority: true, // 支持优先级
});

const reportQueue = queueManager.createQueue("report", {
  concurrency: 2, // 报表生成队列最多2个并发
  retry: 1,
});
```

### 添加和处理任务

```typescript
// 添加任务到队列
await emailQueue.add("send-welcome", { userId: 123 });
await imageQueue.add("resize-image", { imageId: 456 });
await reportQueue.add("generate-report", { reportId: 789 }, {
  priority: "high", // 高优先级
});

// 处理任务（每个队列独立处理，互不影响）
emailQueue.process(async (job) => {
  console.log("处理邮件任务:", job.data);
  // 发送邮件逻辑
  // 如果抛出错误，会自动重试
});

imageQueue.process(async (job) => {
  console.log("处理图片任务:", job.data);
  // 图片处理逻辑
  // 独立运行，不受其他队列影响
});

reportQueue.process(async (job) => {
  console.log("生成报表:", job.data);
  // 报表生成逻辑
});
```

### 延迟任务

```typescript
// 添加延迟任务（1小时后执行）
await emailQueue.add("send-reminder", { userId: 123 }, {
  delay: 3600000, // 1小时后执行（毫秒）
});
```

### 定时任务

定时任务使用 `@dreamer/runtime-adapter` 的 cron API 实现，支持标准的 Cron 表达式，兼容 Deno 和 Bun 环境。

**重要提示**：
- 定时任务使用 **UTC 时区** 来指定计划时间，以避免与夏令时相关的问题
- 支持标准的 5 字段格式（分钟 小时 日 月 星期）和 6 字段格式（秒 分钟 小时 日 月 星期）

```typescript
// 添加定时任务（每天凌晨执行）
queueManager.schedule("daily-report", "0 0 * * *", async (data) => {
  console.log("执行每日报表生成");
  // 报表生成逻辑
}, {
  queueName: "report", // 可选：指定目标队列
  data: { type: "daily" }, // 可选：任务数据
});

// 或者添加到队列中执行
queueManager.schedule("cleanup", "0 2 * * *", undefined, {
  queueName: "maintenance",
  data: { action: "cleanup" },
});

// 移除定时任务
queueManager.unschedule("daily-report");
```

### 任务管理

```typescript
// 获取任务
const job = await emailQueue.getJob("job-id");
console.log("任务状态:", job?.status);

// 获取所有任务
const jobs = await emailQueue.getJobs();
console.log("队列中的任务数:", jobs.length);

// 获取统计信息
const stats = await emailQueue.getStats();
console.log("待处理:", stats.pending);
console.log("处理中:", stats.processing);
console.log("已完成:", stats.completed);
console.log("失败:", stats.failed);

// 清空队列
await emailQueue.clear();
```

## 高级功能

### 任务优先级

```typescript
// 添加高优先级任务
await emailQueue.add("urgent-email", { userId: 123 }, {
  priority: "urgent", // "low" | "normal" | "high" | "urgent"
});

// 添加普通优先级任务
await emailQueue.add("normal-email", { userId: 456 }, {
  priority: "normal",
});
```

### 任务重试

```typescript
// 添加任务时指定重试次数
await emailQueue.add("send-email", { userId: 123 }, {
  maxAttempts: 5, // 最多重试5次
});

// 或者在创建队列时设置默认重试次数
const queue = queueManager.createQueue("email", {
  retry: 3, // 默认重试3次
});
```

### 任务超时

```typescript
// 添加任务时指定超时时间
await emailQueue.add("long-task", { data: "..." }, {
  timeout: 300000, // 5分钟超时
});

// 或者在创建队列时设置默认超时时间
const queue = queueManager.createQueue("email", {
  timeout: 60000, // 默认60秒超时
});
```

### 任务状态追踪

```typescript
emailQueue.process(async (job) => {
  console.log("任务ID:", job.id);
  console.log("任务名称:", job.name);
  console.log("任务数据:", job.data);
  console.log("任务状态:", job.status);
  console.log("重试次数:", job.attempts);
  console.log("创建时间:", new Date(job.createdAt));

  // 处理任务...
});
```

## 持久化适配器

### 内存适配器

内存适配器无需任何依赖，适合开发和测试环境。**注意**：内存适配器不支持持久化，应用重启后任务会丢失。

```typescript
import { QueueManager, MemoryQueueAdapter } from "jsr:@dreamer/queue";

const queueManager = new QueueManager({
  adapter: new MemoryQueueAdapter(), // 显式使用内存适配器
});
```

### 适配器配置选项

#### Redis 适配器配置

```typescript
interface RedisAdapterOptions {
  // Redis 连接配置（如果提供，适配器会内部创建连接）
  connection?: {
    url?: string;                    // Redis 连接 URL
    host?: string;                   // 主机地址（默认：127.0.0.1）
    port?: number;                   // 端口（默认：6379）
    password?: string;               // 密码（可选）
    db?: number;                     // 数据库编号（默认：0）
    socket?: {
      keepAlive?: boolean;           // 是否启用 keepAlive（默认：false）
      connectTimeout?: number;       // 连接超时时间（毫秒，默认：5000）
    };
  };
  // Redis 客户端实例（如果提供 connection，则不需要提供 client）
  client?: RedisClient;
  // 键前缀（可选，默认：queue）
  keyPrefix?: string;
}
```

#### RabbitMQ 适配器配置

```typescript
interface RabbitMQAdapterOptions {
  // RabbitMQ 连接配置（如果提供，适配器会内部创建连接）
  connection?: {
    url?: string;                    // RabbitMQ 连接 URL
    hostname?: string;               // 主机地址（默认：127.0.0.1）
    port?: number;                   // 端口（默认：5672）
    username?: string;               // 用户名（默认：guest）
    password?: string;               // 密码（默认：guest）
    vhost?: string;                  // 虚拟主机（默认：/）
  };
  // RabbitMQ 连接对象（如果提供 connection，则不需要提供此参数）
  connectionObject?: Connection;
  // 队列选项
  queueOptions?: {
    durable?: boolean;               // 是否持久化（默认：false）
  };
}
```

#### MongoDB 适配器配置

```typescript
interface MongoDBAdapterOptions {
  // MongoDB 连接配置（如果提供，适配器会内部创建连接）
  connection?: {
    url?: string;                    // MongoDB 连接 URL
    host?: string;                   // 主机地址（默认：127.0.0.1）
    port?: number;                   // 端口（默认：27017）
    database?: string;               // 数据库名称（默认：queue）
    username?: string;               // 用户名（可选）
    password?: string;               // 密码（可选）
    authSource?: string;             // 认证数据库（可选，默认：admin）
    options?: {
      connectTimeoutMS?: number;    // 连接超时时间（毫秒，默认：5000）
      socketTimeoutMS?: number;     // Socket 超时时间（毫秒，默认：0）
      maxPoolSize?: number;         // 最大连接池大小（默认：10）
      minPoolSize?: number;         // 最小连接池大小（默认：1）
    };
  };
  // MongoDB 客户端实例（如果提供 connection，则不需要提供 client）
  client?: MongoClient;
  // 集合名称（可选，默认：queues）
  // 所有队列的任务都存储在同一个集合中，通过 queueName 字段区分
  collectionPrefix?: string;
  // 数据库名称（可选，默认：queue）
  databaseName?: string;
}
```

### 自定义适配器

实现 `QueueAdapter` 接口以支持 Redis、RabbitMQ 等持久化存储：

```typescript
import { QueueAdapter, Job } from "jsr:@dreamer/queue";

class RedisQueueAdapter implements QueueAdapter {
  // 实现适配器接口
  async add(job: Job): Promise<void> {
    // 存储到 Redis
  }

  async getNext(queueName: string): Promise<Job | null> {
    // 从 Redis 获取下一个任务
  }

  // ... 其他方法
}

// 使用自定义适配器
const queueManager = new QueueManager({
  adapter: new RedisQueueAdapter(),
});
```

## Cron 表达式

定时任务使用 `@dreamer/runtime-adapter` 的 cron API，支持标准的 Cron 表达式格式：

- `* * * * *` - 每分钟执行（5 字段格式）
- `0 * * * *` - 每小时执行
- `0 0 * * *` - 每天执行
- `0 0 1 * *` - 每月1号执行
- `0 0 * * 0` - 每周日执行
- `0 0-12 * * *` - 每天0点到12点每小时执行
- `*/5 * * * *` - 每5分钟执行
- `0 0 1,15 * *` - 每月1号和15号执行
- `*/30 * * * * *` - 每30秒执行（6 字段格式，秒 分钟 小时 日 月 星期）

**重要提示**：
- 定时任务使用 **UTC 时区** 来指定计划时间，以避免与夏令时相关的问题
- 支持 5 字段格式：`分钟 小时 日 月 星期`
- 支持 6 字段格式：`秒 分钟 小时 日 月 星期`

## API 文档

### QueueManager

#### `new QueueManager(options?)`

创建队列管理器。

**参数**：
- `options`: `QueueManagerOptions` - 管理器选项
  - `adapter`: `QueueAdapter` - 队列适配器（可选，默认使用内存适配器）
  - `autoRecover`: `boolean` - 是否自动恢复未完成的任务（默认：true）
  - `recoverTimeout`: `number` - 恢复超时任务的时间（毫秒，默认：30000）

#### `createQueue(name, options?)`

创建队列。

**参数**：
- `name`: `string` - 队列名称
- `options`: `Partial<QueueOptions>` - 队列选项
  - `concurrency`: `number` - 最大并发数（默认：1）
  - `priority`: `boolean` - 是否支持优先级（默认：false）
  - `retry`: `number` - 默认重试次数（默认：0）
  - `timeout`: `number` - 任务超时时间（毫秒，可选）

**返回**：`Queue` - 队列实例

#### `getQueue(name)`

获取队列。

**参数**：
- `name`: `string` - 队列名称

**返回**：`Queue | undefined` - 队列实例

#### `schedule(name, cron, handler?, options?)`

添加定时任务。

**参数**：
- `name`: `string` - 任务名称
- `cron`: `string` - Cron 表达式
- `handler`: `ScheduledTaskHandler` - 任务处理器（可选）
- `options`: `object` - 选项
  - `queueName`: `string` - 目标队列名称（可选）
  - `data`: `JobData` - 任务数据（可选）

#### `unschedule(name)`

移除定时任务。

**参数**：
- `name`: `string` - 任务名称

#### `close()`

关闭管理器。

### Queue

#### `add(name, data, options?)`

添加任务。

**参数**：
- `name`: `string` - 任务名称
- `data`: `JobData` - 任务数据
- `options`: `AddJobOptions` - 选项
  - `priority`: `JobPriority` - 优先级
  - `delay`: `number` - 延迟执行时间（毫秒）
  - `maxAttempts`: `number` - 最大重试次数
  - `timeout`: `number` - 超时时间（毫秒）

**返回**：`Promise<Job>` - 任务对象

#### `process(processor)`

处理任务。

**参数**：
- `processor`: `JobProcessor` - 任务处理函数

#### `getJob(jobId)`

获取任务。

**参数**：
- `jobId`: `string` - 任务 ID

**返回**：`Promise<Job | null>` - 任务对象

#### `getJobs()`

获取所有任务。

**返回**：`Promise<Job[]>` - 任务列表

#### `getStats()`

获取统计信息。

**返回**：`Promise<{ pending, processing, completed, failed }>` - 统计信息

#### `clear()`

清空队列。

#### `stop()`

停止处理任务。

## 类型定义

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
  startedAt?: number;
  completedAt?: number;
  failedAt?: number;
  attempts: number;
  maxAttempts: number;
  error?: string;
  delay?: number;
  timeout?: number;
}

// 任务处理函数
type JobProcessor<T extends JobData = JobData> = (
  job: Job & { data: T }
) => Promise<void>;

// 队列适配器接口
interface QueueAdapter {
  add(job: Job): Promise<void>;
  getNext(queueName: string): Promise<Job | null>;
  update(jobId: string, updates: Partial<Job>): Promise<void>;
  get(jobId: string): Promise<Job | null>;
  remove(jobId: string): Promise<void>;
  getAll(queueName: string): Promise<Job[]>;
  clear(queueName: string): Promise<void>;
  getStats(queueName: string): Promise<{
    pending: number;
    processing: number;
    completed: number;
    failed: number;
  }>;
}
```

---

## 📚 API 文档

```typescript
import { QueueManager, RedisQueueAdapter } from "jsr:@dreamer/queue";
import { createClient } from "npm:redis";

// 创建 Redis 客户端
const redisClient = createClient({
  url: "redis://localhost:6379",
});

await redisClient.connect();

// 创建队列管理器（必须使用持久化适配器）
const queueManager = new QueueManager({
  adapter: new RedisQueueAdapter({ client: redisClient }),
  autoRecover: true,
  recoverTimeout: 30000,
});

// 创建多个独立的队列（互不阻塞）
const emailQueue = queueManager.createQueue("email", {
  concurrency: 5, // 邮件队列最多5个并发
  retry: 3,
});

const imageQueue = queueManager.createQueue("image", {
  concurrency: 3, // 图片处理队列最多3个并发
  retry: 2,
  priority: true,
});

const reportQueue = queueManager.createQueue("report", {
  concurrency: 2, // 报表生成队列最多2个并发
  retry: 1,
});

// 处理邮件任务（快速任务，不会阻塞其他队列）
emailQueue.process(async (job) => {
  console.log(`发送邮件给用户 ${job.data.userId}`);
  // 发送邮件逻辑（快速任务）
  // 如果失败，会自动重试
});

// 处理图片任务（慢速任务，但不会阻塞邮件队列）
imageQueue.process(async (job) => {
  console.log(`处理图片 ${job.data.imageId}`);
  // 图片处理逻辑（可能较慢）
  // 独立运行，不受其他队列影响
});

// 处理报表任务（非常慢的任务，但不会阻塞其他队列）
reportQueue.process(async (job) => {
  console.log(`生成报表 ${job.data.reportId}`);
  // 报表生成逻辑（可能很慢）
  // 完全独立，不影响邮件和图片队列
});

// 添加任务到不同队列
await emailQueue.add("send-welcome", { userId: 123 });
await emailQueue.add("send-reminder", { userId: 456 }, {
  delay: 3600000, // 1小时后执行
});

await imageQueue.add("resize-image", { imageId: 789 }, {
  priority: "high",
});

await reportQueue.add("generate-report", { reportId: 1 });

// 定时任务
queueManager.schedule("daily-cleanup", "0 2 * * *", async () => {
  console.log("执行每日清理");
  // 清理逻辑
});

// 获取统计信息
const emailStats = await emailQueue.getStats();
const imageStats = await imageQueue.getStats();
const reportStats = await reportQueue.getStats();

console.log("邮件队列统计:", emailStats);
console.log("图片队列统计:", imageStats);
console.log("报表队列统计:", reportStats);

// 关闭管理器（应用退出时）
await queueManager.close();
```

## 多队列隔离说明

队列库支持多个独立的队列实例，每个队列完全隔离，互不阻塞：

1. **独立的处理循环**：每个队列有独立的 `processLoop()` 异步循环，独立运行
2. **任务隔离**：每个队列只处理自己队列中的任务（通过队列名称隔离）
3. **独立的并发控制**：每个队列有独立的 `processing` Set 和 `concurrency` 限制
4. **并行处理**：不同队列可以同时处理任务，互不干扰
5. **阻塞隔离**：一个队列的慢任务或阻塞不会影响其他队列

**示例场景**：
- 邮件队列处理快速任务（100ms），配置 5 个并发
- 图片队列处理慢速任务（2秒），配置 3 个并发
- 报表队列处理非常慢的任务（5秒），配置 2 个并发

即使报表队列的任务很慢，也不会阻塞邮件队列和图片队列的处理。


## 注意事项

- **持久化适配器**：**必须提供适配器实例**（Redis、Memcached、RabbitMQ 或 MongoDB）
  - 队列库已内置 `RedisQueueAdapter`、`MemcachedQueueAdapter`、`RabbitMQQueueAdapter` 和 `MongoDBQueueAdapter`
  - 创建 `QueueManager` 时必须提供适配器实例（不支持默认适配器）
  - 推荐使用持久化适配器（Redis、Memcached、RabbitMQ、MongoDB），应用重启后任务会自动恢复
  - 内存适配器（`MemoryQueueAdapter`）仅用于开发和测试，不支持持久化
  - ⚠️ **Memcached 适配器注意**：Memcached 是内存缓存系统，只要服务不重启数据不会丢失，但服务重启后数据会丢失。如果需要真正的持久化（服务重启后数据不丢失），请使用 Redis 或 MongoDB 适配器
- **并发控制**：每个队列独立的并发控制，互不影响
- **任务重试**：任务失败后会自动重试，直到达到最大重试次数
- **任务超时**：任务执行超时后会被标记为失败
- **定时任务**：
  - 使用 `@dreamer/runtime-adapter` 的 cron API，兼容 Deno 和 Bun 环境
  - 支持标准的 5 字段和 6 字段 Cron 表达式格式
  - 使用 UTC 时区来指定计划时间
- **Cron 表达式**：支持标准格式，使用 UTC 时区
- **测试注意事项**：
  - 在 Deno 环境下测试时，如果使用 Redis 或 RabbitMQ 适配器，可能会遇到定时器泄漏警告
  - 这是因为第三方客户端库（如 `npm:redis`、`npm:amqplib`）可能产生内部定时器
  - 如果使用 `@dreamer/test` 进行测试，可以使用 `sanitizeOps: false` 和 `sanitizeResources: false` 选项来禁用定时器检查
  - 示例：
    ```typescript
    import { it } from "@dreamer/test";

    it("应该使用 Redis 适配器处理任务", async () => {
      // 测试代码...
    }, {
      sanitizeOps: false,        // 禁用定时器泄漏检查
      sanitizeResources: false,  // 禁用资源泄漏检查
    });
    ```

## 更多信息

- 适配器接口：实现 `QueueAdapter` 接口以支持自定义持久化
- 任务状态：任务状态会自动更新，可通过 `getJob` 查询
- 故障恢复：自动恢复超时的处理中任务，防止任务丢失

---

## 📝 备注

- **服务端专用**：队列系统是服务端架构模式，客户端不需要
- **统一接口**：提供统一的队列管理 API 接口，降低学习成本
- **适配器模式**：支持多种持久化后端（Redis、RabbitMQ、MongoDB），易于扩展
- **类型安全**：完整的 TypeScript 类型支持
- **依赖**：需要相应的队列适配器（Redis、RabbitMQ、MongoDB）

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
