/**
 * @module @dreamer/queue/adapters
 *
 * @fileoverview 队列适配器模块
 *
 * 导出所有队列适配器实现
 */

// 导出基础类型和接口
export type {
  Job,
  JobData,
  JobPriority,
  JobStatus,
  QueueAdapter,
} from "./base.ts";

// 导出内存适配器
export { MemoryQueueAdapter } from "./memory.ts";

// 导出 Redis 适配器
export { RedisQueueAdapter } from "./redis.ts";
export type {
  RedisAdapterOptions,
  RedisConnectionConfig,
} from "./redis.ts";

// 导出 RabbitMQ 适配器
export { RabbitMQQueueAdapter } from "./rabbitmq.ts";
export type {
  RabbitMQAdapterOptions,
  RabbitMQConnectionConfig,
} from "./rabbitmq.ts";

// 导出 MongoDB 适配器
export { MongoDBQueueAdapter } from "./mongodb.ts";
export type {
  MongoDBAdapterOptions,
  MongoDBConnectionConfig,
} from "./mongodb.ts";
