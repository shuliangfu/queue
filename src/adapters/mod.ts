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
export type { RedisAdapterOptions } from "./redis.ts";
export { RedisQueueAdapter } from "./redis.ts";

// 导出 RabbitMQ 适配器
export type { RabbitMQAdapterOptions } from "./rabbitmq.ts";
export { RabbitMQQueueAdapter } from "./rabbitmq.ts";
