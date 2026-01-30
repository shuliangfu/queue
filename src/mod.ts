/**
 * @module @dreamer/queue
 *
 * @fileoverview 队列和任务调度库
 *
 * 提供任务队列、任务调度、并发控制等功能。
 * 支持内存队列和持久化队列（通过适配器支持 Redis、RabbitMQ）。
 *
 * 定时任务功能使用 @dreamer/runtime-adapter 的 cron API，兼容 Deno 和 Bun 环境。
 */

// 导入类型供当前文件内部使用
import type {
  Job,
  JobData,
  JobPriority,
  QueueAdapter,
} from "./adapters/mod.ts";

// 导入服务容器类型（可选依赖）
import type { ServiceContainer } from "@dreamer/service";

// 导入 runtime-adapter 的 cron API
import { cron, type CronHandle, IS_DENO } from "@dreamer/runtime-adapter";
// 导入类型供当前文件使用
import type { MemcachedConnectionConfig } from "./adapters/memcached.ts";
import type { MongoDBConnectionConfig } from "./adapters/mongodb.ts";
import type { RabbitMQConnectionConfig } from "./adapters/rabbitmq.ts";
import type { RedisConnectionConfig } from "./adapters/redis.ts";

// 导出所有内容（从 adapters/mod.ts 统一导出，包括类型和值）
export * from "./adapters/mod.ts";

/**
 * 任务处理函数
 */
export type JobProcessor<T extends JobData = JobData> = (
  job: Job & { data: T },
) => Promise<void>;

/**
 * 队列选项
 */
export interface QueueOptions {
  /** 队列名称 */
  name: string;
  /** 最大并发数 */
  concurrency?: number;
  /** 是否支持优先级 */
  priority?: boolean;
  /** 默认重试次数 */
  retry?: number;
  /** 是否启用持久化 */
  persistent?: boolean;
  /** 任务超时时间（毫秒） */
  timeout?: number;
}

/**
 * 队列管理器选项
 */
export interface QueueManagerOptions {
  /** 队列适配器（必须提供，推荐使用 RedisQueueAdapter 或 RabbitMQQueueAdapter） */
  adapter: QueueAdapter;
  /** 是否自动恢复未完成的任务 */
  autoRecover?: boolean;
  /** 恢复超时任务的时间（毫秒） */
  recoverTimeout?: number;
}

/**
 * 队列配置选项
 * 统一的配置接口，支持所有适配器类型
 */
export interface QueueConfig extends Omit<QueueManagerOptions, "adapter"> {
  /** 适配器类型（memory、redis、mongodb、rabbitmq、memcached） */
  adapter?: "memory" | "redis" | "mongodb" | "rabbitmq" | "memcached";
  /** Redis 连接配置（仅 redis 适配器） */
  connection?: RedisConnectionConfig;
  /** Redis 客户端实例（仅 redis 适配器，如果提供 connection，则不需要提供 client） */
  client?: unknown;
  /** MongoDB 连接配置（仅 mongodb 适配器） */
  mongodbConnection?: MongoDBConnectionConfig;
  /** MongoDB 客户端实例（仅 mongodb 适配器，如果提供 mongodbConnection，则不需要提供 mongodbClient） */
  mongodbClient?: unknown;
  /** MongoDB 集合名称（仅 mongodb 适配器，默认：jobs） */
  collection?: string;
  /** RabbitMQ 连接配置（仅 rabbitmq 适配器） */
  rabbitmqConnection?: RabbitMQConnectionConfig;
  /** RabbitMQ 连接对象（仅 rabbitmq 适配器，如果提供 rabbitmqConnection，则不需要提供 rabbitmqConnectionObject） */
  rabbitmqConnectionObject?: unknown;
  /** RabbitMQ 队列选项（仅 rabbitmq 适配器） */
  queueOptions?: {
    /** 是否持久化 */
    durable?: boolean;
  };
  /** Memcached 连接配置（仅 memcached 适配器） */
  memcachedConnection?: MemcachedConnectionConfig;
  /** Memcached 客户端实例（仅 memcached 适配器，如果提供 memcachedConnection，则不需要提供 memcachedClient） */
  memcachedClient?: unknown;
}

/**
 * 添加任务选项
 */
export interface AddJobOptions {
  /** 任务优先级 */
  priority?: JobPriority;
  /** 延迟执行时间（毫秒） */
  delay?: number;
  /** 最大重试次数 */
  maxAttempts?: number;
  /** 超时时间（毫秒） */
  timeout?: number;
  /** 是否持久化 */
  persistent?: boolean;
}

/**
 * 定时任务选项
 */
export interface ScheduleOptions {
  /** 任务名称 */
  name: string;
  /** Cron 表达式 */
  cron: string;
  /** 任务数据 */
  data?: JobData;
  /** 是否启用 */
  enabled?: boolean;
  /** 目标队列名称（可选，默认使用第一个队列） */
  queueName?: string;
}

/**
 * 定时任务处理器
 */
export type ScheduledTaskHandler = (data?: JobData) => void | Promise<void>;

/**
 * 队列类
 */
export class Queue {
  private name: string;
  private adapter: QueueAdapter;
  private processor?: JobProcessor;
  private concurrency: number;
  private retry: number;
  private timeout?: number;
  private running: boolean = false;
  private processing: Set<string> = new Set();
  private intervalId?: number;
  private pendingTimeouts: Set<number> = new Set(); // 跟踪所有待处理的定时器
  private lastJobFound: boolean = false; // 记录上次是否找到任务，用于动态延迟
  private consecutiveEmptyPolls: number = 0; // 连续空轮询次数

  constructor(name: string, adapter: QueueAdapter, options: QueueOptions) {
    this.name = name;
    this.adapter = adapter;
    this.concurrency = options.concurrency || 1;
    this.retry = options.retry || 0;
    this.timeout = options.timeout;
  }

  /**
   * 添加任务
   */
  async add(
    name: string,
    data: JobData,
    options: AddJobOptions = {},
  ): Promise<Job> {
    const job: Job = {
      id: `${this.name}.${Date.now()}.${
        Math.random().toString(36).substring(7)
      }`,
      name,
      data,
      status: "pending",
      priority: options.priority || "normal",
      createdAt: Date.now(),
      attempts: 0,
      maxAttempts: options.maxAttempts || this.retry,
      delay: options.delay,
      timeout: options.timeout || this.timeout,
    };

    // 在适配器中存储队列名称信息
    await this.adapter.add(job);
    return job;
  }

  /**
   * 处理任务
   */
  process<T extends JobData = JobData>(processor: JobProcessor<T>): void {
    this.processor = processor as JobProcessor;
    this.start();
  }

  /**
   * 开始处理任务
   */
  private start(): void {
    if (this.running) {
      return;
    }

    this.running = true;
    this.processLoop();
  }

  /**
   * 获取动态延迟时间（毫秒）
   * 根据队列状态和连续空轮询次数调整延迟
   * - 有任务时：0-10ms 快速轮询
   * - 无任务时：根据连续空轮询次数递增延迟（100ms → 500ms → 1000ms）
   */
  private getDynamicDelay(): number {
    // 如果正在处理任务，使用短延迟
    if (this.processing.size > 0) {
      return 0; // 立即继续处理
    }

    // 如果上次找到任务，使用短延迟
    if (this.lastJobFound) {
      this.consecutiveEmptyPolls = 0; // 重置连续空轮询计数
      return 10; // 10ms 快速轮询
    }

    // 如果上次没找到任务，根据连续空轮询次数递增延迟
    this.consecutiveEmptyPolls++;

    // 动态延迟：100ms → 200ms → 500ms → 1000ms（最大 1000ms）
    if (this.consecutiveEmptyPolls <= 1) {
      return 100; // 第一次空轮询，100ms
    } else if (this.consecutiveEmptyPolls <= 3) {
      return 200; // 2-3 次空轮询，200ms
    } else if (this.consecutiveEmptyPolls <= 5) {
      return 500; // 4-5 次空轮询，500ms
    } else {
      return 1000; // 5 次以上空轮询，1000ms（最大延迟）
    }
  }

  /**
   * 处理循环
   * 使用动态延迟优化性能：有任务时快速轮询，无任务时慢速轮询
   */
  private async processLoop(): Promise<void> {
    while (this.running) {
      try {
        // 检查并发限制
        if (this.processing.size >= this.concurrency) {
          if (!this.running) break;
          // 并发达到上限时，使用短延迟等待
          await new Promise((resolve) => {
            const id = setTimeout(() => {
              this.pendingTimeouts.delete(id as unknown as number);
              resolve(undefined);
            }, 50) as unknown as number; // 并发满时使用 50ms 延迟
            this.pendingTimeouts.add(id);
          });
          if (!this.running) break;
          continue;
        }

        // 获取下一个任务（可能因为适配器关闭而失败）
        let job: Job | null = null;
        try {
          if (!this.running) break;
          job = await this.adapter.getNext(this.name);
        } catch (error) {
          // 如果适配器已关闭或出错，停止处理循环
          if (!this.running) {
            break;
          }
          // 检查是否是连接关闭错误，如果是则静默处理（不输出错误日志）
          const errorMessage = error instanceof Error
            ? error.message
            : String(error);
          if (
            !errorMessage.includes("The client is closed") &&
            !errorMessage.includes("Connection closing") &&
            !errorMessage.includes("IllegalOperationError") &&
            !errorMessage.includes("Channel closed") &&
            !errorMessage.includes("未连接") &&
            !errorMessage.includes("未建立")
          ) {
            // 只有非连接关闭错误才记录（包括未连接错误）
            console.error(
              `获取任务失败: ${errorMessage}`,
            );
          }
          if (!this.running) break;
          // 错误时使用动态延迟
          const delay = this.getDynamicDelay();
          await new Promise((resolve) => {
            const id = setTimeout(() => {
              this.pendingTimeouts.delete(id as unknown as number);
              resolve(undefined);
            }, delay) as unknown as number;
            this.pendingTimeouts.add(id);
          });
          if (!this.running) break;
          this.lastJobFound = false; // 错误时标记为未找到任务
          continue;
        }

        if (!this.running) break;

        if (!job) {
          // 未找到任务，使用动态延迟
          this.lastJobFound = false;
          const delay = this.getDynamicDelay();
          if (!this.running) break;
          await new Promise((resolve) => {
            const id = setTimeout(() => {
              this.pendingTimeouts.delete(id as unknown as number);
              resolve(undefined);
            }, delay) as unknown as number;
            this.pendingTimeouts.add(id);
          });
          if (!this.running) break;
          continue;
        }

        // 找到任务，标记并处理
        this.lastJobFound = true;
        this.consecutiveEmptyPolls = 0; // 重置连续空轮询计数

        // 处理任务
        this.processJob(job).catch((error) => {
          console.error(`处理任务失败: ${job.id}`, error);
        });
      } catch (error) {
        // 捕获其他意外错误，避免未捕获的 promise rejection
        if (!this.running) {
          break;
        }
        console.error(
          `处理循环错误: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        if (!this.running) break;
        // 错误时使用动态延迟
        const delay = this.getDynamicDelay();
        await new Promise((resolve) => {
          const id = setTimeout(() => {
            this.pendingTimeouts.delete(id as unknown as number);
            resolve(undefined);
          }, delay) as unknown as number;
          this.pendingTimeouts.add(id);
        });
        if (!this.running) break;
        this.lastJobFound = false; // 错误时标记为未找到任务
      }
    }
  }

  /**
   * 处理单个任务
   */
  private async processJob(job: Job): Promise<void> {
    if (!this.processor) {
      return;
    }

    this.processing.add(job.id);

    try {
      // 设置超时
      let timeoutId: number | undefined;
      if (job.timeout) {
        timeoutId = setTimeout(() => {
          this.handleJobTimeout(job);
          // 超时触发后清理
          if (timeoutId !== undefined) {
            this.pendingTimeouts.delete(timeoutId);
          }
        }, job.timeout) as unknown as number;
        this.pendingTimeouts.add(timeoutId);
      }

      // 执行任务
      await this.processor(job as Job & { data: JobData });

      if (timeoutId) {
        clearTimeout(timeoutId);
        this.pendingTimeouts.delete(timeoutId);
      }

      // 检查任务是否还在处理中（可能已被超时处理）
      // 如果任务不在 processing 集合中，说明已经被超时处理，不应该标记为完成
      if (!this.processing.has(job.id)) {
        // 任务已被超时处理，不更新状态
        return;
      }

      // 再次检查任务当前状态，避免覆盖已失败的状态
      const currentJob = await this.adapter.get(job.id);
      if (currentJob && currentJob.status === "failed") {
        // 任务已被标记为失败（可能是超时），不更新状态
        return;
      }

      // 标记为完成
      await this.adapter.update(job.id, {
        status: "completed",
        completedAt: Date.now(),
      });
    } catch (error) {
      // 处理失败
      await this.handleJobError(job, error);
    } finally {
      this.processing.delete(job.id);
    }
  }

  /**
   * 处理任务错误
   */
  private async handleJobError(job: Job, error: unknown): Promise<void> {
    const attempts = job.attempts + 1;
    const errorMessage = error instanceof Error ? error.message : String(error);

    if (attempts <= job.maxAttempts) {
      // 重试
      await this.adapter.update(job.id, {
        status: "pending",
        attempts,
        error: errorMessage,
        startedAt: undefined,
      });
    } else {
      // 标记为失败
      await this.adapter.update(job.id, {
        status: "failed",
        failedAt: Date.now(),
        attempts,
        error: errorMessage,
      });
    }
  }

  /**
   * 处理任务超时
   */
  private async handleJobTimeout(job: Job): Promise<void> {
    await this.adapter.update(job.id, {
      status: "failed",
      failedAt: Date.now(),
      error: "任务执行超时",
    });
    this.processing.delete(job.id);
  }

  /**
   * 停止处理
   */
  stop(): void {
    this.running = false;
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = undefined;
    }
    // 清理所有待处理的定时器
    // 在 Deno 环境下，需要确保所有定时器都被清理
    for (const timeoutId of this.pendingTimeouts) {
      try {
        clearTimeout(timeoutId);
      } catch {
        // 忽略清理错误（定时器可能已经完成）
      }
    }
    this.pendingTimeouts.clear();
  }

  /**
   * 等待所有定时器完成（用于 Deno 环境下的资源清理）
   * 这是一个辅助方法，主要用于测试环境
   */
  async waitForTimers(): Promise<void> {
    if (IS_DENO && this.pendingTimeouts.size > 0) {
      // 在 Deno 环境下，等待所有定时器完成
      // 最大等待时间 200ms（定时器间隔是 100ms）
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
  }

  /**
   * 获取任务
   */
  async getJob(jobId: string): Promise<Job | null> {
    return await this.adapter.get(jobId);
  }

  /**
   * 获取所有任务
   */
  async getJobs(): Promise<Job[]> {
    return await this.adapter.getAll(this.name);
  }

  /**
   * 获取统计信息
   */
  async getStats(): Promise<{
    pending: number;
    processing: number;
    completed: number;
    failed: number;
  }> {
    return await this.adapter.getStats(this.name);
  }

  /**
   * 清空队列
   */
  async clear(): Promise<void> {
    return await this.adapter.clear(this.name);
  }
}

/**
 * 队列管理器配置选项（扩展）
 */
export interface QueueManagerOptionsExtended extends QueueManagerOptions {
  /** 管理器名称（用于服务容器注册） */
  name?: string;
}

/**
 * 队列管理器
 * 支持服务容器集成，可通过依赖注入方式管理
 */
export class QueueManager {
  /** 队列适配器 */
  private adapter: QueueAdapter;
  /** 队列映射 */
  private queues: Map<string, Queue> = new Map();
  /** 是否自动恢复 */
  private autoRecover: boolean;
  /** 恢复超时时间 */
  private recoverTimeout: number;
  /** 恢复定时器 ID */
  private recoveryIntervalId?: number;
  /** 定时任务配置 */
  private scheduledTasks: Map<string, ScheduleOptions> = new Map();
  /** 定时任务处理器 */
  private scheduledHandlers: Map<string, ScheduledTaskHandler> = new Map();
  /** Cron 任务映射 */
  private cronTasks: Map<
    string,
    { handle: CronHandle; signal: AbortController }
  > = new Map();
  /** 服务容器引用 */
  private container?: ServiceContainer;
  /** 管理器名称 */
  private readonly managerName: string;

  constructor(options: QueueManagerOptions | QueueManagerOptionsExtended) {
    if (!options.adapter) {
      throw new Error(
        "必须提供队列适配器！请使用 RedisQueueAdapter 或 RabbitMQQueueAdapter 实现持久化。\n" +
          "示例：\n" +
          "  import { QueueManager, RedisQueueAdapter } from 'jsr:@dreamer/queue';\n" +
          "  const adapter = new RedisQueueAdapter({ client: redisClient });\n" +
          "  const queueManager = new QueueManager({ adapter });",
      );
    }
    this.adapter = options.adapter;
    this.autoRecover = options.autoRecover ?? true;
    this.recoverTimeout = options.recoverTimeout || 30000;
    this.managerName = (options as QueueManagerOptionsExtended).name || "default";

    // 启动自动恢复
    if (this.autoRecover) {
      this.startRecovery();
    }
  }

  /**
   * 获取管理器名称
   * @returns 管理器名称
   */
  getName(): string {
    return this.managerName;
  }

  /**
   * 设置服务容器
   * 将管理器注册到服务容器中
   * @param container 服务容器实例
   * @returns 当前管理器实例（链式调用）
   */
  setContainer(container: ServiceContainer): this {
    this.container = container;
    // 注册自身到容器
    const serviceName = this.managerName === "default"
      ? "queueManager"
      : `queueManager:${this.managerName}`;
    container.registerSingleton(serviceName, () => this);
    return this;
  }

  /**
   * 获取服务容器
   * @returns 服务容器实例或 undefined
   */
  getContainer(): ServiceContainer | undefined {
    return this.container;
  }

  /**
   * 从服务容器获取队列管理器
   * @param container 服务容器实例
   * @param name 管理器名称（默认：default）
   * @returns 队列管理器实例
   */
  static fromContainer(container: ServiceContainer, name?: string): QueueManager {
    const serviceName = !name || name === "default"
      ? "queueManager"
      : `queueManager:${name}`;
    return container.get<QueueManager>(serviceName);
  }

  /**
   * 创建队列
   */
  createQueue(name: string, options: Partial<QueueOptions> = {}): Queue {
    if (this.queues.has(name)) {
      return this.queues.get(name)!;
    }

    const queueOptions: QueueOptions = {
      name,
      concurrency: options.concurrency || 1,
      priority: options.priority || false,
      retry: options.retry || 0,
      persistent: options.persistent || false,
      timeout: options.timeout,
    };

    const queue = new Queue(name, this.adapter, queueOptions);
    this.queues.set(name, queue);
    return queue;
  }

  /**
   * 获取队列
   */
  getQueue(name: string): Queue | undefined {
    return this.queues.get(name);
  }

  /**
   * 启动自动恢复
   */
  private startRecovery(): void {
    this.recoveryIntervalId = setInterval(async () => {
      await this.recoverJobs();
    }, this.recoverTimeout) as unknown as number;
  }

  /**
   * 恢复未完成的任务
   */
  private async recoverJobs(): Promise<void> {
    for (const [name, _queue] of this.queues.entries()) {
      const jobs = await this.adapter.getAll(name);
      const now = Date.now();

      for (const job of jobs) {
        // 恢复超时的处理中任务
        if (
          job.status === "processing" &&
          job.startedAt &&
          job.timeout &&
          now - job.startedAt > job.timeout
        ) {
          await this.adapter.update(job.id, {
            status: "pending",
            startedAt: undefined,
          });
        }
      }
    }
  }

  /**
   * 创建定时任务
   *
   * 使用 @dreamer/runtime-adapter 的 cron API，兼容 Deno 和 Bun 环境。
   * 注意：使用 UTC 时区。
   */
  private createCronTask(
    name: string,
    schedule: ScheduleOptions,
  ): void {
    // 如果任务已存在，先移除
    this.removeCronTask(name);

    // 创建 AbortController 用于取消任务
    const signal = new AbortController();

    // 使用 runtime-adapter 的 cron API 创建定时任务
    // runtime-adapter 使用 node-cron，支持 5 字段和 6 字段格式
    try {
      const handle = cron(
        schedule.cron,
        async () => {
          // 检查任务是否仍然启用
          const currentSchedule = this.scheduledTasks.get(name);
          if (!currentSchedule || !currentSchedule.enabled) {
            return;
          }

          // 执行定时任务
          await this.executeScheduledTask(name, currentSchedule).catch(
            (error) => {
              console.error(`执行定时任务失败: ${name}`, error);
            },
          );
        },
        {
          signal: signal.signal,
          timezone: "UTC",
        },
      );

      // 存储任务句柄
      this.cronTasks.set(name, { handle, signal });
    } catch (error) {
      console.error(
        `创建定时任务失败: ${name} - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error;
    }
  }

  /**
   * 简单的 Cron 匹配检查（已废弃，现在使用 runtime-adapter 的 cron）
   * @deprecated 此方法已不再使用，保留仅用于兼容性
   */
  private shouldRunCronSimple(cron: string, now: Date): boolean {
    const parts = cron.trim().split(/\s+/);
    if (parts.length !== 5) {
      return false;
    }

    const [minute, hour, day, month, weekday] = parts;

    // 简化的匹配逻辑
    if (minute !== "*" && parseInt(minute) !== now.getUTCMinutes()) {
      return false;
    }
    if (hour !== "*" && parseInt(hour) !== now.getUTCHours()) {
      return false;
    }
    if (day !== "*" && parseInt(day) !== now.getUTCDate()) {
      return false;
    }
    if (month !== "*" && parseInt(month) !== now.getUTCMonth() + 1) {
      return false;
    }
    if (weekday !== "*" && parseInt(weekday) !== now.getUTCDay()) {
      return false;
    }

    return true;
  }

  /**
   * 移除 cron 任务
   */
  private removeCronTask(name: string): void {
    const task = this.cronTasks.get(name);
    if (task) {
      try {
        // 使用 runtime-adapter 的 CronHandle 关闭任务
        // CronHandle 接口定义了 close() 方法
        if (task.handle && typeof task.handle.close === "function") {
          task.handle.close();
        }
        // 取消 AbortController（如果尚未取消）
        // AbortController.abort() 可以安全地多次调用
        if (task.signal && !task.signal.signal.aborted) {
          task.signal.abort();
        }
      } catch (error) {
        // 忽略关闭错误，确保任务被移除
        console.warn(`关闭定时任务失败: ${name}`, error);
      }
      this.cronTasks.delete(name);
    }
  }

  /**
   * 执行定时任务
   */
  private async executeScheduledTask(
    name: string,
    schedule: ScheduleOptions,
  ): Promise<void> {
    // 获取目标队列
    const queueName = schedule.queueName || this.queues.keys().next().value;
    const queue = queueName ? this.queues.get(queueName) : null;

    if (!queue) {
      console.warn(`定时任务 ${name} 的目标队列不存在`);
      return;
    }

    // 如果有处理器，直接执行
    const handler = this.scheduledHandlers.get(name);
    if (handler) {
      try {
        await handler(schedule.data);
      } catch (error) {
        console.error(`定时任务处理器执行失败: ${name}`, error);
      }
    } else {
      // 否则添加到队列
      await queue.add(name, schedule.data || {});
    }
  }

  /**
   * 添加定时任务
   *
   * 使用 @dreamer/runtime-adapter 的 cron API 来调度任务。
   * 注意：使用 UTC 时区来指定计划时间。
   * 支持 5 字段格式（分钟 小时 日 月 星期）和 6 字段格式（秒 分钟 小时 日 月 星期）。
   *
   * @param name 任务名称
   * @param cron Cron 表达式（标准 5 字段格式或 6 字段格式）
   * @param handler 任务处理器（可选，如果提供则直接执行，否则添加到队列）
   * @param options 选项（队列名称、数据等）
   */
  schedule(
    name: string,
    cron: string,
    handler?: ScheduledTaskHandler,
    options?: {
      queueName?: string;
      data?: JobData;
    },
  ): void {
    const schedule: ScheduleOptions = {
      name,
      cron,
      data: options?.data,
      enabled: true,
      queueName: options?.queueName,
    };

    this.scheduledTasks.set(name, schedule);

    if (handler) {
      this.scheduledHandlers.set(name, handler);
    }

    // 使用 runtime-adapter 的 cron API 创建定时任务
    try {
      this.createCronTask(name, schedule);
    } catch (error) {
      console.error(`创建定时任务失败: ${name}`, error);
    }
  }

  /**
   * 移除定时任务
   */
  unschedule(name: string): void {
    // 移除 cron 任务
    this.removeCronTask(name);

    // 移除任务配置
    this.scheduledTasks.delete(name);
    this.scheduledHandlers.delete(name);
  }

  /**
   * 关闭管理器
   */
  async close(): Promise<void> {
    await Promise.resolve();
    // 停止自动恢复
    if (this.recoveryIntervalId !== undefined) {
      clearInterval(this.recoveryIntervalId);
      this.recoveryIntervalId = undefined;
    }

    // 停止所有队列
    for (const queue of this.queues.values()) {
      queue.stop();
    }

    // 停止所有 cron 任务
    for (const [name] of this.cronTasks.entries()) {
      this.removeCronTask(name);
    }
  }
}

/**
 * 创建队列管理器的工厂函数
 * @param options 队列管理器配置选项
 * @param container 服务容器实例（可选）
 * @returns 队列管理器实例
 *
 * @example
 * ```typescript
 * import { createQueueManager, RedisQueueAdapter } from "@dreamer/queue";
 * import { ServiceContainer } from "@dreamer/service";
 *
 * const container = new ServiceContainer();
 * const adapter = new RedisQueueAdapter({ client: redisClient });
 *
 * // 创建并注册到服务容器
 * const queueManager = createQueueManager({ adapter }, container);
 *
 * // 之后可以从容器获取
 * const queueFromContainer = QueueManager.fromContainer(container);
 * ```
 */
export function createQueueManager(
  options: QueueManagerOptions | QueueManagerOptionsExtended,
  container?: ServiceContainer,
): QueueManager {
  const manager = new QueueManager(options);
  if (container) {
    manager.setContainer(container);
  }
  return manager;
}
