/**
 * @module @dreamer/queue/adapters/redis
 *
 * @fileoverview Redis 队列适配器
 *
 * 使用 Redis 作为任务存储后端，支持任务持久化和故障恢复。
 */

import type { Job, JobPriority, QueueAdapter } from "./base.ts";

/**
 * Redis 连接配置
 */
export interface RedisConnectionConfig {
  /** Redis 连接 URL（例如：redis://127.0.0.1:6379） */
  url?: string;
  /** Redis 主机地址（默认：127.0.0.1） */
  host?: string;
  /** Redis 端口（默认：6379） */
  port?: number;
  /** Redis 密码（可选） */
  password?: string;
  /** Redis 数据库编号（默认：0） */
  db?: number;
  /** Socket 选项 */
  socket?: {
    /** 是否启用 keepAlive（默认：false，减少内部定时器） */
    keepAlive?: boolean;
    /** 连接超时时间（毫秒，默认：5000） */
    connectTimeout?: number;
  };
}

/**
 * Redis 队列适配器配置
 */
export interface RedisAdapterOptions {
  /** Redis 连接配置（如果提供，适配器会内部创建连接） */
  connection?: RedisConnectionConfig;
  /** Redis 客户端实例（如果提供 connection，则不需要提供 client） */
  client?: {
    /** 设置键值 */
    set(key: string, value: string): Promise<void> | void;
    /** 获取值 */
    get(key: string): Promise<string | null> | string | null;
    /** 从列表左侧推入 */
    lpush(key: string, value: string): Promise<number> | number;
    /** 从列表右侧弹出 */
    rpop(key: string): Promise<string | null> | string | null;
    /** 获取列表所有元素 */
    lrange(
      key: string,
      start: number,
      stop: number,
    ): Promise<string[]> | string[];
    /** 删除键 */
    del(key: string): Promise<number> | number;
    /** 获取列表长度 */
    llen(key: string): Promise<number> | number;
    /** 从列表中移除元素（LREM 命令） */
    lrem?(
      key: string,
      count: number,
      value: string,
    ): Promise<number> | number;
    /** 断开连接 */
    disconnect?: () => Promise<void> | void;
    /** 退出连接 */
    quit?: () => Promise<void> | void;
  };
  /** 键前缀（可选，默认：queue） */
  keyPrefix?: string;
}

/**
 * Redis 队列适配器（持久化）
 *
 * 使用 Redis 作为任务存储后端，支持任务持久化和故障恢复。
 *
 * 适配器会自动创建和管理 Redis 连接，用户只需提供连接参数。
 *
 * @example
 * ```typescript
 * import { RedisQueueAdapter } from "jsr:@dreamer/queue/adapters";
 *
 * // 方式1：使用连接配置（推荐）
 * const adapter = new RedisQueueAdapter({
 *   connection: { url: "redis://localhost:6379" }
 * });
 * await adapter.connect();
 *
 * // 方式2：使用已创建的客户端（兼容旧代码）
 * const adapter = new RedisQueueAdapter({ client: redisClient });
 * ```
 */
export class RedisQueueAdapter implements QueueAdapter {
  private client: RedisAdapterOptions["client"];
  private keyPrefix: string;
  private internalClient: any = null; // 内部创建的客户端
  private connectionConfig?: RedisConnectionConfig;

  constructor(options: RedisAdapterOptions) {
    if (options.connection) {
      // 如果提供了连接配置，保存配置，稍后创建连接
      this.connectionConfig = options.connection;
      this.keyPrefix = options.keyPrefix || "queue";
    } else if (options.client) {
      // 如果提供了客户端，直接使用
      this.client = options.client;
      this.keyPrefix = options.keyPrefix || "queue";
    } else {
      throw new Error(
        "RedisQueueAdapter 需要提供 connection 配置或 client 实例",
      );
    }
  }

  /**
   * 连接到 Redis（如果使用 connection 配置）
   */
  async connect(): Promise<void> {
    if (this.connectionConfig && !this.internalClient) {
      try {
        // 动态导入 Redis 客户端库
        // 在 Bun 中，直接使用包名；在 Deno 中，使用 npm: 前缀
        const isBun = typeof (globalThis as any).Bun !== "undefined";
        const redisModule = isBun
          ? await import("redis")
          : await import("npm:redis@^5.0.0");
        const { createClient } = redisModule;

        // 构建连接配置
        const clientOptions: any = {};
        if (this.connectionConfig.url) {
          clientOptions.url = this.connectionConfig.url;
          // 如果使用 URL，也需要设置 socket 选项来减少定时器
          clientOptions.socket = {
            keepAlive: this.connectionConfig.socket?.keepAlive ?? false,
            connectTimeout: this.connectionConfig.socket?.connectTimeout ??
              5000,
            // 禁用 reconnect 以减少定时器
            reconnectStrategy: false,
          };
        } else {
          clientOptions.socket = {
            host: this.connectionConfig.host || "127.0.0.1",
            port: this.connectionConfig.port || 6379,
            keepAlive: this.connectionConfig.socket?.keepAlive ?? false,
            connectTimeout: this.connectionConfig.socket?.connectTimeout ??
              5000,
            // 禁用 reconnect 以减少定时器
            reconnectStrategy: false,
          };
          if (this.connectionConfig.password) {
            clientOptions.password = this.connectionConfig.password;
          }
          if (this.connectionConfig.db !== undefined) {
            clientOptions.database = this.connectionConfig.db;
          }
        }

        // 创建并连接客户端
        this.internalClient = createClient(clientOptions);
        await this.internalClient.connect();

        // 包装为适配器需要的接口
        this.client = {
          set: async (key: string, value: string) => {
            await this.internalClient.set(key, value);
          },
          get: (key: string) => this.internalClient.get(key),
          lpush: (key: string, value: string) =>
            this.internalClient.lPush(key, value),
          rpop: (key: string) => this.internalClient.rPop(key),
          lrange: (key: string, start: number, stop: number) =>
            this.internalClient.lRange(key, start, stop),
          del: (key: string) => this.internalClient.del(key),
          llen: (key: string) => this.internalClient.lLen(key),
          lrem: (key: string, count: number, value: string) =>
            this.internalClient.lRem(key, count, value),
        };
      } catch (error) {
        throw new Error(
          `无法创建 Redis 连接: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
  }

  /**
   * 断开 Redis 连接
   */
  async disconnect(): Promise<void> {
    if (this.internalClient) {
      try {
        // 先尝试优雅关闭
        if (this.internalClient.quit) {
          await this.internalClient.quit();
        } else if (this.internalClient.disconnect) {
          await this.internalClient.disconnect();
        }
      } catch {
        // 如果 quit 失败，尝试强制断开
        try {
          if (this.internalClient.disconnect) {
            await this.internalClient.disconnect();
          }
        } catch {
          // 忽略断开错误
        }
      }
      // 等待客户端完全关闭（给内部定时器时间清理）
      // Redis 客户端可能有内部定时器（如 keepAlive、重连等）
      await new Promise((resolve) => setTimeout(resolve, 100));
      this.internalClient = null;
      this.client = undefined;
    } else if (this.client?.disconnect) {
      // 如果使用的是外部客户端，调用其 disconnect 方法
      await this.client.disconnect();
      // 等待客户端完全关闭
      await new Promise((resolve) => setTimeout(resolve, 100));
    } else if (this.client?.quit) {
      // 如果使用的是外部客户端，调用其 quit 方法
      await this.client.quit();
      // 等待客户端完全关闭
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  /**
   * 获取任务键名
   */
  private getJobKey(jobId: string): string {
    return `${this.keyPrefix}:job:${jobId}`;
  }

  /**
   * 获取队列键名
   */
  private getQueueKey(queueName: string): string {
    return `${this.keyPrefix}:queue:${queueName}`;
  }

  /**
   * 从任务 ID 提取队列名称
   * 任务 ID 格式：${queueName}-${timestamp}-${random}
   * 例如：test-redis-process-1234567890-abc123
   */
  private getQueueName(jobId: string): string {
    // 任务 ID 格式：queueName-timestamp-random
    // 需要提取第一个部分（队列名称可能包含连字符）
    // 实际上，任务 ID 的格式是：${this.name}-${Date.now()}-${random}
    // 所以队列名称是除了最后两个部分（timestamp 和 random）之外的所有部分
    const parts = jobId.split("-");
    if (parts.length >= 3) {
      // 队列名称是除了最后两个部分之外的所有部分
      return parts.slice(0, -2).join("-");
    }
    // 如果格式不符合预期，返回第一个部分作为后备
    return parts[0] || "default";
  }

  /**
   * 比较优先级
   */
  private comparePriority(a: JobPriority, b: JobPriority): number {
    const priorityMap: Record<JobPriority, number> = {
      low: 1,
      normal: 2,
      high: 3,
      urgent: 4,
    };
    return priorityMap[a] - priorityMap[b];
  }

  async add(job: Job): Promise<void> {
    if (!this.client) {
      throw new Error("Redis 客户端未连接，请先调用 connect()");
    }
    // 存储任务数据
    const jobKey = this.getJobKey(job.id);
    await this.client.set(jobKey, JSON.stringify(job));

    // 添加到队列列表
    const queueName = this.getQueueName(job.id);
    const queueKey = this.getQueueKey(queueName);
    await this.client.lpush(queueKey, job.id);
  }

  async getNext(queueName: string): Promise<Job | null> {
    if (!this.client) {
      throw new Error("Redis 客户端未连接，请先调用 connect()");
    }
    const queueKey = this.getQueueKey(queueName);
    const length = await this.client.llen(queueKey);

    if (length === 0) {
      return null;
    }

    // 获取所有任务 ID
    const jobIds = await this.client.lrange(queueKey, 0, -1);
    const now = Date.now();
    let nextJob: Job | null = null;
    let nextJobId: string | null = null;

    // 查找下一个可执行的任务（考虑延迟和优先级）
    for (const jobId of jobIds) {
      const jobKey = this.getJobKey(jobId);
      const jobData = await this.client.get(jobKey);
      if (!jobData) continue;

      const job = JSON.parse(jobData) as Job;
      if (job.status !== "pending") {
        continue;
      }

      // 检查延迟
      if (job.delay && job.createdAt + job.delay > now) {
        continue;
      }

      // 选择优先级最高的任务
      if (
        !nextJob || this.comparePriority(job.priority, nextJob.priority) > 0
      ) {
        nextJob = job;
        nextJobId = jobId;
      }
    }

    if (nextJob && nextJobId) {
      // 更新任务状态
      nextJob.status = "processing";
      nextJob.startedAt = now;
      const jobKey = this.getJobKey(nextJobId);
      await this.client.set(jobKey, JSON.stringify(nextJob));

      // 从队列列表中移除该任务 ID（使用 LREM）
      const queueKey = this.getQueueKey(queueName);
      if (this.client.lrem) {
        // 使用 LREM 移除一个匹配的元素（count=1 表示只移除第一个匹配的）
        await this.client.lrem(queueKey, 1, nextJobId);
      } else {
        // 如果客户端不支持 LREM，使用 lrange + lpush 的方式重建列表
        // 注意：这不是原子操作，但在大多数情况下可以工作
        const allJobIds = await this.client.lrange(queueKey, 0, -1);
        const filteredJobIds = allJobIds.filter((id) => id !== nextJobId);

        // 删除原列表并重建
        await this.client.del(queueKey);
        if (filteredJobIds.length > 0) {
          for (const id of filteredJobIds.reverse()) {
            await this.client.lpush(queueKey, id);
          }
        }
      }

      return nextJob;
    }

    return null;
  }

  async update(jobId: string, updates: Partial<Job>): Promise<void> {
    if (!this.client) {
      throw new Error("Redis 客户端未连接，请先调用 connect()");
    }
    const jobKey = this.getJobKey(jobId);
    const jobData = await this.client.get(jobKey);
    if (jobData) {
      const job = JSON.parse(jobData) as Job;
      Object.assign(job, updates);
      await this.client.set(jobKey, JSON.stringify(job));
    }
  }

  async get(jobId: string): Promise<Job | null> {
    if (!this.client) {
      throw new Error("Redis 客户端未连接，请先调用 connect()");
    }
    const jobKey = this.getJobKey(jobId);
    const jobData = await this.client.get(jobKey);
    if (!jobData) return null;
    return JSON.parse(jobData) as Job;
  }

  async remove(jobId: string): Promise<void> {
    if (!this.client) {
      throw new Error("Redis 客户端未连接，请先调用 connect()");
    }
    // 删除任务数据
    const jobKey = this.getJobKey(jobId);
    await this.client.del(jobKey);

    // 从队列列表中移除
    const queueName = this.getQueueName(jobId);
    const queueKey = this.getQueueKey(queueName);

    if (this.client.lrem) {
      // 使用 LREM 移除所有匹配的元素（count=0 表示移除所有匹配的）
      await this.client.lrem(queueKey, 0, jobId);
    } else {
      // 如果客户端不支持 LREM，使用 lrange + lpush 的方式重建列表
      const allJobIds = await this.client.lrange(queueKey, 0, -1);
      const filteredJobIds = allJobIds.filter((id) => id !== jobId);

      // 删除原列表并重建
      await this.client.del(queueKey);
      if (filteredJobIds.length > 0) {
        for (const id of filteredJobIds.reverse()) {
          await this.client.lpush(queueKey, id);
        }
      }
    }
  }

  async getAll(queueName: string): Promise<Job[]> {
    if (!this.client) {
      throw new Error("Redis 客户端未连接，请先调用 connect()");
    }
    const queueKey = this.getQueueKey(queueName);
    const jobIds = await this.client.lrange(queueKey, 0, -1);
    const jobs: Job[] = [];

    for (const jobId of jobIds) {
      const job = await this.get(jobId);
      if (job) {
        jobs.push(job);
      }
    }

    return jobs;
  }

  async clear(queueName: string): Promise<void> {
    if (!this.client) {
      throw new Error("Redis 客户端未连接，请先调用 connect()");
    }
    const queueKey = this.getQueueKey(queueName);
    const jobIds = await this.client.lrange(queueKey, 0, -1);

    // 删除所有任务
    for (const jobId of jobIds) {
      const jobKey = this.getJobKey(jobId);
      await this.client.del(jobKey);
    }

    // 清空队列列表
    await this.client.del(queueKey);
  }

  async getStats(queueName: string): Promise<{
    pending: number;
    processing: number;
    completed: number;
    failed: number;
  }> {
    const jobs = await this.getAll(queueName);
    return {
      pending: jobs.filter((j) => j.status === "pending").length,
      processing: jobs.filter((j) => j.status === "processing").length,
      completed: jobs.filter((j) => j.status === "completed").length,
      failed: jobs.filter((j) => j.status === "failed").length,
    };
  }
}
