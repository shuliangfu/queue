/**
 * @module @dreamer/queue/adapters/memcached
 *
 * @fileoverview Memcached 队列适配器
 *
 * 使用 Memcached 作为任务存储后端，支持任务持久化（只要 Memcached 服务不重启，数据不会丢失）。
 * Memcached 是内存缓存系统，性能高，适合单机或小规模分布式场景。
 */

import type { Job, JobPriority, QueueAdapter } from "./base.ts";

/**
 * Memcached 连接配置
 */
export interface MemcachedConnectionConfig {
  /** Memcached 服务器地址（默认：127.0.0.1） */
  host?: string;
  /** Memcached 端口（默认：11211） */
  port?: number;
  /** 连接超时时间（毫秒，默认：5000） */
  timeout?: number;
  /** 是否启用压缩（默认：false） */
  compress?: boolean;
  /** 最大连接数（默认：10） */
  maxConnections?: number;
}

/**
 * Memcached 客户端接口（用于队列适配器）
 *
 * 此类型定义了队列适配器所需的 Memcached 客户端接口，可以在框架中直接使用。
 */
export interface MemcachedQueueClient {
  /** 设置键值 */
  set(
    key: string,
    value: string,
    options?: { expires?: number },
  ): Promise<boolean>;
  /** 获取值 */
  get(key: string): Promise<string | null>;
  /** 删除键 */
  delete(key: string): Promise<boolean>;
  /** 批量获取值 */
  getMulti?(keys: string[]): Promise<Record<string, string | null>>;
  /** 关闭连接 */
  close?: () => Promise<void> | void;
  /** 退出连接 */
  quit?: () => Promise<void> | void;
}

/**
 * Memcached 队列适配器配置
 */
export interface MemcachedAdapterOptions {
  /** Memcached 连接配置（如果提供，适配器会内部创建连接） */
  connection?: MemcachedConnectionConfig;
  /** Memcached 客户端实例（如果提供 connection，则不需要提供 client） */
  client?: MemcachedQueueClient;
  /** 键前缀（可选，默认：queue） */
  keyPrefix?: string;
}

/**
 * Memcached 队列适配器（持久化）
 *
 * 使用 Memcached 作为任务存储后端，支持任务持久化。
 * ⚠️ 注意：Memcached 是内存缓存系统，数据存储在内存中。
 * 只要 Memcached 服务不重启，数据不会丢失。但服务重启后数据会丢失。
 * 如果需要真正的持久化（服务重启后数据不丢失），请使用 RedisQueueAdapter 或 MongoDBQueueAdapter。
 *
 * 适配器会自动创建和管理 Memcached 连接，用户只需提供连接参数。
 *
 * @example
 * ```typescript
 * import { MemcachedQueueAdapter } from "jsr:@dreamer/queue/adapters";
 *
 * // 方式1：使用连接配置（推荐）
 * const adapter = new MemcachedQueueAdapter({
 *   connection: { host: "127.0.0.1", port: 11211 }
 * });
 * await adapter.connect();
 *
 * // 方式2：使用已创建的客户端（兼容旧代码）
 * const adapter = new MemcachedQueueAdapter({ client: memcachedClient });
 * ```
 */
export class MemcachedQueueAdapter implements QueueAdapter {
  private client: MemcachedAdapterOptions["client"];
  private keyPrefix: string;
  private internalClient: any = null; // 内部创建的客户端
  private connectionConfig?: MemcachedConnectionConfig;

  constructor(options: MemcachedAdapterOptions) {
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
        "MemcachedQueueAdapter 需要提供 connection 配置或 client 实例",
      );
    }
  }

  /**
   * 连接到 Memcached（如果使用 connection 配置）
   */
  async connect(): Promise<void> {
    if (this.connectionConfig && !this.internalClient) {
      try {
        // 动态导入 memcache-client（npm 包）
        const { MemcacheClient } = await import("memcache-client");

        // 构建连接配置
        const host = this.connectionConfig.host || "127.0.0.1";
        const port = this.connectionConfig.port || 11211;
        const timeout = this.connectionConfig.timeout || 5000;
        const compress = this.connectionConfig.compress || false;
        const maxConnections = this.connectionConfig.maxConnections || 10;

        // 创建 Memcached 客户端
        const clientOptions: any = {
          server: `${host}:${port}`,
          connectTimeout: timeout,
          cmdTimeout: timeout, // 命令超时时间
          maxConnections,
        };

        // 如果启用压缩，添加 compressor 选项
        if (compress) {
          clientOptions.compressor = true;
        }

        this.internalClient = new MemcacheClient(clientOptions);

        // 包装为适配器需要的接口
        this.client = {
          set: async (
            key: string,
            value: string,
            options?: { expires?: number },
          ) => {
            // Memcached 的过期时间以秒为单位（lifetime 选项）
            const lifetime = options?.expires
              ? Math.floor(options.expires / 1000)
              : undefined;
            await this.internalClient.set(key, value, { lifetime });
            return true;
          },
          get: async (key: string) => {
            const result = await this.internalClient.get(key);
            // memcache-client 返回 { value: ... } 格式
            if (result && typeof result === "object" && "value" in result) {
              const value = result.value;
              // 如果 value 是 Uint8Array，转换为字符串
              if (value instanceof Uint8Array) {
                return new TextDecoder().decode(value);
              }
              // 如果是字符串，直接返回
              if (typeof value === "string") {
                return value;
              }
              // 其他类型转换为 JSON 字符串
              return JSON.stringify(value);
            }
            return null;
          },
          delete: async (key: string) => {
            await this.internalClient.delete(key);
            return true;
          },
          getMulti: async (keys: string[]) => {
            // memcache-client 支持传入数组到 get 方法
            const results = await this.internalClient.get(keys);
            const record: Record<string, string | null> = {};
            for (const key of keys) {
              const result = results[key];
              if (result && typeof result === "object" && "value" in result) {
                const value = result.value;
                // 如果 value 是 Uint8Array，转换为字符串
                if (value instanceof Uint8Array) {
                  record[key] = new TextDecoder().decode(value);
                } else if (typeof value === "string") {
                  record[key] = value;
                } else {
                  record[key] = JSON.stringify(value);
                }
              } else {
                record[key] = null;
              }
            }
            return record;
          },
          close: async () => {
            await this.internalClient.quit();
          },
        };
      } catch (error) {
        throw new Error(
          `无法创建 Memcached 连接: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
  }

  /**
   * 断开 Memcached 连接
   */
  async disconnect(): Promise<void> {
    if (this.internalClient) {
      try {
        if (this.internalClient.quit) {
          await this.internalClient.quit();
        } else if (this.internalClient.close) {
          await this.internalClient.close();
        }
      } catch {
        // 忽略断开错误
      }
      this.internalClient = null;
      this.client = undefined;
    } else if (this.client?.close) {
      // 如果使用的是外部客户端，调用其 close 方法
      await this.client.close();
    } else if (this.client?.quit) {
      // 如果使用的是外部客户端，调用其 quit 方法
      await this.client.quit();
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
   * 任务 ID 格式：${queueName}.${timestamp}.${random}
   */
  private getQueueName(jobId: string): string {
    const parts = jobId.split(".");
    if (parts.length >= 3) {
      return parts.slice(0, -2).join(".");
    }
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
      throw new Error("Memcached 客户端未连接，请先调用 connect()");
    }

    // 存储任务数据（不过期，因为 Memcached 会一直保存直到服务重启）
    const jobKey = this.getJobKey(job.id);
    await this.client.set(jobKey, JSON.stringify(job));

    // 获取队列列表键
    const queueName = this.getQueueName(job.id);
    const queueKey = this.getQueueKey(queueName);

    // 获取当前队列列表
    const queueListStr = await this.client.get(queueKey);
    const queueList: string[] = queueListStr ? JSON.parse(queueListStr) : [];

    // 添加任务 ID 到队列列表
    if (!queueList.includes(job.id)) {
      queueList.push(job.id);
      await this.client.set(queueKey, JSON.stringify(queueList));
    }
  }

  async getNext(queueName: string): Promise<Job | null> {
    if (!this.client) {
      throw new Error("Memcached 客户端未连接，请先调用 connect()");
    }

    const queueKey = this.getQueueKey(queueName);

    // 获取队列列表
    const queueListStr = await this.client.get(queueKey);
    if (!queueListStr) {
      return null;
    }

    const jobIds: string[] = JSON.parse(queueListStr);
    if (jobIds.length === 0) {
      return null;
    }

    const now = Date.now();
    let nextJob: Job | null = null;
    let nextJobId: string | null = null;

    // 性能优化：使用 getMulti 批量获取任务数据（如果支持）
    if (this.client.getMulti && jobIds.length > 1) {
      // 构建所有任务键名
      const jobKeys = jobIds.map((jobId) => this.getJobKey(jobId));

      // 批量获取所有任务数据
      const jobDataRecord = await this.client.getMulti(jobKeys);

      // 解析并查找下一个可执行的任务（考虑延迟和优先级）
      for (const jobId of jobIds) {
        const jobKey = this.getJobKey(jobId);
        const jobData = jobDataRecord[jobKey];
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
    } else {
      // 回退到单个获取（兼容不支持 getMulti 的客户端或单个任务的情况）
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
    }

    if (nextJob && nextJobId) {
      // 更新任务状态
      nextJob.status = "processing";
      nextJob.startedAt = now;
      const jobKey = this.getJobKey(nextJobId);
      await this.client.set(jobKey, JSON.stringify(nextJob));

      // 从队列列表中移除该任务 ID
      const updatedQueueList = jobIds.filter((id) => id !== nextJobId);
      await this.client.set(queueKey, JSON.stringify(updatedQueueList));

      return nextJob;
    }

    return null;
  }

  async update(jobId: string, updates: Partial<Job>): Promise<void> {
    if (!this.client) {
      throw new Error("Memcached 客户端未连接，请先调用 connect()");
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
      throw new Error("Memcached 客户端未连接，请先调用 connect()");
    }

    const jobKey = this.getJobKey(jobId);
    const jobData = await this.client.get(jobKey);
    if (!jobData) return null;
    return JSON.parse(jobData) as Job;
  }

  async remove(jobId: string): Promise<void> {
    if (!this.client) {
      throw new Error("Memcached 客户端未连接，请先调用 connect()");
    }

    // 删除任务数据
    const jobKey = this.getJobKey(jobId);
    await this.client.delete(jobKey);

    // 从队列列表中移除
    const queueName = this.getQueueName(jobId);
    const queueKey = this.getQueueKey(queueName);
    const queueListStr = await this.client.get(queueKey);
    if (queueListStr) {
      const queueList: string[] = JSON.parse(queueListStr);
      const updatedQueueList = queueList.filter((id) => id !== jobId);
      await this.client.set(queueKey, JSON.stringify(updatedQueueList));
    }
  }

  async getAll(queueName: string): Promise<Job[]> {
    if (!this.client) {
      throw new Error("Memcached 客户端未连接，请先调用 connect()");
    }

    const queueKey = this.getQueueKey(queueName);
    const queueListStr = await this.client.get(queueKey);
    if (!queueListStr) {
      return [];
    }

    const jobIds: string[] = JSON.parse(queueListStr);
    const jobs: Job[] = [];

    // 性能优化：使用 getMulti 批量获取任务数据（如果支持）
    if (this.client.getMulti && jobIds.length > 1) {
      const jobKeys = jobIds.map((jobId) => this.getJobKey(jobId));
      const jobDataRecord = await this.client.getMulti(jobKeys);

      for (const jobId of jobIds) {
        const jobKey = this.getJobKey(jobId);
        const jobData = jobDataRecord[jobKey];
        if (jobData) {
          try {
            const job = JSON.parse(jobData) as Job;
            jobs.push(job);
          } catch {
            // 忽略解析错误的任务
          }
        }
      }
    } else {
      // 回退到单个获取
      for (const jobId of jobIds) {
        const job = await this.get(jobId);
        if (job) {
          jobs.push(job);
        }
      }
    }

    return jobs;
  }

  async clear(queueName: string): Promise<void> {
    if (!this.client) {
      throw new Error("Memcached 客户端未连接，请先调用 connect()");
    }

    const queueKey = this.getQueueKey(queueName);
    const queueListStr = await this.client.get(queueKey);
    if (queueListStr) {
      const jobIds: string[] = JSON.parse(queueListStr);

      // 删除所有任务数据
      for (const jobId of jobIds) {
        const jobKey = this.getJobKey(jobId);
        await this.client.delete(jobKey);
      }
    }

    // 清空队列列表
    await this.client.delete(queueKey);
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
