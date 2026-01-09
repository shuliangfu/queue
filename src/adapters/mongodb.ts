/**
 * @module @dreamer/queue/adapters/mongodb
 *
 * @fileoverview MongoDB 队列适配器
 *
 * 使用 MongoDB 作为任务存储后端，支持任务持久化和故障恢复。
 */

import type { Job, JobPriority, QueueAdapter } from "./base.ts";

/**
 * MongoDB 连接配置
 */
export interface MongoDBConnectionConfig {
  /** MongoDB 连接 URL（例如：mongodb://127.0.0.1:27017） */
  url?: string;
  /** MongoDB 主机地址（默认：127.0.0.1） */
  host?: string;
  /** MongoDB 端口（默认：27017） */
  port?: number;
  /** 数据库名称（默认：queue） */
  database?: string;
  /** 用户名（可选） */
  username?: string;
  /** 密码（可选） */
  password?: string;
  /** 认证数据库（可选，默认：admin） */
  authSource?: string;
  /** 连接选项 */
  options?: {
    /** 连接超时时间（毫秒，默认：5000） */
    connectTimeoutMS?: number;
    /** Socket 超时时间（毫秒，默认：0，无超时） */
    socketTimeoutMS?: number;
    /** 最大连接池大小（默认：10） */
    maxPoolSize?: number;
    /** 最小连接池大小（默认：1） */
    minPoolSize?: number;
  };
}

/**
 * MongoDB 队列适配器配置
 */
export interface MongoDBAdapterOptions {
  /** MongoDB 连接配置（如果提供，适配器会内部创建连接） */
  connection?: MongoDBConnectionConfig;
  /** MongoDB 客户端实例（如果提供 connection，则不需要提供 client） */
  client?: {
    /** 获取数据库 */
    db(name?: string): {
      /** 获取集合 */
      collection(name: string): {
        /** 插入文档 */
        insertOne(doc: any): Promise<{ insertedId: any }>;
        /** 插入多个文档 */
        insertMany(docs: any[]): Promise<{ insertedIds: any }>;
        /** 查找一个文档 */
        findOne(filter: any): Promise<any | null>;
        /** 查找多个文档 */
        find(filter: any): {
          toArray(): Promise<any[]>;
          sort(sort: any): any;
          limit(n: number): any;
        };
        /** 更新一个文档 */
        updateOne(filter: any, update: any): Promise<{ modifiedCount: number }>;
        /** 更新多个文档 */
        updateMany(
          filter: any,
          update: any,
        ): Promise<{ modifiedCount: number }>;
        /** 删除一个文档 */
        deleteOne(filter: any): Promise<{ deletedCount: number }>;
        /** 删除多个文档 */
        deleteMany(filter: any): Promise<{ deletedCount: number }>;
        /** 统计文档数量 */
        countDocuments(filter?: any): Promise<number>;
        /** 创建索引 */
        createIndex(keys: any, options?: any): Promise<string>;
        /** 删除索引 */
        dropIndex(name: string): Promise<any>;
      };
    };
    /** 关闭连接 */
    close(): Promise<void>;
  };
  /** 集合名称（可选，默认：queues）。所有队列的任务都存储在同一个集合中，通过 queueName 字段区分 */
  collectionPrefix?: string;
  /** 数据库名称（可选，默认：queue） */
  databaseName?: string;
}

/**
 * MongoDB 队列适配器（持久化）
 *
 * 使用 MongoDB 作为任务存储后端，支持任务持久化和故障恢复。
 *
 * 所有队列的任务都存储在同一个集合中（默认：queues），通过 queueName 字段区分不同队列。
 * 这种设计简化了表管理，避免了为每个队列创建单独的表。
 *
 * 适配器会自动创建和管理 MongoDB 连接，用户只需提供连接参数。
 *
 * @example
 * ```typescript
 * import { MongoDBQueueAdapter } from "jsr:@dreamer/queue/adapters";
 *
 * // 方式1：使用连接配置（推荐）
 * const adapter = new MongoDBQueueAdapter({
 *   connection: { url: "mongodb://localhost:27017" }
 * });
 * await adapter.connect();
 *
 * // 方式2：使用已创建的客户端（兼容旧代码）
 * const adapter = new MongoDBQueueAdapter({ client: mongoClient });
 * ```
 */
export class MongoDBQueueAdapter implements QueueAdapter {
  private client: MongoDBAdapterOptions["client"];
  private collectionPrefix: string;
  private databaseName: string;
  private internalClient: any = null; // 内部创建的客户端
  private connectionConfig?: MongoDBConnectionConfig;

  constructor(options: MongoDBAdapterOptions) {
    if (options.connection) {
      // 如果提供了连接配置，保存配置，稍后创建连接
      this.connectionConfig = options.connection;
      this.collectionPrefix = options.collectionPrefix || "queues";
      this.databaseName = options.databaseName ||
        options.connection.database || "queue";
    } else if (options.client) {
      // 如果提供了客户端，直接使用
      this.client = options.client;
      this.collectionPrefix = options.collectionPrefix || "queues";
      this.databaseName = options.databaseName || "queue";
    } else {
      throw new Error(
        "MongoDBQueueAdapter 需要提供 connection 配置或 client 实例",
      );
    }
  }

  /**
   * 连接到 MongoDB（如果使用 connection 配置）
   */
  async connect(): Promise<void> {
    if (this.connectionConfig && !this.internalClient) {
      try {
        // 动态导入 MongoDB 客户端库
        // 在 Bun 中，直接使用包名；在 Deno 中，使用 deno.json 中配置的 imports map
        const isBun = typeof (globalThis as any).Bun !== "undefined";
        const mongoModule = isBun
          ? await import("mongodb")
          : await import("mongodb");

        const { MongoClient } = mongoModule;

        // 构建连接 URL
        let connectionUrl: string;
        if (this.connectionConfig.url) {
          connectionUrl = this.connectionConfig.url;
        } else {
          // 构建连接 URL
          const host = this.connectionConfig.host || "127.0.0.1";
          const port = this.connectionConfig.port || 27017;
          const auth =
            this.connectionConfig.username && this.connectionConfig.password
              ? `${this.connectionConfig.username}:${this.connectionConfig.password}@`
              : "";
          const authSource = this.connectionConfig.authSource
            ? `?authSource=${this.connectionConfig.authSource}`
            : "";
          connectionUrl = `mongodb://${auth}${host}:${port}/${authSource}`;
        }

        // 构建客户端选项
        const clientOptions: any = {
          ...this.connectionConfig.options,
        };

        // 创建并连接客户端
        this.internalClient = new MongoClient(connectionUrl, clientOptions);
        await this.internalClient.connect();

        // 使用内部客户端
        this.client = this.internalClient;

        // 创建索引以优化查询性能
        await this.createIndexes();
      } catch (error) {
        throw new Error(
          `无法创建 MongoDB 连接: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
  }

  /**
   * 断开 MongoDB 连接
   */
  async disconnect(): Promise<void> {
    if (this.internalClient) {
      try {
        await this.internalClient.close();
      } catch {
        // 忽略断开错误
      }
      this.internalClient = null;
      this.client = undefined;
    } else if (this.client?.close) {
      // 如果使用的是外部客户端，调用其 close 方法
      await this.client.close();
    }
  }

  /**
   * 获取集合名称
   * 使用固定的集合名称，所有队列的任务都存储在同一个集合中
   */
  private getCollectionName(): string {
    return this.collectionPrefix;
  }

  /**
   * 获取数据库
   */
  private getDatabase() {
    if (!this.client) {
      throw new Error("MongoDB 客户端未连接，请先调用 connect()");
    }
    return this.client.db(this.databaseName);
  }

  /**
   * 获取集合
   * 所有队列共享同一个集合，通过 queueName 字段区分
   */
  private getCollection() {
    const db = this.getDatabase();
    return db.collection(this.getCollectionName());
  }

  /**
   * 从任务 ID 提取队列名称
   * 任务 ID 格式：${queueName}-${timestamp}-${random}
   * 例如：test-mongodb-process-1234567890-abc123
   */
  private getQueueName(jobId: string): string {
    // 任务 ID 格式：queueName-timestamp-random
    // 需要提取第一个部分（队列名称可能包含连字符）
    // 但队列名称通常是最后一个包含连字符的部分，或者是第一个部分
    // 实际上，任务 ID 的格式是：${this.name}-${Date.now()}-${random}
    // 所以队列名称就是第一个连字符之前的部分
    const parts = jobId.split("-");
    // 如果队列名称本身包含连字符（如 "test-mongodb-process"），
    // 我们需要找到最后两个连字符之间的部分
    // 但更简单的方法是：队列名称是除了最后两个部分（timestamp 和 random）之外的所有部分
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

  /**
   * 创建索引以优化查询性能
   */
  private async createIndexes(): Promise<void> {
    // 在连接时创建全局索引
    await this.ensureIndexes();
  }

  /**
   * 确保集合索引存在
   * 创建包含 queueName 字段的复合索引，以优化查询性能
   */
  private async ensureIndexes(): Promise<void> {
    const collection = this.getCollection();
    try {
      // 创建复合索引：queueName + status + priority + createdAt（用于 getNext 查询）
      await collection.createIndex(
        { queueName: 1, status: 1, priority: -1, createdAt: 1 },
        { name: "queueName_status_priority_created" },
      );
      // 创建 ID 索引（用于 get 查询）
      await collection.createIndex(
        { id: 1 },
        { unique: true, name: "id_unique" },
      );
      // 创建 queueName 索引（用于 getAll、clear、getStats 查询）
      await collection.createIndex(
        { queueName: 1 },
        { name: "queueName_index" },
      );
    } catch {
      // 索引可能已存在，忽略错误
    }
  }

  async add(job: Job): Promise<void> {
    if (!this.client) {
      throw new Error("MongoDB 客户端未连接，请先调用 connect()");
    }
    const queueName = this.getQueueName(job.id);
    const collection = this.getCollection();

    // 确保索引存在
    await this.ensureIndexes();

    // 插入任务文档，添加 queueName 字段用于区分不同队列
    const jobWithQueueName = { ...job, queueName };
    await collection.insertOne(jobWithQueueName);
  }

  async getNext(queueName: string): Promise<Job | null> {
    if (!this.client) {
      throw new Error("MongoDB 客户端未连接，请先调用 connect()");
    }
    const collection = this.getCollection();

    // 确保索引存在
    await this.ensureIndexes();

    const now = Date.now();

    // 查找下一个可执行的任务（考虑延迟和优先级）
    // 查询条件：queueName 匹配 + status = "pending" 且 (delay 不存在 或 createdAt + delay <= now)
    const query: any = {
      queueName,
      status: "pending",
      $or: [
        { delay: { $exists: false } },
        { delay: null },
        {
          $expr: {
            $lte: [{ $add: ["$createdAt", { $ifNull: ["$delay", 0] }] }, now],
          },
        },
      ],
    };

    // 获取所有符合条件的任务，然后在内存中按优先级排序
    // 因为 MongoDB 的优先级是字符串，需要转换为数字排序
    const jobs = await collection.find(query).toArray();

    if (jobs.length === 0) {
      return null;
    }

    // 过滤掉延迟未到的任务
    const availableJobs = (jobs as Job[]).filter((job) => {
      if (!job.delay) return true;
      return job.createdAt + job.delay <= now;
    });

    if (availableJobs.length === 0) {
      return null;
    }

    // 按优先级和创建时间排序
    availableJobs.sort((a, b) => {
      const priorityDiff = this.comparePriority(b.priority, a.priority);
      if (priorityDiff !== 0) return priorityDiff;
      return a.createdAt - b.createdAt;
    });

    const nextJob = availableJobs[0];

    // 更新任务状态为 processing
    nextJob.status = "processing";
    nextJob.startedAt = now;
    await collection.updateOne(
      { id: nextJob.id, queueName },
      {
        $set: {
          status: "processing",
          startedAt: now,
        },
      },
    );

    // 返回时移除 queueName 字段（如果不需要的话，也可以保留）
    const { queueName: _, ...jobWithoutQueueName } = nextJob as any;
    return jobWithoutQueueName as Job;
  }

  async update(jobId: string, updates: Partial<Job>): Promise<void> {
    if (!this.client) {
      throw new Error("MongoDB 客户端未连接，请先调用 connect()");
    }
    const queueName = this.getQueueName(jobId);
    const collection = this.getCollection();

    // 更新时同时匹配 id 和 queueName，确保只更新对应队列的任务
    await collection.updateOne(
      { id: jobId, queueName },
      { $set: updates },
    );
  }

  async get(jobId: string): Promise<Job | null> {
    if (!this.client) {
      throw new Error("MongoDB 客户端未连接，请先调用 connect()");
    }
    const queueName = this.getQueueName(jobId);
    const collection = this.getCollection();

    // 查询时同时匹配 id 和 queueName，确保只返回对应队列的任务
    const job = await collection.findOne({ id: jobId, queueName });
    if (!job) {
      return null;
    }
    // 返回时移除 queueName 字段
    const { queueName: _, ...jobWithoutQueueName } = job as any;
    return jobWithoutQueueName as Job;
  }

  async remove(jobId: string): Promise<void> {
    if (!this.client) {
      throw new Error("MongoDB 客户端未连接，请先调用 connect()");
    }
    const queueName = this.getQueueName(jobId);
    const collection = this.getCollection();

    // 删除时同时匹配 id 和 queueName，确保只删除对应队列的任务
    await collection.deleteOne({ id: jobId, queueName });
  }

  async getAll(queueName: string): Promise<Job[]> {
    if (!this.client) {
      throw new Error("MongoDB 客户端未连接，请先调用 connect()");
    }
    const collection = this.getCollection();

    // 根据 queueName 查询对应队列的所有任务
    const jobs = await collection.find({ queueName }).toArray();
    // 返回时移除 queueName 字段
    return jobs.map((job: any) => {
      const { queueName: _, ...jobWithoutQueueName } = job;
      return jobWithoutQueueName as Job;
    });
  }

  async clear(queueName: string): Promise<void> {
    if (!this.client) {
      throw new Error("MongoDB 客户端未连接，请先调用 connect()");
    }
    const collection = this.getCollection();

    // 只删除指定队列的任务
    await collection.deleteMany({ queueName });
  }

  async getStats(queueName: string): Promise<{
    pending: number;
    processing: number;
    completed: number;
    failed: number;
  }> {
    if (!this.client) {
      throw new Error("MongoDB 客户端未连接，请先调用 connect()");
    }
    const collection = this.getCollection();

    // 统计指定队列的任务状态
    const [pending, processing, completed, failed] = await Promise.all([
      collection.countDocuments({ queueName, status: "pending" }),
      collection.countDocuments({ queueName, status: "processing" }),
      collection.countDocuments({ queueName, status: "completed" }),
      collection.countDocuments({ queueName, status: "failed" }),
    ]);

    return {
      pending,
      processing,
      completed,
      failed,
    };
  }
}
