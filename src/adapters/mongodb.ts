/**
 * @module @dreamer/queue/adapters/mongodb
 *
 * @fileoverview MongoDB 队列适配器
 *
 * 使用 MongoDB 作为任务存储后端，支持任务持久化和故障恢复。
 */

import { MongoClient } from "mongodb";
import type { Job, JobPriority, QueueAdapter } from "./base.ts";
import { $tr } from "../i18n.ts";

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
    /** 副本集名称（可选，用于单节点副本集） */
    replicaSet?: string;
    /** 是否直接连接（默认：false，设置为 true 可减少连接时间） */
    directConnection?: boolean;
    /** 服务器选择超时时间（毫秒） */
    serverSelectionTimeoutMS?: number;
    /** 其他 MongoDB 客户端选项 */
    [key: string]: unknown;
  };
}

/**
 * MongoDB 集合接口（用于队列适配器）
 */
export interface MongoDBQueueCollection {
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
  /** 查找并更新一个文档（原子操作） */
  findOneAndUpdate(
    filter: any,
    update: any,
    options?: { returnDocument?: "before" | "after" },
  ): Promise<any | null>;
  /** 更新多个文档 */
  updateMany(
    filter: any,
    update: any,
  ): Promise<{ modifiedCount: number }>;
  /** 聚合管道 */
  aggregate(pipeline: any[]): {
    toArray(): Promise<any[]>;
  };
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
}

/**
 * MongoDB 数据库接口（用于队列适配器）
 */
export interface MongoDBQueueDatabase {
  /** 获取集合 */
  collection(name: string): MongoDBQueueCollection;
}

/**
 * MongoDB 客户端接口（用于队列适配器）
 *
 * 此类型定义了队列适配器所需的 MongoDB 客户端接口，可以在框架中直接使用。
 */
export interface MongoDBQueueClient {
  /** 获取数据库 */
  db(name?: string): MongoDBQueueDatabase;
  /** 关闭连接 */
  close(): Promise<void>;
}

/**
 * MongoDB 队列适配器配置
 */
export interface MongoDBAdapterOptions {
  /** MongoDB 连接配置（如果提供，适配器会内部创建连接） */
  connection?: MongoDBConnectionConfig;
  /** MongoDB 客户端实例（如果提供 connection，则不需要提供 client） */
  client?: MongoDBQueueClient;
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
      throw new Error($tr("errors.mongodbConfigRequired"));
    }
  }

  /**
   * 连接到 MongoDB（如果使用 connection 配置）
   */
  async connect(): Promise<void> {
    if (this.connectionConfig && !this.internalClient) {
      try {
        // 使用静态导入的 MongoClient（已在文件顶部导入）
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
        const message = error instanceof Error ? error.message : String(error);
        throw new Error($tr("errors.mongodbConnectFailed", { message }));
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
      throw new Error($tr("errors.mongodbNotConnected"));
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
   * 任务 ID 格式：${queueName}.${timestamp}.${random}
   * 例如：test-mongodb-process.1234567890.abc123
   */
  private getQueueName(jobId: string): string {
    // 任务 ID 格式：queueName.timestamp.random
    // 需要提取第一个部分（队列名称可能包含点号）
    // 实际上，任务 ID 的格式是：${this.name}.${Date.now()}.${random}
    // 所以队列名称是除了最后两个部分（timestamp 和 random）之外的所有部分
    const parts = jobId.split(".");
    // 如果队列名称本身包含点号（如 "test.mongodb.process"），
    // 我们需要找到最后两个点号之间的部分
    // 但更简单的方法是：队列名称是除了最后两个部分（timestamp 和 random）之外的所有部分
    if (parts.length >= 3) {
      // 队列名称是除了最后两个部分之外的所有部分
      return parts.slice(0, -2).join(".");
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
      // 注意：priority 字段在聚合管道中转换为 priorityValue 进行排序，但索引仍然有助于过滤
      await collection.createIndex(
        { queueName: 1, status: 1, priority: -1, createdAt: 1 },
        { name: "queueName_status_priority_created" },
      );
      // 创建复合索引：queueName + status + createdAt（用于延迟任务过滤）
      await collection.createIndex(
        { queueName: 1, status: 1, createdAt: 1 },
        { name: "queueName_status_created" },
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
      throw new Error($tr("errors.mongodbNotConnected"));
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
      throw new Error($tr("errors.mongodbNotConnected"));
    }
    const collection = this.getCollection();

    // 确保索引存在
    await this.ensureIndexes();

    const now = Date.now();

    // 性能优化：使用聚合管道在数据库层面排序和限制，避免在内存中处理大量数据
    // 优先级映射：low=1, normal=2, high=3, urgent=4（在聚合管道中使用 $switch 实现）
    const _priorityMap: Record<string, number> = {
      low: 1,
      normal: 2,
      high: 3,
      urgent: 4,
    };

    // 使用聚合管道查找下一个可执行的任务
    const pipeline = [
      // 匹配条件：queueName + status = "pending"
      {
        $match: {
          queueName,
          status: "pending",
        },
      },
      // 添加计算字段：优先级数值和延迟到期时间
      {
        $addFields: {
          // 将优先级字符串转换为数字
          priorityValue: {
            $switch: {
              branches: [
                { case: { $eq: ["$priority", "low"] }, then: 1 },
                { case: { $eq: ["$priority", "normal"] }, then: 2 },
                { case: { $eq: ["$priority", "high"] }, then: 3 },
                { case: { $eq: ["$priority", "urgent"] }, then: 4 },
              ],
              default: 2, // 默认 normal
            },
          },
          // 计算延迟到期时间
          delayExpiry: {
            $cond: {
              if: {
                $and: [{ $ne: ["$delay", null] }, {
                  $ne: ["$delay", undefined],
                }],
              },
              then: { $add: ["$createdAt", "$delay"] },
              else: 0, // 无延迟任务，立即可用
            },
          },
        },
      },
      // 过滤延迟未到的任务
      {
        $match: {
          $expr: {
            $or: [
              { $eq: ["$delayExpiry", 0] }, // 无延迟任务
              { $lte: ["$delayExpiry", now] }, // 延迟已到期
            ],
          },
        },
      },
      // 按优先级（降序）和创建时间（升序）排序
      {
        $sort: {
          priorityValue: -1, // 高优先级优先
          createdAt: 1, // 相同优先级按创建时间排序
        },
      },
      // 只取第一个任务
      {
        $limit: 1,
      },
    ];

    // 执行聚合管道
    const results = await collection.aggregate(pipeline).toArray();

    if (results.length === 0) {
      return null;
    }

    const nextJob = results[0] as any;

    // 移除聚合管道添加的临时字段
    delete nextJob.priorityValue;
    delete nextJob.delayExpiry;

    // 更新任务状态为 processing（使用 findOneAndUpdate 原子操作）
    const updatedJob = await collection.findOneAndUpdate(
      { id: nextJob.id, queueName, status: "pending" }, // 确保状态仍然是 pending（防止并发）
      {
        $set: {
          status: "processing",
          startedAt: now,
        },
      },
      { returnDocument: "after" }, // 返回更新后的文档
    );

    // 如果更新失败（可能被其他进程获取），返回 null
    if (!updatedJob) {
      return null;
    }

    // 返回时移除 queueName 字段（如果不需要的话，也可以保留）
    const { queueName: _, ...jobWithoutQueueName } = updatedJob as any;
    return jobWithoutQueueName as Job;
  }

  async update(jobId: string, updates: Partial<Job>): Promise<void> {
    if (!this.client) {
      throw new Error($tr("errors.mongodbNotConnected"));
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
      throw new Error($tr("errors.mongodbNotConnected"));
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
      throw new Error($tr("errors.mongodbNotConnected"));
    }
    const queueName = this.getQueueName(jobId);
    const collection = this.getCollection();

    // 删除时同时匹配 id 和 queueName，确保只删除对应队列的任务
    await collection.deleteOne({ id: jobId, queueName });
  }

  async getAll(queueName: string): Promise<Job[]> {
    if (!this.client) {
      throw new Error($tr("errors.mongodbNotConnected"));
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
      throw new Error($tr("errors.mongodbNotConnected"));
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
      throw new Error($tr("errors.mongodbNotConnected"));
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
