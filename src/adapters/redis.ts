/**
 * @module @dreamer/queue/adapters/redis
 *
 * @fileoverview Redis 队列适配器
 *
 * 使用 Redis 作为任务存储后端，支持任务持久化和故障恢复。
 */

import type { Job, JobPriority, QueueAdapter } from "./base.ts";

/**
 * Redis 队列适配器配置
 */
export interface RedisAdapterOptions {
  /** Redis 客户端实例（需要用户自行安装和创建） */
  client: {
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
  };
  /** 键前缀（可选，默认：queue） */
  keyPrefix?: string;
}

/**
 * Redis 队列适配器（持久化）
 *
 * 使用 Redis 作为任务存储后端，支持任务持久化和故障恢复。
 *
 * 需要用户自行安装 Redis 客户端库，例如：
 * - npm:redis
 * - npm:ioredis
 *
 * @example
 * ```typescript
 * import { RedisQueueAdapter } from "jsr:@dreamer/queue/adapters";
 * import { createClient } from "npm:redis";
 *
 * const redisClient = createClient({ url: "redis://localhost:6379" });
 * await redisClient.connect();
 *
 * const adapter = new RedisQueueAdapter({ client: redisClient });
 * ```
 */
export class RedisQueueAdapter implements QueueAdapter {
  private client: RedisAdapterOptions["client"];
  private keyPrefix: string;

  constructor(options: RedisAdapterOptions) {
    this.client = options.client;
    this.keyPrefix = options.keyPrefix || "queue";
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
   */
  private getQueueName(jobId: string): string {
    return jobId.split("-")[0];
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
    // 存储任务数据
    const jobKey = this.getJobKey(job.id);
    await this.client.set(jobKey, JSON.stringify(job));

    // 添加到队列列表
    const queueName = this.getQueueName(job.id);
    const queueKey = this.getQueueKey(queueName);
    await this.client.lpush(queueKey, job.id);
  }

  async getNext(queueName: string): Promise<Job | null> {
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
    const jobKey = this.getJobKey(jobId);
    const jobData = await this.client.get(jobKey);
    if (jobData) {
      const job = JSON.parse(jobData) as Job;
      Object.assign(job, updates);
      await this.client.set(jobKey, JSON.stringify(job));
    }
  }

  async get(jobId: string): Promise<Job | null> {
    const jobKey = this.getJobKey(jobId);
    const jobData = await this.client.get(jobKey);
    if (!jobData) return null;
    return JSON.parse(jobData) as Job;
  }

  async remove(jobId: string): Promise<void> {
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
