/**
 * @module @dreamer/queue/adapters/memory
 *
 * @fileoverview 内存队列适配器
 *
 * ⚠️ 警告：内存适配器在应用重启后会丢失所有任务数据！
 * 生产环境请使用 RedisQueueAdapter 或 RabbitMQQueueAdapter。
 */

import type { Job, JobPriority, QueueAdapter } from "./base.ts";

/**
 * 内存队列适配器（仅用于测试，不推荐生产环境使用）
 *
 * ⚠️ 警告：内存适配器在应用重启后会丢失所有任务数据！
 * 生产环境请使用 RedisQueueAdapter 或 RabbitMQQueueAdapter。
 */
export class MemoryQueueAdapter implements QueueAdapter {
  private jobs: Map<string, Job> = new Map();
  private queues: Map<string, Set<string>> = new Map();
  // 任务 ID 到队列名称的映射
  private jobToQueue: Map<string, string> = new Map();

  async add(job: Job): Promise<void> {
    await Promise.resolve();
    this.jobs.set(job.id, job);
    // 从任务 ID 中提取队列名称（格式：queueName-timestamp-random）
    const queueName = job.id.split("-")[0];
    if (!this.queues.has(queueName)) {
      this.queues.set(queueName, new Set());
    }
    this.queues.get(queueName)!.add(job.id);
    this.jobToQueue.set(job.id, queueName);
  }

  async getNext(queueName: string): Promise<Job | null> {
    await Promise.resolve();
    const queue = this.queues.get(queueName);
    if (!queue || queue.size === 0) {
      return null;
    }

    const now = Date.now();
    let nextJob: Job | null = null;
    let nextJobId: string | null = null;

    // 查找下一个可执行的任务（考虑延迟和优先级）
    for (const jobId of queue) {
      const job = this.jobs.get(jobId);
      if (!job || job.status !== "pending") {
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
      nextJob.status = "processing";
      nextJob.startedAt = now;
      return nextJob;
    }

    return null;
  }

  async update(jobId: string, updates: Partial<Job>): Promise<void> {
    await Promise.resolve();
    const job = this.jobs.get(jobId);
    if (job) {
      Object.assign(job, updates);
      this.jobs.set(jobId, job);
    }
  }

  async get(jobId: string): Promise<Job | null> {
    await Promise.resolve();
    return this.jobs.get(jobId) || null;
  }

  async remove(jobId: string): Promise<void> {
    await Promise.resolve();
    const queueName = this.jobToQueue.get(jobId);
    if (queueName) {
      const queue = this.queues.get(queueName);
      if (queue) {
        queue.delete(jobId);
      }
      this.jobToQueue.delete(jobId);
    }
    this.jobs.delete(jobId);
  }

  async getAll(queueName: string): Promise<Job[]> {
    await Promise.resolve();
    const queue = this.queues.get(queueName);
    if (!queue) {
      return [];
    }

    const jobs: Job[] = [];
    for (const jobId of queue) {
      const job = this.jobs.get(jobId);
      if (job) {
        jobs.push(job);
      }
    }
    return jobs;
  }

  async clear(queueName: string): Promise<void> {
    await Promise.resolve();
    const queue = this.queues.get(queueName);
    if (queue) {
      for (const jobId of queue) {
        this.jobs.delete(jobId);
      }
      queue.clear();
    }
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
}
