/**
 * @module @dreamer/queue/adapters/base
 *
 * @fileoverview 队列适配器基础接口和类型定义
 */

/**
 * 任务状态
 */
export type JobStatus = "pending" | "processing" | "completed" | "failed";

/**
 * 任务优先级
 */
export type JobPriority = "low" | "normal" | "high" | "urgent";

/**
 * 任务数据
 */
export interface JobData {
  [key: string]: unknown;
}

/**
 * 任务接口
 */
export interface Job {
  /** 任务 ID */
  id: string;
  /** 任务名称 */
  name: string;
  /** 任务数据 */
  data: JobData;
  /** 任务状态 */
  status: JobStatus;
  /** 任务优先级 */
  priority: JobPriority;
  /** 创建时间 */
  createdAt: number;
  /** 开始处理时间 */
  startedAt?: number;
  /** 完成时间 */
  completedAt?: number;
  /** 失败时间 */
  failedAt?: number;
  /** 重试次数 */
  attempts: number;
  /** 最大重试次数 */
  maxAttempts: number;
  /** 错误信息 */
  error?: string;
  /** 延迟执行时间（毫秒） */
  delay?: number;
  /** 超时时间（毫秒） */
  timeout?: number;
}

/**
 * 队列适配器接口
 */
export interface QueueAdapter {
  /**
   * 添加任务
   */
  add(job: Job): Promise<void>;

  /**
   * 获取下一个待处理的任务
   */
  getNext(queueName: string): Promise<Job | null>;

  /**
   * 更新任务状态
   */
  update(jobId: string, updates: Partial<Job>): Promise<void>;

  /**
   * 获取任务
   */
  get(jobId: string): Promise<Job | null>;

  /**
   * 删除任务
   */
  remove(jobId: string): Promise<void>;

  /**
   * 获取队列中的所有任务
   */
  getAll(queueName: string): Promise<Job[]>;

  /**
   * 清空队列
   */
  clear(queueName: string): Promise<void>;

  /**
   * 获取队列统计信息
   */
  getStats(queueName: string): Promise<{
    pending: number;
    processing: number;
    completed: number;
    failed: number;
  }>;
}
