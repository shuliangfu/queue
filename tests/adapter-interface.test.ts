/**
 * @fileoverview 适配器接口完整功能测试
 */

import { beforeEach, describe, expect, it } from "@dreamer/test";
import { type Job, MemoryQueueAdapter } from "../src/mod.ts";

describe("Queue > 适配器接口完整功能", () => {
  describe("MemoryQueueAdapter 接口方法", () => {
    let adapter: MemoryQueueAdapter;

    beforeEach(() => {
      adapter = new MemoryQueueAdapter();
    });

    it("应该更新任务状态", async () => {
      // 现在支持包含 "-" 的队列名称
      const queueName = "test-queue";
      // MemoryQueueAdapter 从 job.id 中提取队列名称（格式：queueName.timestamp.random）
      const timestamp = Date.now();
      const random = Math.random().toString(36).substring(7);
      const job: Job = {
        id: `${queueName}.${timestamp}.${random}`,
        name: "test-job",
        data: { test: "data" },
        status: "pending",
        priority: "normal",
        createdAt: timestamp,
        attempts: 0,
        maxAttempts: 3,
      };

      await adapter.add(job);

      // 更新任务状态
      const startedAt = Date.now();
      await adapter.update(job.id, {
        status: "processing",
        startedAt: startedAt,
      });

      const updatedJob = await adapter.get(job.id);
      expect(updatedJob?.status).toBe("processing");
      expect(updatedJob?.startedAt).toBe(startedAt);
    });

    it("应该删除任务", async () => {
      // 现在支持包含 "-" 的队列名称
      const queueName = "test-queue";
      const timestamp = Date.now();
      const random = Math.random().toString(36).substring(7);
      const job: Job = {
        id: `${queueName}.${timestamp}.${random}`,
        name: "test-job",
        data: { test: "data" },
        status: "pending",
        priority: "normal",
        createdAt: timestamp,
        attempts: 0,
        maxAttempts: 3,
      };

      await adapter.add(job);

      // 验证任务存在
      const jobBefore = await adapter.get(job.id);
      expect(jobBefore).toBeTruthy();

      // 删除任务
      await adapter.remove(job.id);

      // 验证任务已删除
      const jobAfter = await adapter.get(job.id);
      expect(jobAfter).toBeNull();
    });

    it("应该获取队列中的所有任务", async () => {
      // 现在支持包含 "-" 的队列名称
      const queueName = "test-queue";

      const timestamp1 = Date.now();
      const timestamp2 = timestamp1 + 1;
      const timestamp3 = timestamp1 + 2;
      const random1 = Math.random().toString(36).substring(7);
      const random2 = Math.random().toString(36).substring(7);
      const random3 = Math.random().toString(36).substring(7);

      const job1: Job = {
        id: `${queueName}.${timestamp1}.${random1}`,
        name: "job1",
        data: { test: "data1" },
        status: "pending",
        priority: "normal",
        createdAt: timestamp1,
        attempts: 0,
        maxAttempts: 3,
      };

      const job2: Job = {
        id: `${queueName}.${timestamp2}.${random2}`,
        name: "job2",
        data: { test: "data2" },
        status: "pending",
        priority: "normal",
        createdAt: timestamp2,
        attempts: 0,
        maxAttempts: 3,
      };

      const job3: Job = {
        id: `${queueName}.${timestamp3}.${random3}`,
        name: "job3",
        data: { test: "data3" },
        status: "completed",
        priority: "normal",
        createdAt: timestamp3,
        completedAt: timestamp3,
        attempts: 0,
        maxAttempts: 3,
      };

      await adapter.add(job1);
      await adapter.add(job2);
      await adapter.add(job3);

      const allJobs = await adapter.getAll(queueName);

      // 验证获取到所有任务
      expect(allJobs.length).toBeGreaterThanOrEqual(3);
      const jobIds = allJobs.map((j) => j.id);
      expect(jobIds).toContain(job1.id);
      expect(jobIds).toContain(job2.id);
      expect(jobIds).toContain(job3.id);
    });

    it("应该清空队列", async () => {
      // 现在支持包含 "-" 的队列名称
      const queueName = "test-queue";

      const timestamp1 = Date.now();
      const timestamp2 = timestamp1 + 1;
      const random1 = Math.random().toString(36).substring(7);
      const random2 = Math.random().toString(36).substring(7);

      const job1: Job = {
        id: `${queueName}.${timestamp1}.${random1}`,
        name: "job1",
        data: { test: "data1" },
        status: "pending",
        priority: "normal",
        createdAt: timestamp1,
        attempts: 0,
        maxAttempts: 3,
      };

      const job2: Job = {
        id: `${queueName}.${timestamp2}.${random2}`,
        name: "job2",
        data: { test: "data2" },
        status: "pending",
        priority: "normal",
        createdAt: timestamp2,
        attempts: 0,
        maxAttempts: 3,
      };

      await adapter.add(job1);
      await adapter.add(job2);

      // 验证任务已添加
      let allJobs = await adapter.getAll(queueName);
      expect(allJobs.length).toBeGreaterThanOrEqual(2);

      // 清空队列
      await adapter.clear(queueName);

      // 验证队列已清空
      allJobs = await adapter.getAll(queueName);
      expect(allJobs.length).toBe(0);
    });

    it("应该获取队列统计信息", async () => {
      // 现在支持包含 "-" 的队列名称
      const queueName = "test-queue";

      const timestamp = Date.now();
      const random1 = Math.random().toString(36).substring(7);
      const random2 = Math.random().toString(36).substring(7);
      const random3 = Math.random().toString(36).substring(7);
      const random4 = Math.random().toString(36).substring(7);

      const pendingJob: Job = {
        id: `${queueName}.${timestamp}.${random1}`,
        name: "pending",
        data: {},
        status: "pending",
        priority: "normal",
        createdAt: timestamp,
        attempts: 0,
        maxAttempts: 3,
      };

      const processingJob: Job = {
        id: `${queueName}.${timestamp + 1}.${random2}`,
        name: "processing",
        data: {},
        status: "processing",
        priority: "normal",
        createdAt: timestamp + 1,
        startedAt: timestamp + 1,
        attempts: 0,
        maxAttempts: 3,
      };

      const completedJob: Job = {
        id: `${queueName}.${timestamp + 2}.${random3}`,
        name: "completed",
        data: {},
        status: "completed",
        priority: "normal",
        createdAt: timestamp + 2,
        completedAt: timestamp + 2,
        attempts: 0,
        maxAttempts: 3,
      };

      const failedJob: Job = {
        id: `${queueName}.${timestamp + 3}.${random4}`,
        name: "failed",
        data: {},
        status: "failed",
        priority: "normal",
        createdAt: timestamp + 3,
        failedAt: timestamp + 3,
        attempts: 3,
        maxAttempts: 3,
        error: "处理失败",
      };

      await adapter.add(pendingJob);
      await adapter.add(processingJob);
      await adapter.add(completedJob);
      await adapter.add(failedJob);

      const stats = await adapter.getStats(queueName);

      expect(stats).toBeTruthy();
      expect(typeof stats.pending).toBe("number");
      expect(typeof stats.processing).toBe("number");
      expect(typeof stats.completed).toBe("number");
      expect(typeof stats.failed).toBe("number");
      expect(stats.pending).toBeGreaterThanOrEqual(1); // 至少有一个 pending
      expect(stats.processing).toBeGreaterThanOrEqual(1); // 至少有一个 processing
      expect(stats.completed).toBeGreaterThanOrEqual(1); // 至少有一个 completed
      expect(stats.failed).toBeGreaterThanOrEqual(1); // 至少有一个 failed
    });

    it("应该处理不存在的任务", async () => {
      // 更新不存在的任务不应该报错
      await adapter.update("non-existent-id", {
        status: "completed",
      });

      // 删除不存在的任务不应该报错
      await adapter.remove("non-existent-id");

      // 获取不存在的任务应该返回 null
      const job = await adapter.get("non-existent-id");
      expect(job).toBeNull();
    });

    it("应该处理空队列的统计信息", async () => {
      const queueName = "empty-queue";

      const stats = await adapter.getStats(queueName);

      expect(stats).toBeTruthy();
      expect(stats.pending).toBe(0);
      expect(stats.processing).toBe(0);
      expect(stats.completed).toBe(0);
      expect(stats.failed).toBe(0);
    });
  });
});
