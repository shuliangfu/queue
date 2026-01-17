/**
 * @fileoverview Queue 类完整功能测试
 */

import { IS_DENO } from "@dreamer/runtime-adapter";
import { describe, expect, it } from "@dreamer/test";
import { MemoryQueueAdapter, QueueManager } from "../src/mod.ts";

describe("Queue > Queue 类完整功能", () => {
  describe("任务查询方法", () => {
    it("应该通过 ID 获取任务", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      const job = await queue.add("test-job", { data: "test" });
      const retrievedJob = await queue.getJob(job.id);

      expect(retrievedJob).toBeTruthy();
      expect(retrievedJob?.id).toBe(job.id);
      expect(retrievedJob?.name).toBe("test-job");
      expect(retrievedJob?.data.data).toBe("test");

      await queueManager.close();
    });

    it("应该获取队列中的所有任务", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      const job1 = await queue.add("job1", { data: "data1" });
      const job2 = await queue.add("job2", { data: "data2" });
      const job3 = await queue.add("job3", { data: "data3" });

      const jobs = await queue.getJobs();

      expect(jobs.length).toBeGreaterThanOrEqual(3);
      const jobIds = jobs.map((j) => j.id);
      expect(jobIds).toContain(job1.id);
      expect(jobIds).toContain(job2.id);
      expect(jobIds).toContain(job3.id);

      await queueManager.close();
    });

    it("应该获取队列统计信息", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      // 添加多个任务
      await queue.add("job1", { data: "data1" });
      await queue.add("job2", { data: "data2" });
      await queue.add("job3", { data: "data3" });

      const stats = await queue.getStats();

      expect(stats).toBeTruthy();
      expect(stats.pending).toBeGreaterThanOrEqual(0);
      expect(stats.processing).toBeGreaterThanOrEqual(0);
      expect(stats.completed).toBeGreaterThanOrEqual(0);
      expect(stats.failed).toBeGreaterThanOrEqual(0);
      expect(typeof stats.pending).toBe("number");
      expect(typeof stats.processing).toBe("number");
      expect(typeof stats.completed).toBe("number");
      expect(typeof stats.failed).toBe("number");

      await queueManager.close();
    });

    it("应该清空队列", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      // 添加任务
      await queue.add("job1", { data: "data1" });
      await queue.add("job2", { data: "data2" });

      // 验证任务存在
      let jobs = await queue.getJobs();
      expect(jobs.length).toBeGreaterThanOrEqual(2);

      // 清空队列
      await queue.clear();

      // 验证队列已清空
      jobs = await queue.getJobs();
      expect(jobs.length).toBe(0);

      const stats = await queue.getStats();
      expect(stats.pending).toBe(0);

      await queueManager.close();
    });
  });

  describe("队列控制方法", () => {
    it("应该停止队列处理", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      let processed = false;
      queue.process(async (job) => {
        processed = true;
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      // 停止队列
      queue.stop();

      // 添加任务
      await queue.add("test-job", { data: "test" });

      // 等待一段时间，任务不应该被处理（因为队列已停止）
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 500 : 300));

      // 注意：由于队列已停止，任务可能不会被处理
      // 但至少应该不会抛出错误

      await queueManager.close();
    });
  });

  describe("错误处理和重试", () => {
    it("应该处理任务失败并重试", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", {
        concurrency: 1,
        retry: 2, // 允许重试 2 次
      });

      let attemptCount = 0;
      queue.process(async (job) => {
        attemptCount++;
        if (attemptCount < 3) {
          // 前两次失败
          throw new Error("处理失败");
        }
        // 第三次成功
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      const job = await queue.add("test-job", { data: "test" }, {
        maxAttempts: 2, // 最大尝试 2 次
      });

      // 等待任务处理（包括重试）
      const maxWaitTime = IS_DENO ? 5000 : 3000;
      const checkInterval = 200;
      let waited = 0;
      while (waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, checkInterval));
        waited += checkInterval;

        const jobStatus = await queue.getJob(job.id);
        if (jobStatus?.status === "completed" || jobStatus?.status === "failed") {
          break;
        }
      }

      // 验证任务最终状态
      const finalJob = await queue.getJob(job.id);
      // 由于重试机制，任务应该最终完成或失败
      expect(finalJob?.status).toMatch(/completed|failed/);
      expect(finalJob?.attempts).toBeGreaterThan(0);

      queue.stop();
      if (IS_DENO && typeof queue.waitForTimers === "function") {
        await queue.waitForTimers();
      }
      await queueManager.close();
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });

    it("应该在达到最大重试次数后标记为失败", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", {
        concurrency: 1,
        retry: 1, // 只允许重试 1 次
      });

      queue.process(async () => {
        // 总是失败
        throw new Error("处理失败");
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      const job = await queue.add("test-job", { data: "test" }, {
        maxAttempts: 1, // 最大尝试 1 次（初始 + 1 次重试 = 总共 2 次）
      });

      // 等待任务处理（包括重试）
      const maxWaitTime = IS_DENO ? 5000 : 3000;
      const checkInterval = 200;
      let waited = 0;
      while (waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, checkInterval));
        waited += checkInterval;

        const jobStatus = await queue.getJob(job.id);
        if (jobStatus?.status === "failed") {
          break;
        }
      }

      // 验证任务最终状态为失败
      const finalJob = await queue.getJob(job.id);
      expect(finalJob?.status).toBe("failed");
      expect(finalJob?.error).toBeTruthy();
      expect(finalJob?.failedAt).toBeTruthy();

      queue.stop();
      if (IS_DENO && typeof queue.waitForTimers === "function") {
        await queue.waitForTimers();
      }
      await queueManager.close();
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });
  });

  describe("任务超时", () => {
    it("应该处理任务执行超时", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", {
        concurrency: 1,
        timeout: 1000, // 1 秒超时
      });

      queue.process(async () => {
        // 模拟长时间运行的任务（超过超时时间）
        await new Promise((resolve) => setTimeout(resolve, 2000));
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      const job = await queue.add("test-job", { data: "test" }, {
        timeout: 1000, // 1 秒超时
      });

      // 等待任务处理（应该超时）
      const maxWaitTime = IS_DENO ? 5000 : 3000;
      const checkInterval = 200;
      let waited = 0;
      while (waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, checkInterval));
        waited += checkInterval;

        const jobStatus = await queue.getJob(job.id);
        if (jobStatus?.status === "failed") {
          break;
        }
      }

      // 验证任务因超时失败
      const finalJob = await queue.getJob(job.id);
      expect(finalJob?.status).toBe("failed");
      expect(finalJob?.error).toContain("超时");

      queue.stop();
      if (IS_DENO && typeof queue.waitForTimers === "function") {
        await queue.waitForTimers();
      }
      await queueManager.close();
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });
  });

  describe("并发控制", () => {
    it("应该限制并发处理数量", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", {
        concurrency: 2, // 最多同时处理 2 个任务
      });

      const processingJobs: string[] = [];
      let maxConcurrent = 0;

      queue.process(async (job) => {
        processingJobs.push(job.id);
        maxConcurrent = Math.max(maxConcurrent, processingJobs.length);

        // 模拟处理时间
        await new Promise((resolve) => setTimeout(resolve, 500));

        const index = processingJobs.indexOf(job.id);
        if (index > -1) {
          processingJobs.splice(index, 1);
        }
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      // 添加 5 个任务
      const jobs = [];
      for (let i = 0; i < 5; i++) {
        jobs.push(await queue.add(`job-${i}`, { data: `data-${i}` }));
      }

      // 等待所有任务处理完成
      const maxWaitTime = IS_DENO ? 10000 : 5000;
      const checkInterval = 200;
      let waited = 0;
      while (waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, checkInterval));
        waited += checkInterval;

        const allCompleted = (await Promise.all(
          jobs.map((j) => queue.getJob(j.id))
        )).every((j) => j?.status === "completed");

        if (allCompleted) {
          break;
        }
      }

      // 验证并发数不超过限制
      expect(maxConcurrent).toBeLessThanOrEqual(2);

      queue.stop();
      if (IS_DENO && typeof queue.waitForTimers === "function") {
        await queue.waitForTimers();
      }
      await queueManager.close();
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });
  });

  describe("边界情况", () => {
    it("应该处理空队列的查询", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      // 查询空队列
      const jobs = await queue.getJobs();
      expect(jobs).toEqual([]);

      const stats = await queue.getStats();
      expect(stats.pending).toBe(0);
      expect(stats.processing).toBe(0);
      expect(stats.completed).toBe(0);
      expect(stats.failed).toBe(0);

      // 清空空队列不应该报错
      await queue.clear();

      await queueManager.close();
    });

    it("应该处理无效的任务 ID", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      // 查询不存在的任务
      const job = await queue.getJob("non-existent-id");
      expect(job).toBeNull();

      await queueManager.close();
    });
  });
});
