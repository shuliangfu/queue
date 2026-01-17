/**
 * @fileoverview 延迟任务功能测试
 */

import { IS_DENO } from "@dreamer/runtime-adapter";
import { describe, expect, it } from "@dreamer/test";
import { MemoryQueueAdapter, QueueManager } from "../src/mod.ts";

describe("Queue > 延迟任务功能", () => {
  describe("延迟任务处理", () => {
    it("应该延迟执行任务", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      let processed = false;
      const startTime = Date.now();

      queue.process(async (job) => {
        processed = true;
        const elapsed = Date.now() - startTime;
        // 验证延迟时间（允许 100ms 误差）
        expect(elapsed).toBeGreaterThanOrEqual(500);
        expect(elapsed).toBeLessThan(1000);
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      // 添加延迟 500ms 的任务
      await queue.add("delayed-job", { data: "test" }, {
        delay: 500,
      });

      // 等待任务处理（应该延迟 500ms）
      const maxWaitTime = IS_DENO ? 3000 : 2000;
      const checkInterval = 100;
      let waited = 0;

      while (!processed && waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, checkInterval));
        waited += checkInterval;
      }

      expect(processed).toBeTruthy();

      queue.stop();
      if (IS_DENO && typeof queue.waitForTimers === "function") {
        await queue.waitForTimers();
      }
      await queueManager.close();
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
      timeout: 30000,
    });

    it("应该在延迟时间到达前不处理任务", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      let processed = false;

      queue.process(async () => {
        processed = true;
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      // 添加延迟 1000ms 的任务
      const job = await queue.add("delayed-job", { data: "test" }, {
        delay: 1000,
      });

      // 等待 500ms（应该还没处理）
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 验证任务还未处理
      expect(processed).toBeFalsy();

      // 验证任务状态
      const jobStatus = await queue.getJob(job.id);
      expect(jobStatus?.status).toBe("pending");

      // 等待剩余时间（增加一些缓冲时间，因为动态延迟可能导致处理稍慢）
      // 使用轮询方式检查任务是否被处理，最多等待 2 秒
      const maxWaitTime = IS_DENO ? 2000 : 2000;
      const checkInterval = 100;
      let waited = 0;

      while (!processed && waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, checkInterval));
        waited += checkInterval;
      }

      // 现在应该已经处理了
      expect(processed).toBeTruthy();

      queue.stop();
      if (IS_DENO && typeof queue.waitForTimers === "function") {
        await queue.waitForTimers();
      }
      await queueManager.close();
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });

    it("应该处理多个延迟任务", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      const processedJobs: string[] = [];

      queue.process(async (job) => {
        processedJobs.push(job.name);
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      // 添加多个不同延迟的任务
      await queue.add("job1", { data: "1" }, { delay: 200 });
      await queue.add("job2", { data: "2" }, { delay: 400 });
      await queue.add("job3", { data: "3" }, { delay: 600 });

      // 等待所有任务处理完成
      const maxWaitTime = IS_DENO ? 3000 : 2000;
      const checkInterval = 100;
      let waited = 0;

      while (processedJobs.length < 3 && waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, checkInterval));
        waited += checkInterval;
      }

      // 验证所有任务都已处理
      expect(processedJobs.length).toBe(3);
      expect(processedJobs).toContain("job1");
      expect(processedJobs).toContain("job2");
      expect(processedJobs).toContain("job3");

      queue.stop();
      if (IS_DENO && typeof queue.waitForTimers === "function") {
        await queue.waitForTimers();
      }
      await queueManager.close();
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });

    it("应该处理无延迟的任务（delay = 0 或未设置）", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      let processed = false;

      queue.process(async () => {
        processed = true;
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      // 添加无延迟的任务
      await queue.add("immediate-job", { data: "test" }); // 不设置 delay

      // 等待任务处理（应该立即处理）
      const maxWaitTime = IS_DENO ? 2000 : 1000;
      const checkInterval = 100;
      let waited = 0;

      while (!processed && waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, checkInterval));
        waited += checkInterval;
      }

      expect(processed).toBeTruthy();

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
});
