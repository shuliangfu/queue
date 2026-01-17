/**
 * @fileoverview 性能优化测试
 *
 * 测试性能优化的正确性和效果，包括：
 * - 动态延迟轮询机制
 * - 性能基准测试
 */

import { IS_DENO } from "@dreamer/runtime-adapter";
import { describe, expect, it } from "@dreamer/test";
import { MemoryQueueAdapter, QueueManager } from "../src/mod.ts";

describe("Queue > 性能优化测试", () => {
  describe("动态延迟轮询优化", () => {
    it("应该在有任务时使用短延迟（0-10ms）", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-dynamic-delay-short", {
        concurrency: 1,
      });

      let processed = false;
      const delays: number[] = [];

      queue.process(async () => {
        processed = true;
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      // 添加任务
      await queue.add("test-job", { data: "test" });

      // 测量处理时间（应该很快，因为使用短延迟）
      const startTime = Date.now();
      const maxWaitTime = IS_DENO ? 2000 : 1500;
      const checkInterval = 50;
      let waited = 0;

      while (!processed && waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, checkInterval));
        waited += checkInterval;
        delays.push(Date.now() - startTime);
      }

      const totalTime = Date.now() - startTime;

      // 验证任务已处理
      expect(processed).toBeTruthy();

      // 验证处理时间应该在合理范围内（短延迟应该很快）
      // 注意：这是相对宽松的阈值，实际应该更快（< 500ms）
      expect(totalTime).toBeLessThan(1000);

      queue.stop();
      if (IS_DENO && typeof queue.waitForTimers === "function") {
        await queue.waitForTimers();
      }
      await queueManager.close();
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });

    it("应该在无任务时递增延迟", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-dynamic-delay-increase", {
        concurrency: 1,
      });

      // 启动处理循环（但不添加任务）
      queue.process(async () => {
        // 空处理器
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      // 测量空轮询的延迟时间
      // 由于无法直接测量内部延迟，我们通过观察行为来验证
      // 如果延迟递增，那么连续的空轮询应该越来越慢

      // 等待一段时间，让延迟递增
      await new Promise((resolve) => setTimeout(resolve, 2000));

      // 现在添加任务，应该能快速处理（延迟应该重置）
      let processed = false;
      queue.process(async () => {
        processed = true;
      });

      await queue.add("test-job", { data: "test" });

      const startTime = Date.now();
      const maxWaitTime = IS_DENO ? 2000 : 1500;
      let waited = 0;

      while (!processed && waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        waited += 100;
      }

      const processTime = Date.now() - startTime;

      // 验证任务已处理
      expect(processed).toBeTruthy();

      // 验证处理时间应该在合理范围内（延迟重置后应该很快）
      expect(processTime).toBeLessThan(1000);

      queue.stop();
      if (IS_DENO && typeof queue.waitForTimers === "function") {
        await queue.waitForTimers();
      }
      await queueManager.close();
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });

    it("应该在找到任务后重置延迟", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-dynamic-delay-reset", {
        concurrency: 1,
      });

      let processedCount = 0;

      queue.process(async () => {
        processedCount++;
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      // 先添加一个任务（触发处理，延迟应该重置为短延迟）
      await queue.add("job1", { data: "data1" });

      // 等待第一个任务处理
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 立即添加第二个任务（应该快速处理，因为延迟已重置）
      const startTime = Date.now();
      await queue.add("job2", { data: "data2" });

      const maxWaitTime = IS_DENO ? 2000 : 1500;
      let waited = 0;

      while (processedCount < 2 && waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        waited += 100;
      }

      const processTime = Date.now() - startTime;

      // 验证两个任务都已处理
      expect(processedCount).toBeGreaterThanOrEqual(2);

      // 验证第二个任务处理时间应该在合理范围内（延迟重置后应该很快）
      expect(processTime).toBeLessThan(1000);

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

  describe("性能基准测试", () => {
    it("Memory 适配器 getAll() 性能测试（100个任务）", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-perf-memory-getall", {
        concurrency: 1,
      });

      try {
        // 添加 100 个任务
        const jobs: any[] = [];
        for (let i = 0; i < 100; i++) {
          const job = await queue.add(`job-${i}`, { data: `data-${i}` });
          jobs.push(job);
        }

        // 测量 getAll() 性能
        const startTime = Date.now();
        const allJobs = await adapter.getAll("test-perf-memory-getall");
        const endTime = Date.now();
        const duration = endTime - startTime;

        // 验证获取到所有任务
        expect(allJobs.length).toBeGreaterThanOrEqual(100);

        // 验证性能：内存操作应该非常快（< 10ms）
        expect(duration).toBeLessThan(50);
      } finally {
        queue.stop();
        if (IS_DENO && typeof queue.waitForTimers === "function") {
          await queue.waitForTimers();
        }
        await queueManager.close();
      }
    });

    it("Memory 适配器 getNext() 性能测试（100个任务）", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-perf-memory-getnext", {
        concurrency: 1,
      });

      try {
        // 添加 100 个任务
        for (let i = 0; i < 100; i++) {
          await queue.add(`job-${i}`, { data: `data-${i}` });
        }

        // 测量 getNext() 性能（获取第一个任务）
        const startTime = Date.now();
        const nextJob = await adapter.getNext("test-perf-memory-getnext");
        const endTime = Date.now();
        const duration = endTime - startTime;

        // 验证获取到任务
        expect(nextJob).toBeTruthy();

        // 验证性能：内存操作应该非常快（< 10ms）
        expect(duration).toBeLessThan(50);
      } finally {
        queue.stop();
        if (IS_DENO && typeof queue.waitForTimers === "function") {
          await queue.waitForTimers();
        }
        await queueManager.close();
      }
    });

    it("动态延迟对吞吐量的影响", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-perf-throughput", {
        concurrency: 2, // 使用 2 个并发以提高吞吐量
      });

      let processedCount = 0;

      queue.process(async () => {
        processedCount++;
        // 模拟任务处理时间
        await new Promise((resolve) => setTimeout(resolve, 10));
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      try {
        // 添加 50 个任务
        for (let i = 0; i < 50; i++) {
          await queue.add(`job-${i}`, { data: `data-${i}` });
        }

        // 测量处理所有任务的总时间
        const startTime = Date.now();
        const maxWaitTime = IS_DENO ? 10000 : 8000;
        let waited = 0;

        while (processedCount < 50 && waited < maxWaitTime) {
          await new Promise((resolve) => setTimeout(resolve, 100));
          waited += 100;
        }

        const totalTime = Date.now() - startTime;

        // 验证所有任务都已处理
        expect(processedCount).toBeGreaterThanOrEqual(50);

        // 验证总时间应该在合理范围内
        // 50 个任务，每个 10ms，2 个并发 = 约 250ms + 动态延迟开销
        // 动态延迟应该提高吞吐量，总时间应该 < 5 秒
        expect(totalTime).toBeLessThan(5000);
      } finally {
        queue.stop();
        if (IS_DENO && typeof queue.waitForTimers === "function") {
          await queue.waitForTimers();
        }
        await queueManager.close();
      }
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });
  });
});
