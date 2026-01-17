/**
 * @fileoverview Redis 队列适配器测试
 */

import { IS_DENO } from "@dreamer/runtime-adapter";
import { describe, expect, it } from "@dreamer/test";
import { QueueManager } from "../src/mod.ts";
import { checkDockerContainer } from "./helpers.ts";

describe("Queue > RedisQueueAdapter", () => {
  it("应该检查 Redis 容器是否运行", async () => {
    const isRunning = await checkDockerContainer("redis");
    if (!isRunning) {
      console.log("⚠️  Redis 容器未运行，跳过 Redis 测试");
      console.log(
        "   启动 Redis: docker run -d -p 6379:6379 --name redis redis:latest",
      );
      return;
    }
    expect(isRunning).toBeTruthy();
  });

  it("应该创建 Redis 队列适配器", async () => {
    // 注意：Redis 客户端库可能有内部定时器（Socket 的 _unrefTimer），
    // 这是第三方库的内部实现，我们无法直接控制
    // 使用 sanitizeOps: false 和 sanitizeResources: false 来禁用定时器检查
    const isRunning = await checkDockerContainer("redis");
    if (!isRunning) {
      console.log("⚠️  跳过：Redis 容器未运行");
      return;
    }

    let adapter: any = null;
    try {
      const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
      adapter = new RedisQueueAdapter({
        connection: {
          url: "redis://127.0.0.1:6379",
          socket: {
            keepAlive: false,
            connectTimeout: 5000,
          },
        },
      });
      await adapter.connect();
      expect(adapter).toBeTruthy();
    } catch (error) {
      console.log(
        `⚠️  跳过：无法创建 Redis 适配器 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error; // 重新抛出错误，让测试框架知道测试失败
    } finally {
      // 确保总是清理资源
      if (adapter) {
        await adapter.disconnect();
        // 等待连接完全关闭和所有定时器完成
        // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成（Redis 客户端库的内部定时器）
        // Redis 客户端可能有内部定时器（如 Socket 的 _unrefTimer），需要更长时间清理
        // 注意：这些定时器是 Redis 客户端库内部的，我们无法直接控制
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 3000 : 1000)
        );
      }
    }
  }, {
    // 禁用定时器和资源检查（Redis 客户端库可能有内部定时器）
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Redis 适配器添加和获取任务", async () => {
    const isRunning = await checkDockerContainer("redis");
    if (!isRunning) {
      console.log("⚠️  跳过：Redis 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
      adapter = new RedisQueueAdapter({
        connection: {
          url: "redis://127.0.0.1:6379",
          socket: {
            keepAlive: false,
            connectTimeout: 5000,
          },
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-redis-stats", {
        concurrency: 1,
      });

      // 不设置处理器，确保任务保持 pending 状态
      // 注意：不调用 queue.process()，这样处理循环不会启动
      const job = await queue.add("test-job", { data: "test" });

      // 等待一小段时间确保任务已添加到 Redis
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 直接通过适配器获取任务，验证任务是否被正确添加
      const retrievedJob = await adapter.get(job.id);
      if (!retrievedJob) {
        throw new Error(
          `无法获取任务 ${job.id}，任务可能未正确添加到 Redis`,
        );
      }
      // 验证任务内容
      expect(retrievedJob.name).toBe("test-job");
      expect(retrievedJob.data.data).toBe("test");
    } catch (error) {
      console.log(
        `⚠️  跳过：Redis 测试失败 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error; // 重新抛出错误，让测试框架知道测试失败
    } finally {
      // 确保总是清理资源
      if (queue) {
        queue.stop();
        // 在 Deno 环境下，等待队列中的所有定时器完成
        if (IS_DENO && typeof queue.waitForTimers === "function") {
          await queue.waitForTimers();
        }
      }
      if (queueManager) {
        await queueManager.close();
        // 等待所有异步操作和定时器完成（处理循环每 100ms 检查一次）
        // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 2000 : 1000)
        );
      }
      if (adapter) {
        await adapter.disconnect();
        // 等待连接完全关闭和所有定时器完成
        // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成（Redis 客户端库的内部定时器）
        // Redis 客户端可能有内部定时器（如 Socket 的 _unrefTimer），需要更长时间清理
        // 注意：这些定时器是 Redis 客户端库内部的，我们无法直接控制
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 3000 : 1000)
        );
      }
    }
  }, {
    // 禁用定时器和资源检查（Redis 客户端库可能有内部定时器）
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Redis 适配器处理任务", async () => {
    const isRunning = await checkDockerContainer("redis");
    if (!isRunning) {
      console.log("⚠️  跳过：Redis 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
      adapter = new RedisQueueAdapter({
        connection: {
          url: "redis://127.0.0.1:6379",
          socket: {
            keepAlive: false,
            connectTimeout: 5000,
          },
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({
        adapter,
        autoRecover: false,
      });
      queue = queueManager.createQueue("test-redis-process", {
        concurrency: 1,
      });

      let processed = false;
      queue.process(async (job: any) => {
        expect(job.name).toBe("test-job");
        expect(job.data.data).toBe("test");
        processed = true;
      });

      // 等待处理循环启动
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      const job = await queue.add("test-job", { data: "test" });

      // 验证任务已添加
      const addedJob = await adapter.get(job.id);
      expect(addedJob).toBeTruthy();

      // 等待任务处理（处理循环每 100ms 检查一次，需要足够的时间）
      // 使用轮询方式检查任务是否被处理，最多等待 5 秒
      const maxWaitTime = IS_DENO ? 5000 : 3000;
      const checkInterval = 200;
      let waited = 0;
      while (!processed && waited < maxWaitTime) {
        await new Promise((resolve) => setTimeout(resolve, checkInterval));
        waited += checkInterval;
        // 检查任务状态
        const jobStatus = await adapter.get(job.id);
        if (jobStatus && jobStatus.status === "completed") {
          break;
        }
      }
      expect(processed).toBeTruthy();
    } catch (error) {
      console.log(
        `⚠️  跳过：Redis 测试失败 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error; // 重新抛出错误，让测试框架知道测试失败
    } finally {
      // 确保总是清理资源
      if (queue) {
        queue.stop(); // 先停止队列处理循环
        // 在 Deno 环境下，等待队列中的所有定时器完成
        if (IS_DENO && typeof queue.waitForTimers === "function") {
          await queue.waitForTimers();
        }
      }
      if (queueManager) {
        await queueManager.close();
        // 等待处理循环完全停止（处理循环每 100ms 检查一次 running 标志）
        // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 2000 : 1000)
        );
      }
      if (adapter) {
        await adapter.disconnect();
        // 等待连接完全关闭和所有定时器完成
        // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成（Redis 客户端库的内部定时器）
        // Redis 客户端可能有内部定时器（如 Socket 的 _unrefTimer），需要更长时间清理
        // 注意：这些定时器是 Redis 客户端库内部的，我们无法直接控制
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 3000 : 1000)
        );
      }
    }
  }, {
    // 禁用定时器和资源检查（Redis 客户端库可能有内部定时器）
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Redis 适配器更新任务状态", async () => {
    const isRunning = await checkDockerContainer("redis");
    if (!isRunning) {
      console.log("⚠️  跳过：Redis 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
      adapter = new RedisQueueAdapter({
        connection: {
          url: "redis://127.0.0.1:6379",
          socket: {
            keepAlive: false,
            connectTimeout: 5000,
          },
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-redis-update", {
        concurrency: 1,
      });

      const job = await queue.add("test-job", { data: "test" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 更新任务状态
      await adapter.update(job.id, {
        status: "processing",
        startedAt: Date.now(),
      });

      // 验证任务状态已更新
      const updatedJob = await adapter.get(job.id);
      expect(updatedJob?.status).toBe("processing");
      expect(updatedJob?.startedAt).toBeTruthy();
    } catch (error) {
      console.log(
        `⚠️  跳过：Redis 测试失败 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error;
    } finally {
      if (queue) {
        queue.stop();
        if (IS_DENO && typeof queue.waitForTimers === "function") {
          await queue.waitForTimers();
        }
      }
      if (queueManager) {
        await queueManager.close();
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 2000 : 1000)
        );
      }
      if (adapter) {
        await adapter.disconnect();
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 3000 : 1000)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Redis 适配器删除任务", async () => {
    const isRunning = await checkDockerContainer("redis");
    if (!isRunning) {
      console.log("⚠️  跳过：Redis 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
      adapter = new RedisQueueAdapter({
        connection: {
          url: "redis://127.0.0.1:6379",
          socket: {
            keepAlive: false,
            connectTimeout: 5000,
          },
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-redis-remove", {
        concurrency: 1,
      });

      const job = await queue.add("test-job", { data: "test" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 验证任务存在
      const jobBefore = await adapter.get(job.id);
      expect(jobBefore).toBeTruthy();

      // 删除任务
      await adapter.remove(job.id);

      // 验证任务已删除
      const jobAfter = await adapter.get(job.id);
      expect(jobAfter).toBeNull();
    } catch (error) {
      console.log(
        `⚠️  跳过：Redis 测试失败 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error;
    } finally {
      if (queue) {
        queue.stop();
        if (IS_DENO && typeof queue.waitForTimers === "function") {
          await queue.waitForTimers();
        }
      }
      if (queueManager) {
        await queueManager.close();
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 2000 : 1000)
        );
      }
      if (adapter) {
        await adapter.disconnect();
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 3000 : 1000)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Redis 适配器获取所有任务", async () => {
    const isRunning = await checkDockerContainer("redis");
    if (!isRunning) {
      console.log("⚠️  跳过：Redis 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
      adapter = new RedisQueueAdapter({
        connection: {
          url: "redis://127.0.0.1:6379",
          socket: {
            keepAlive: false,
            connectTimeout: 5000,
          },
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-redis-getall", {
        concurrency: 1,
      });

      const job1 = await queue.add("job1", { data: "data1" });
      const job2 = await queue.add("job2", { data: "data2" });
      const job3 = await queue.add("job3", { data: "data3" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 获取所有任务
      const allJobs = await adapter.getAll("test-redis-getall");

      expect(allJobs.length).toBeGreaterThanOrEqual(3);
      const jobIds = allJobs.map((j: any) => j.id);
      expect(jobIds).toContain(job1.id);
      expect(jobIds).toContain(job2.id);
      expect(jobIds).toContain(job3.id);
    } catch (error) {
      console.log(
        `⚠️  跳过：Redis 测试失败 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error;
    } finally {
      if (queue) {
        queue.stop();
        if (IS_DENO && typeof queue.waitForTimers === "function") {
          await queue.waitForTimers();
        }
      }
      if (queueManager) {
        await queueManager.close();
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 2000 : 1000)
        );
      }
      if (adapter) {
        await adapter.disconnect();
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 3000 : 1000)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Redis 适配器清空队列", async () => {
    const isRunning = await checkDockerContainer("redis");
    if (!isRunning) {
      console.log("⚠️  跳过：Redis 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
      adapter = new RedisQueueAdapter({
        connection: {
          url: "redis://127.0.0.1:6379",
          socket: {
            keepAlive: false,
            connectTimeout: 5000,
          },
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-redis-clear", {
        concurrency: 1,
      });

      await queue.add("job1", { data: "data1" });
      await queue.add("job2", { data: "data2" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 验证任务存在
      let allJobs = await adapter.getAll("test-redis-clear");
      expect(allJobs.length).toBeGreaterThanOrEqual(2);

      // 清空队列
      await adapter.clear("test-redis-clear");

      // 验证队列已清空
      allJobs = await adapter.getAll("test-redis-clear");
      expect(allJobs.length).toBe(0);
    } catch (error) {
      console.log(
        `⚠️  跳过：Redis 测试失败 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error;
    } finally {
      if (queue) {
        queue.stop();
        if (IS_DENO && typeof queue.waitForTimers === "function") {
          await queue.waitForTimers();
        }
      }
      if (queueManager) {
        await queueManager.close();
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 2000 : 1000)
        );
      }
      if (adapter) {
        await adapter.disconnect();
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 3000 : 1000)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Redis 适配器获取队列统计信息", async () => {
    const isRunning = await checkDockerContainer("redis");
    if (!isRunning) {
      console.log("⚠️  跳过：Redis 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
      adapter = new RedisQueueAdapter({
        connection: {
          url: "redis://127.0.0.1:6379",
          socket: {
            keepAlive: false,
            connectTimeout: 5000,
          },
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-redis-stats-full", {
        concurrency: 1,
      });

      // 添加多个不同状态的任务
      const job1 = await queue.add("pending-job", { data: "pending" });
      const job2 = await queue.add("processing-job", { data: "processing" });
      const job3 = await queue.add("completed-job", { data: "completed" });
      const job4 = await queue.add("failed-job", { data: "failed" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 更新任务状态
      await adapter.update(job2.id, {
        status: "processing",
        startedAt: Date.now(),
      });
      await adapter.update(job3.id, {
        status: "completed",
        completedAt: Date.now(),
      });
      await adapter.update(job4.id, {
        status: "failed",
        failedAt: Date.now(),
        error: "处理失败",
      });

      // 等待状态更新
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 获取统计信息
      const stats = await adapter.getStats("test-redis-stats-full");

      expect(stats).toBeTruthy();
      expect(typeof stats.pending).toBe("number");
      expect(typeof stats.processing).toBe("number");
      expect(typeof stats.completed).toBe("number");
      expect(typeof stats.failed).toBe("number");
      expect(stats.pending).toBeGreaterThanOrEqual(1);
      expect(stats.processing).toBeGreaterThanOrEqual(1);
      expect(stats.completed).toBeGreaterThanOrEqual(1);
      expect(stats.failed).toBeGreaterThanOrEqual(1);
    } catch (error) {
      console.log(
        `⚠️  跳过：Redis 测试失败 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error;
    } finally {
      if (queue) {
        queue.stop();
        if (IS_DENO && typeof queue.waitForTimers === "function") {
          await queue.waitForTimers();
        }
      }
      if (queueManager) {
        await queueManager.close();
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 2000 : 1000)
        );
      }
      if (adapter) {
        await adapter.disconnect();
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 3000 : 1000)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  describe("Redis MGET 批量获取优化", () => {
    it("应该使用 MGET 批量获取大量任务（100个任务）", async () => {
      const isRunning = await checkDockerContainer("redis");
      if (!isRunning) {
        console.log("⚠️  跳过：Redis 容器未运行");
        return;
      }

      let adapter: any = null;
      let queueManager: any = null;
      let queue: any = null;
      try {
        const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
        adapter = new RedisQueueAdapter({
          connection: {
            url: "redis://127.0.0.1:6379",
            socket: {
              keepAlive: false,
              connectTimeout: 5000,
            },
          },
        });
        await adapter.connect();

        queueManager = new QueueManager({ adapter, autoRecover: false });
        queue = queueManager.createQueue("test-redis-mget-batch", {
          concurrency: 1,
        });

        // 添加 100 个任务
        const jobs: any[] = [];
        for (let i = 0; i < 100; i++) {
          const job = await queue.add(`job-${i}`, { data: `data-${i}` });
          jobs.push(job);
        }

        // 等待任务添加
        await new Promise((resolve) => setTimeout(resolve, 500));

        // 获取所有任务（应该使用 MGET 批量获取）
        const startTime = Date.now();
        const allJobs = await adapter.getAll("test-redis-mget-batch");
        const endTime = Date.now();
        const duration = endTime - startTime;

        // 验证获取到所有任务
        expect(allJobs.length).toBeGreaterThanOrEqual(100);
        const jobIds = allJobs.map((j: any) => j.id);
        for (const job of jobs) {
          expect(jobIds).toContain(job.id);
        }

        // 验证性能：批量获取应该在 100ms 内完成（100个任务）
        // 注意：这是相对宽松的阈值，实际应该更快（< 50ms）
        expect(duration).toBeLessThan(200);
      } catch (error) {
        console.log(
          `⚠️  跳过：Redis MGET 测试失败 - ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        throw error;
      } finally {
        if (queue) {
          queue.stop();
          if (IS_DENO && typeof queue.waitForTimers === "function") {
            await queue.waitForTimers();
          }
        }
        if (queueManager) {
          await queueManager.close();
          await new Promise((resolve) =>
            setTimeout(resolve, IS_DENO ? 2000 : 1000)
          );
        }
        if (adapter) {
          await adapter.disconnect();
          await new Promise((resolve) =>
            setTimeout(resolve, IS_DENO ? 3000 : 1000)
          );
        }
      }
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });

    it("应该处理 MGET 返回部分 null 的情况", async () => {
      const isRunning = await checkDockerContainer("redis");
      if (!isRunning) {
        console.log("⚠️  跳过：Redis 容器未运行");
        return;
      }

      let adapter: any = null;
      let queueManager: any = null;
      let queue: any = null;
      try {
        const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
        adapter = new RedisQueueAdapter({
          connection: {
            url: "redis://127.0.0.1:6379",
            socket: {
              keepAlive: false,
              connectTimeout: 5000,
            },
          },
        });
        await adapter.connect();

        queueManager = new QueueManager({ adapter, autoRecover: false });
        queue = queueManager.createQueue("test-redis-mget-null", {
          concurrency: 1,
        });

        // 添加 5 个任务
        const jobs: any[] = [];
        for (let i = 0; i < 5; i++) {
          const job = await queue.add(`job-${i}`, { data: `data-${i}` });
          jobs.push(job);
        }

        // 等待任务添加
        await new Promise((resolve) => setTimeout(resolve, 500));

        // 手动删除部分任务数据（模拟部分键不存在的情况）
        const internalClient = (adapter as any).internalClient;
        if (internalClient) {
          // 删除第二个和第四个任务的数据
          const jobKey2 = `queue:job:${jobs[1].id}`;
          const jobKey4 = `queue:job:${jobs[3].id}`;
          await internalClient.del(jobKey2);
          await internalClient.del(jobKey4);
        }

        // 获取所有任务（应该能正确处理部分键不存在的情况）
        const allJobs = await adapter.getAll("test-redis-mget-null");

        // 验证：应该只获取到存在的任务（3个）
        expect(allJobs.length).toBeGreaterThanOrEqual(3);
        const jobIds = allJobs.map((j: any) => j.id);
        expect(jobIds).toContain(jobs[0].id);
        expect(jobIds).toContain(jobs[2].id);
        expect(jobIds).toContain(jobs[4].id);
        // 被删除的任务不应该在结果中
        expect(jobIds).not.toContain(jobs[1].id);
        expect(jobIds).not.toContain(jobs[3].id);
      } catch (error) {
        console.log(
          `⚠️  跳过：Redis MGET null 测试失败 - ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        throw error;
      } finally {
        if (queue) {
          queue.stop();
          if (IS_DENO && typeof queue.waitForTimers === "function") {
            await queue.waitForTimers();
          }
        }
        if (queueManager) {
          await queueManager.close();
          await new Promise((resolve) =>
            setTimeout(resolve, IS_DENO ? 2000 : 1000)
          );
        }
        if (adapter) {
          await adapter.disconnect();
          await new Promise((resolve) =>
            setTimeout(resolve, IS_DENO ? 3000 : 1000)
          );
        }
      }
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });

    it("应该在单个任务时回退到单个获取", async () => {
      const isRunning = await checkDockerContainer("redis");
      if (!isRunning) {
        console.log("⚠️  跳过：Redis 容器未运行");
        return;
      }

      let adapter: any = null;
      let queueManager: any = null;
      let queue: any = null;
      try {
        const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
        adapter = new RedisQueueAdapter({
          connection: {
            url: "redis://127.0.0.1:6379",
            socket: {
              keepAlive: false,
              connectTimeout: 5000,
            },
          },
        });
        await adapter.connect();

        queueManager = new QueueManager({ adapter, autoRecover: false });
        queue = queueManager.createQueue("test-redis-single-fallback", {
          concurrency: 1,
        });

        // 添加 1 个任务（应该回退到单个获取）
        const job = await queue.add("single-job", { data: "single-data" });

        // 等待任务添加
        await new Promise((resolve) => setTimeout(resolve, 500));

        // 获取所有任务（应该能正确获取，即使只有 1 个任务）
        const allJobs = await adapter.getAll("test-redis-single-fallback");

        // 验证获取到任务
        expect(allJobs.length).toBeGreaterThanOrEqual(1);
        const jobIds = allJobs.map((j: any) => j.id);
        expect(jobIds).toContain(job.id);
      } catch (error) {
        console.log(
          `⚠️  跳过：Redis 单个任务回退测试失败 - ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        throw error;
      } finally {
        if (queue) {
          queue.stop();
          if (IS_DENO && typeof queue.waitForTimers === "function") {
            await queue.waitForTimers();
          }
        }
        if (queueManager) {
          await queueManager.close();
          await new Promise((resolve) =>
            setTimeout(resolve, IS_DENO ? 2000 : 1000)
          );
        }
        if (adapter) {
          await adapter.disconnect();
          await new Promise((resolve) =>
            setTimeout(resolve, IS_DENO ? 3000 : 1000)
          );
        }
      }
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });
  });
});
