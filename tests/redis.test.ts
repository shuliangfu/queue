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
});
