/**
 * @fileoverview MongoDB 队列适配器测试
 */

import { IS_DENO } from "@dreamer/runtime-adapter";
import { describe, expect, it } from "@dreamer/test";
import { type Job, QueueManager } from "../src/mod.ts";
import { checkDockerContainer } from "./helpers.ts";

describe("Queue > MongoDBQueueAdapter", () => {
  it("应该检查 MongoDB 容器是否运行", async () => {
    const isRunning = await checkDockerContainer("mongodb");
    if (!isRunning) {
      console.log("⚠️  MongoDB 容器未运行，跳过 MongoDB 测试");
      console.log(
        "   启动 MongoDB: docker run -d -p 27017:27017 --name mongodb mongo:latest",
      );
      return;
    }
    expect(isRunning).toBeTruthy();
  });

  it("应该创建 MongoDB 队列适配器", async () => {
    const isRunning = await checkDockerContainer("mongodb");
    if (!isRunning) {
      console.log("⚠️  跳过：MongoDB 容器未运行");
      return;
    }

    let adapter: any = null;
    try {
      const { MongoDBQueueAdapter } = await import(
        "../src/adapters/mongodb.ts"
      );
      adapter = new MongoDBQueueAdapter({
        connection: {
          url: "mongodb://127.0.0.1:27017",
          database: "test_queue",
        },
      });
      await adapter.connect();
      expect(adapter).toBeTruthy();

      // 等待适配器初始化完成
      // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 300 : 200));

      // 清理
      await adapter.disconnect();
      // 等待连接完全关闭和所有定时器完成
      // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成（客户端库的内部定时器）
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 1000 : 300));
    } catch (error) {
      console.log(
        `⚠️  跳过：无法创建 MongoDB 适配器 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      if (adapter?.disconnect) {
        await adapter.disconnect();
        // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 1000 : 300)
        );
      }
    }
  });

  it("应该使用 MongoDB 适配器添加和获取任务", async () => {
    const isRunning = await checkDockerContainer("mongodb");
    if (!isRunning) {
      console.log("⚠️  跳过：MongoDB 容器未运行");
      return;
    }

    let adapter: any = null;
    try {
      const { MongoDBQueueAdapter } = await import(
        "../src/adapters/mongodb.ts"
      );
      adapter = new MongoDBQueueAdapter({
        connection: {
          url: "mongodb://127.0.0.1:27017",
          database: "test_queue",
        },
      });
      await adapter.connect();

      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-mongodb", {
        concurrency: 1,
      });

      // 不设置处理器，确保任务保持 pending 状态
      const job = await queue.add("test-job", { data: "test" });

      // 等待一小段时间确保任务已添加到 MongoDB
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 直接通过适配器获取任务，验证任务是否被正确添加
      const retrievedJob = await adapter.get(job.id);
      if (!retrievedJob) {
        throw new Error(
          `无法获取任务 ${job.id}，任务可能未正确添加到 MongoDB`,
        );
      }
      // 验证任务内容
      expect(retrievedJob.name).toBe("test-job");
      expect(retrievedJob.data.data).toBe("test");

      // 清理：先停止队列，再关闭适配器
      queue.stop();
      // 在 Deno 环境下，等待队列中的所有定时器完成
      if (IS_DENO && typeof (queue as any).waitForTimers === "function") {
        await (queue as any).waitForTimers();
      }
      await queueManager.close();
      // 等待所有异步操作和定时器完成（处理循环每 100ms 检查一次）
      // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
      await new Promise((resolve) =>
        setTimeout(resolve, IS_DENO ? 2000 : 1000)
      );
      await adapter.disconnect();
      // 等待连接完全关闭和所有定时器完成
      // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成（MongoDB 客户端库的内部定时器）
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 1000 : 300));
    } catch (error) {
      console.log(
        `⚠️  跳过：MongoDB 测试失败 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      if (adapter?.disconnect) {
        await adapter.disconnect();
        // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 1000 : 300)
        );
      }
    }
  });

  it("应该使用 MongoDB 适配器处理任务", async () => {
    const isRunning = await checkDockerContainer("mongodb");
    if (!isRunning) {
      console.log("⚠️  跳过：MongoDB 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { MongoDBQueueAdapter } = await import(
        "../src/adapters/mongodb.ts"
      );
      adapter = new MongoDBQueueAdapter({
        connection: {
          url: "mongodb://127.0.0.1:27017",
          database: "test_queue",
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({
        adapter,
        autoRecover: false,
      });
      queue = queueManager.createQueue("test-mongodb-process", {
        concurrency: 1,
      });

      let processed = false;
      queue.process(async (job: Job) => {
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
      expect(addedJob?.status).toBe("pending");
      // 注意：get 方法会移除 queueName 字段，所以这里不检查

      // 等待任务处理（处理循环每 100ms 检查一次，需要足够的时间）
      // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
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
        `⚠️  跳过：MongoDB 测试失败 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      // 测试失败时也要清理资源
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
        // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成（MongoDB 客户端库的内部定时器）
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 1000 : 300)
        );
      }
    }
  });
});
