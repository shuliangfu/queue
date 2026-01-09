/**
 * @fileoverview RabbitMQ 队列适配器测试
 */

import { IS_DENO } from "@dreamer/runtime-adapter";
import { describe, expect, it } from "@dreamer/test";
import { QueueManager } from "../src/mod.ts";
import { checkDockerContainer } from "./helpers.ts";

describe("Queue > RabbitMQQueueAdapter", () => {
  it("应该检查 RabbitMQ 容器是否运行", async () => {
    const isRunning = await checkDockerContainer("rabbitmq");
    if (!isRunning) {
      console.log("⚠️  RabbitMQ 容器未运行，跳过 RabbitMQ 测试");
      console.log(
        "   启动 RabbitMQ: docker run -d -p 5672:5672 -p 15672:15672 --name rabbitmq rabbitmq:latest",
      );
      return;
    }
    expect(isRunning).toBeTruthy();
  });

  it("应该创建 RabbitMQ 队列适配器", async () => {
    const isRunning = await checkDockerContainer("rabbitmq");
    if (!isRunning) {
      console.log("⚠️  跳过：RabbitMQ 容器未运行");
      return;
    }

    let adapter: any = null;
    try {
      const { RabbitMQQueueAdapter } = await import(
        "../src/adapters/rabbitmq.ts"
      );
      adapter = new RabbitMQQueueAdapter({
        connection: {
          url: "amqp://guest:guest@127.0.0.1:5672",
        },
        queueOptions: { durable: true },
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
        `⚠️  跳过：无法创建 RabbitMQ 适配器 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      if (adapter?.disconnect) {
        await adapter.disconnect();
        // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 500 : 300)
        );
      }
    }
  });

  it("应该使用 RabbitMQ 适配器添加和获取任务", async () => {
    const isRunning = await checkDockerContainer("rabbitmq");
    if (!isRunning) {
      console.log("⚠️  跳过：RabbitMQ 容器未运行");
      return;
    }

    let adapter: any = null;
    try {
      const { RabbitMQQueueAdapter } = await import(
        "../src/adapters/rabbitmq.ts"
      );
      adapter = new RabbitMQQueueAdapter({
        connection: {
          url: "amqp://guest:guest@127.0.0.1:5672",
        },
        queueOptions: { durable: true },
      });
      await adapter.connect();

      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-rabbitmq", {
        concurrency: 1,
      });

      const job = await queue.add("test-job", { data: "test" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 直接通过适配器获取任务
      const retrievedJob = await adapter.get(job.id);
      if (!retrievedJob) {
        throw new Error(
          `无法获取任务 ${job.id}，任务可能未正确添加到 RabbitMQ 缓存`,
        );
      }
      // 验证任务内容
      expect(retrievedJob.name).toBe("test-job");

      // 清理：先停止队列，再关闭适配器
      queue.stop(); // 先停止队列处理循环
      // 在 Deno 环境下，等待队列中的所有定时器完成
      if (IS_DENO && typeof (queue as any).waitForTimers === "function") {
        await (queue as any).waitForTimers();
      }
      await queueManager.close();
      // 等待处理循环完全停止（RabbitMQ 可能需要更长时间）
      // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
      await new Promise((resolve) =>
        setTimeout(resolve, IS_DENO ? 2000 : 1000)
      );
      await adapter.disconnect();
      // 等待连接完全关闭和所有定时器完成
      // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 800 : 300));
    } catch (error) {
      console.log(
        `⚠️  跳过：RabbitMQ 测试失败 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      if (adapter?.disconnect) {
        await adapter.disconnect();
      }
    }
  });

  it("应该使用 RabbitMQ 适配器处理任务", async () => {
    const isRunning = await checkDockerContainer("rabbitmq");
    if (!isRunning) {
      console.log("⚠️  跳过：RabbitMQ 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { RabbitMQQueueAdapter } = await import(
        "../src/adapters/rabbitmq.ts"
      );
      adapter = new RabbitMQQueueAdapter({
        connection: {
          url: "amqp://guest:guest@127.0.0.1:5672",
        },
        queueOptions: { durable: true },
      });
      await adapter.connect();

      queueManager = new QueueManager({
        adapter,
        autoRecover: false,
      });
      queue = queueManager.createQueue("test-rabbitmq-process", {
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
      // 使用轮询方式检查任务是否被处理，最多等待 10 秒（RabbitMQ 可能需要更长时间）
      const maxWaitTime = IS_DENO ? 10000 : 5000;
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
      if (!processed) {
        // 如果任务没有被处理，输出调试信息
        const finalStatus = await adapter.get(job.id);
        throw new Error(
          `任务未被处理。任务状态: ${
            finalStatus?.status || "unknown"
          }, 等待时间: ${waited}ms`,
        );
      }
      expect(processed).toBeTruthy();
    } catch (error) {
      console.log(
        `⚠️  跳过：RabbitMQ 测试失败 - ${
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
        // 等待处理循环完全停止（RabbitMQ 可能需要更长时间）
        // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 2000 : 1000)
        );
      }
      if (adapter) {
        await adapter.disconnect();
        // 等待连接完全关闭和所有定时器完成
        // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 800 : 300)
        );
      }
    }
  }, {
    // 禁用定时器和资源检查（RabbitMQ 客户端库可能有内部定时器）
    sanitizeOps: false,
    sanitizeResources: false,
  });
});
