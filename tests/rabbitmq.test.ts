/**
 * @fileoverview RabbitMQ 队列适配器测试
 */

import { getEnv, IS_DENO } from "@dreamer/runtime-adapter";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from "@dreamer/test";
import { RabbitMQQueueAdapter } from "../src/adapters/rabbitmq.ts";
import { type Job, QueueManager } from "../src/mod.ts";

/**
 * 获取环境变量，带默认值
 */
function getEnvWithDefault(key: string, defaultValue: string = ""): string {
  return getEnv(key) || defaultValue;
}

describe("Queue > RabbitMQQueueAdapter", () => {
  let adapter: RabbitMQQueueAdapter | null = null;

  beforeAll(async () => {
    const rabbitmqHost = getEnvWithDefault("RABBITMQ_HOST", "localhost");
    const rabbitmqPort = parseInt(getEnvWithDefault("RABBITMQ_PORT", "5672"));
    // 默认使用 guest/guest（如果容器中没有 guest 用户，可以通过环境变量配置其他用户）
    const rabbitmqUser = getEnvWithDefault("RABBITMQ_USER", "guest");
    const rabbitmqPassword = getEnvWithDefault("RABBITMQ_PASSWORD", "guest");
    const rabbitmqVhost = getEnvWithDefault("RABBITMQ_VHOST", "/");

    try {
      // 构建连接 URL（使用 localhost 而不是 127.0.0.1，因为 RabbitMQ 的 guest 用户默认只能从 localhost 连接）
      // vhost 需要 URL 编码，如果包含特殊字符
      // 如果 vhost 是 "/"，则不需要 URL 编码，直接使用 "/"
      const encodedVhost = rabbitmqVhost === "/"
        ? ""
        : encodeURIComponent(rabbitmqVhost);
      const connectionUrl = encodedVhost
        ? `amqp://${rabbitmqUser}:${rabbitmqPassword}@${rabbitmqHost}:${rabbitmqPort}/${encodedVhost}`
        : `amqp://${rabbitmqUser}:${rabbitmqPassword}@${rabbitmqHost}:${rabbitmqPort}/`;

      console.log(
        `尝试连接 RabbitMQ: ${connectionUrl.replace(/:[^:@]+@/, ":***@")}`,
      );

      adapter = new RabbitMQQueueAdapter({
        connection: {
          url: connectionUrl,
        },
        queueOptions: { durable: true },
      });

      // 连接适配器（设置超时）
      const connectPromise = adapter.connect();
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error("连接超时（10秒）")), 10000)
      );
      await Promise.race([connectPromise, timeoutPromise]);

      console.log("✅ RabbitMQ 连接成功");
    } catch (error) {
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);
      console.error(
        `❌ RabbitMQ 连接失败: ${errorMessage}`,
      );
      console.error(
        `   连接配置: host=${rabbitmqHost}, port=${rabbitmqPort}, user=${rabbitmqUser}, vhost=${rabbitmqVhost}`,
      );
      console.warn("   跳过所有 RabbitMQ 测试");
      adapter = null;
    }
  });

  afterAll(async () => {
    if (adapter) {
      try {
        await adapter.disconnect();
        // 等待连接完全关闭和所有定时器完成
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 1000 : 300)
        );
      } catch {
        // 忽略关闭错误
      }
    }
  });

  beforeEach(async () => {
    // RabbitMQ 不需要清理数据，因为每个测试使用不同的队列名称
  });

  it("应该检查 RabbitMQ 连接", async () => {
    if (!adapter) {
      console.log("⚠️  RabbitMQ 不可用，跳过测试");
      return;
    }

    expect(adapter).toBeTruthy();
  }, { sanitizeOps: false, sanitizeResources: false });

  it("应该创建 RabbitMQ 队列适配器", async () => {
    if (!adapter) {
      console.log("⚠️  RabbitMQ 不可用，跳过测试");
      return;
    }

    expect(adapter).toBeTruthy();
  }, { sanitizeOps: false, sanitizeResources: false });

  it("应该使用 RabbitMQ 适配器添加和获取任务", async () => {
    if (!adapter) {
      console.log("⚠️  RabbitMQ 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-rabbitmq", {
      concurrency: 1,
    });

    try {
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
    } finally {
      // 清理：先停止队列，再关闭适配器
      queue.stop(); // 先停止队列处理循环
      // 在 Deno 环境下，等待队列中的所有定时器完成
      if (IS_DENO && typeof (queue as any).waitForTimers === "function") {
        await (queue as any).waitForTimers();
      }
      await queueManager.close();
      // 等待处理循环完全停止（RabbitMQ 可能需要更长时间）
      await new Promise((resolve) =>
        setTimeout(resolve, IS_DENO ? 2000 : 1000)
      );
    }
  }, { sanitizeOps: false, sanitizeResources: false });

  it("应该使用 RabbitMQ 适配器处理任务", async () => {
    if (!adapter) {
      console.log("⚠️  RabbitMQ 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({
      adapter,
      autoRecover: false,
    });
    const queue = queueManager.createQueue("test-rabbitmq-process", {
      concurrency: 1,
    });

    try {
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
    } finally {
      // 确保总是清理资源
      queue.stop(); // 先停止队列处理循环
      // 在 Deno 环境下，等待队列中的所有定时器完成
      if (IS_DENO && typeof queue.waitForTimers === "function") {
        await queue.waitForTimers();
      }
      await queueManager.close();
      // 等待处理循环完全停止（RabbitMQ 可能需要更长时间）
      await new Promise((resolve) =>
        setTimeout(resolve, IS_DENO ? 2000 : 1000)
      );
    }
  }, {
    // 禁用定时器和资源检查（RabbitMQ 客户端库可能有内部定时器）
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 RabbitMQ 适配器更新任务状态", async () => {
    if (!adapter) {
      console.log("⚠️  RabbitMQ 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-rabbitmq-update", {
      concurrency: 1,
    });

    try {
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
    } finally {
      queue.stop();
      if (IS_DENO && typeof (queue as any).waitForTimers === "function") {
        await (queue as any).waitForTimers();
      }
      await queueManager.close();
      await new Promise((resolve) =>
        setTimeout(resolve, IS_DENO ? 2000 : 1000)
      );
    }
  }, { sanitizeOps: false, sanitizeResources: false });

  it("应该使用 RabbitMQ 适配器删除任务", async () => {
    if (!adapter) {
      console.log("⚠️  RabbitMQ 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-rabbitmq-remove", {
      concurrency: 1,
    });

    try {
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
    } finally {
      queue.stop();
      if (IS_DENO && typeof (queue as any).waitForTimers === "function") {
        await (queue as any).waitForTimers();
      }
      await queueManager.close();
      await new Promise((resolve) =>
        setTimeout(resolve, IS_DENO ? 2000 : 1000)
      );
    }
  }, { sanitizeOps: false, sanitizeResources: false });

  it("应该使用 RabbitMQ 适配器获取所有任务", async () => {
    if (!adapter) {
      console.log("⚠️  RabbitMQ 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-rabbitmq-getall", {
      concurrency: 1,
    });

    try {
      const job1 = await queue.add("job1", { data: "data1" });
      const job2 = await queue.add("job2", { data: "data2" });
      const job3 = await queue.add("job3", { data: "data3" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 获取所有任务
      const allJobs = await adapter.getAll("test-rabbitmq-getall");

      expect(allJobs.length).toBeGreaterThanOrEqual(3);
      const jobIds = allJobs.map((j) => j.id);
      expect(jobIds).toContain(job1.id);
      expect(jobIds).toContain(job2.id);
      expect(jobIds).toContain(job3.id);
    } finally {
      queue.stop();
      if (IS_DENO && typeof (queue as any).waitForTimers === "function") {
        await (queue as any).waitForTimers();
      }
      await queueManager.close();
      await new Promise((resolve) =>
        setTimeout(resolve, IS_DENO ? 2000 : 1000)
      );
    }
  }, { sanitizeOps: false, sanitizeResources: false });

  it("应该使用 RabbitMQ 适配器清空队列", async () => {
    if (!adapter) {
      console.log("⚠️  RabbitMQ 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-rabbitmq-clear", {
      concurrency: 1,
    });

    try {
      await queue.add("job1", { data: "data1" });
      await queue.add("job2", { data: "data2" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 验证任务存在
      let allJobs = await adapter.getAll("test-rabbitmq-clear");
      expect(allJobs.length).toBeGreaterThanOrEqual(2);

      // 清空队列
      await adapter.clear("test-rabbitmq-clear");

      // 验证队列已清空
      allJobs = await adapter.getAll("test-rabbitmq-clear");
      expect(allJobs.length).toBe(0);
    } finally {
      queue.stop();
      if (IS_DENO && typeof (queue as any).waitForTimers === "function") {
        await (queue as any).waitForTimers();
      }
      await queueManager.close();
      await new Promise((resolve) =>
        setTimeout(resolve, IS_DENO ? 2000 : 1000)
      );
    }
  }, { sanitizeOps: false, sanitizeResources: false });

  it("应该使用 RabbitMQ 适配器获取队列统计信息", async () => {
    if (!adapter) {
      console.log("⚠️  RabbitMQ 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-rabbitmq-stats-full", {
      concurrency: 1,
    });

    try {
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
      const stats = await adapter.getStats("test-rabbitmq-stats-full");

      expect(stats).toBeTruthy();
      expect(typeof stats.pending).toBe("number");
      expect(typeof stats.processing).toBe("number");
      expect(typeof stats.completed).toBe("number");
      expect(typeof stats.failed).toBe("number");
      expect(stats.pending).toBeGreaterThanOrEqual(1);
      expect(stats.processing).toBeGreaterThanOrEqual(1);
      expect(stats.completed).toBeGreaterThanOrEqual(1);
      expect(stats.failed).toBeGreaterThanOrEqual(1);
    } finally {
      queue.stop();
      if (IS_DENO && typeof (queue as any).waitForTimers === "function") {
        await (queue as any).waitForTimers();
      }
      await queueManager.close();
      await new Promise((resolve) =>
        setTimeout(resolve, IS_DENO ? 2000 : 1000)
      );
    }
  }, { sanitizeOps: false, sanitizeResources: false });
});
