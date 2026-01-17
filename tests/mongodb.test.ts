/**
 * @fileoverview MongoDB 队列适配器测试
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
import { MongoDBQueueAdapter } from "../src/adapters/mongodb.ts";
import { type Job, QueueManager } from "../src/mod.ts";

/**
 * 获取环境变量，带默认值
 */
function getEnvWithDefault(key: string, defaultValue: string = ""): string {
  return getEnv(key) || defaultValue;
}

// 定义集合名常量（使用目录名_文件名_作为前缀）
const COLLECTION_PREFIX = "queue_mongodb_test";

describe("Queue > MongoDBQueueAdapter", () => {
  let adapter: MongoDBQueueAdapter | null = null;

  beforeAll(async () => {
    const mongoHost = getEnvWithDefault("MONGODB_HOST", "localhost");
    const mongoPort = parseInt(getEnvWithDefault("MONGODB_PORT", "27017"));
    const mongoDatabase = getEnvWithDefault(
      "MONGODB_DATABASE",
      "test_queue_mongodb",
    );
    const replicaSet = getEnvWithDefault("MONGODB_REPLICA_SET", "rs0");
    const directConnection = getEnvWithDefault(
      "MONGODB_DIRECT_CONNECTION",
      "true",
    ) === "true";

    try {
      // 构建连接 URL
      let connectionUrl: string;
      if (mongoHost.includes("://")) {
        // 如果 host 已经是完整的 URL，直接使用
        connectionUrl = mongoHost;
      } else {
        // 否则构建 URL
        connectionUrl = `mongodb://${mongoHost}:${mongoPort}`;
      }

      adapter = new MongoDBQueueAdapter({
        connection: {
          url: connectionUrl,
          database: mongoDatabase,
          options: {
            // 添加副本集和直接连接选项（参考 database 库的测试）
            replicaSet: replicaSet,
            directConnection: directConnection,
            connectTimeoutMS: 5000,
          },
        },
        collectionPrefix: COLLECTION_PREFIX,
        databaseName: mongoDatabase,
      });

      // 连接适配器
      await adapter.connect();
    } catch (error) {
      console.warn(
        `MongoDB not available, skipping tests: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      adapter = null;
    }
  });

  afterAll(async () => {
    if (adapter) {
      try {
        // 清理测试数据
        const collection = (adapter as any).getCollection();
        if (collection) {
          await collection.deleteMany({});
        }
      } catch {
        // 忽略清理错误
      }
      // 关闭适配器连接
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
    if (!adapter) return;

    // 清理测试数据
    try {
      const collection = (adapter as any).getCollection();
      if (collection) {
        await collection.deleteMany({});
      }
    } catch {
      // 忽略清理错误
    }
  });

  it("应该检查 MongoDB 连接", async () => {
    if (!adapter) {
      console.log("⚠️  MongoDB 不可用，跳过测试");
      return;
    }

    expect(adapter).toBeTruthy();
  }, { sanitizeOps: false, sanitizeResources: false });

  it("应该创建 MongoDB 队列适配器", async () => {
    if (!adapter) {
      console.log("⚠️  MongoDB 不可用，跳过测试");
      return;
    }

    expect(adapter).toBeTruthy();
  }, { sanitizeOps: false, sanitizeResources: false });

  it("应该使用 MongoDB 适配器添加和获取任务", async () => {
    if (!adapter) {
      console.log("⚠️  MongoDB 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-mongodb", {
      concurrency: 1,
    });

    try {
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
    } finally {
      // 清理：先停止队列，再关闭适配器
      queue.stop();
      // 在 Deno 环境下，等待队列中的所有定时器完成
      if (IS_DENO && typeof (queue as any).waitForTimers === "function") {
        await (queue as any).waitForTimers();
      }
      await queueManager.close();
      // 等待所有异步操作和定时器完成（处理循环每 100ms 检查一次）
      await new Promise((resolve) =>
        setTimeout(resolve, IS_DENO ? 2000 : 1000)
      );
    }
  }, { sanitizeOps: false, sanitizeResources: false });

  it("应该使用 MongoDB 适配器处理任务", async () => {
    if (!adapter) {
      console.log("⚠️  MongoDB 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({
      adapter,
      autoRecover: false,
    });
    const queue = queueManager.createQueue("test-mongodb-process", {
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
      expect(addedJob?.status).toBe("pending");

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
    } finally {
      // 确保总是清理资源
      queue.stop(); // 先停止队列处理循环
      // 在 Deno 环境下，等待队列中的所有定时器完成
      if (IS_DENO && typeof queue.waitForTimers === "function") {
        await queue.waitForTimers();
      }
      await queueManager.close();
      // 等待处理循环完全停止（处理循环每 100ms 检查一次 running 标志）
      await new Promise((resolve) =>
        setTimeout(resolve, IS_DENO ? 2000 : 1000)
      );
    }
  }, {
    // 禁用定时器和资源检查（MongoDB 客户端库可能有内部定时器）
    sanitizeOps: false,
    sanitizeResources: false,
  });
});
