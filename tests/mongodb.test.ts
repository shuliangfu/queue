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
    // 副本集已开启，使用 rs0；账户密码写死用于本地/CI 测试
    const replicaSet = getEnvWithDefault("MONGODB_REPLICA_SET", "rs0");
    const directConnection = getEnvWithDefault(
      "MONGODB_DIRECT_CONNECTION",
      "true",
    ) === "true";
    const mongoUser = getEnvWithDefault("MONGODB_USER", "root");
    const mongoPassword = getEnvWithDefault("MONGODB_PASSWORD", "8866231");

    try {
      let connectionUrl: string;
      if (mongoHost.includes("://")) {
        connectionUrl = mongoHost;
      } else {
        const authPart = `${encodeURIComponent(mongoUser)}:${
          encodeURIComponent(mongoPassword)
        }@`;
        connectionUrl = `mongodb://${authPart}${mongoHost}:${mongoPort}`;
      }

      adapter = new MongoDBQueueAdapter({
        connection: {
          url: connectionUrl,
          database: mongoDatabase,
          options: {
            replicaSet: replicaSet,
            directConnection: directConnection,
            connectTimeoutMS: 5000,
          },
        },
        collectionPrefix: COLLECTION_PREFIX,
        databaseName: mongoDatabase,
      });

      await adapter.connect();

      try {
        const col = (adapter as unknown as {
          getCollection(): { findOne(filter: object): Promise<unknown> };
        }).getCollection();
        await col.findOne({});
      } catch (verifyErr) {
        const msg = verifyErr instanceof Error
          ? verifyErr.message
          : String(verifyErr);
        if (
          /requires authentication|Command .* requires authentication/i.test(
            msg,
          )
        ) {
          console.warn("MongoDB 认证失败，跳过 MongoDB 测试。");
          adapter = null;
        } else {
          console.warn("MongoDB 验证操作失败，跳过测试:", msg);
          adapter = null;
        }
      }
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

  it("应该使用 MongoDB 适配器更新任务状态", async () => {
    if (!adapter) {
      console.log("⚠️  MongoDB 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-mongodb-update", {
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

  it("应该使用 MongoDB 适配器删除任务", async () => {
    if (!adapter) {
      console.log("⚠️  MongoDB 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-mongodb-remove", {
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

  it("应该使用 MongoDB 适配器获取所有任务", async () => {
    if (!adapter) {
      console.log("⚠️  MongoDB 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-mongodb-getall", {
      concurrency: 1,
    });

    try {
      const job1 = await queue.add("job1", { data: "data1" });
      const job2 = await queue.add("job2", { data: "data2" });
      const job3 = await queue.add("job3", { data: "data3" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 获取所有任务
      const allJobs = await adapter.getAll("test-mongodb-getall");

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

  it("应该使用 MongoDB 适配器清空队列", async () => {
    if (!adapter) {
      console.log("⚠️  MongoDB 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-mongodb-clear", {
      concurrency: 1,
    });

    try {
      await queue.add("job1", { data: "data1" });
      await queue.add("job2", { data: "data2" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 验证任务存在
      let allJobs = await adapter.getAll("test-mongodb-clear");
      expect(allJobs.length).toBeGreaterThanOrEqual(2);

      // 清空队列
      await adapter.clear("test-mongodb-clear");

      // 验证队列已清空
      allJobs = await adapter.getAll("test-mongodb-clear");
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

  it("应该使用 MongoDB 适配器获取队列统计信息", async () => {
    if (!adapter) {
      console.log("⚠️  MongoDB 不可用，跳过测试");
      return;
    }

    const queueManager = new QueueManager({ adapter, autoRecover: false });
    const queue = queueManager.createQueue("test-mongodb-stats-full", {
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
      const stats = await adapter.getStats("test-mongodb-stats-full");

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

  describe("MongoDB 聚合管道优化", () => {
    it("应该使用聚合管道获取最高优先级任务（100个任务）", async () => {
      if (!adapter) {
        console.log("⚠️  MongoDB 不可用，跳过测试");
        return;
      }

      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-mongodb-aggregate", {
        concurrency: 1,
      });

      try {
        // 添加 100 个不同优先级的任务
        const jobs: any[] = [];
        const priorities: Array<"low" | "normal" | "high" | "urgent"> = [
          "low",
          "normal",
          "high",
          "urgent",
        ];

        for (let i = 0; i < 100; i++) {
          const priority = priorities[i % 4];
          const job = await queue.add(`job-${i}`, { data: `data-${i}` }, {
            priority,
          });
          jobs.push({ ...job, priority });
        }

        // 等待任务添加
        await new Promise((resolve) => setTimeout(resolve, 500));

        // 获取下一个任务（应该使用聚合管道，返回最高优先级）
        const startTime = Date.now();
        const nextJob = await adapter.getNext("test-mongodb-aggregate");
        const endTime = Date.now();
        const duration = endTime - startTime;

        // 验证返回的是最高优先级（urgent）的任务
        expect(nextJob).toBeTruthy();
        expect(nextJob?.priority).toBe("urgent");

        // 验证性能：聚合管道应该在 100ms 内完成（100个任务）
        // 注意：这是相对宽松的阈值，实际应该更快（< 50ms）
        expect(duration).toBeLessThan(200);

        // 验证返回的任务确实存在
        const jobIds = jobs.map((j) => j.id);
        expect(jobIds).toContain(nextJob?.id);
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

    it("应该正确处理延迟任务的聚合查询", async () => {
      if (!adapter) {
        console.log("⚠️  MongoDB 不可用，跳过测试");
        return;
      }

      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-mongodb-delay-aggregate", {
        concurrency: 1,
      });

      try {
        // 添加多个延迟任务
        const immediateJob = await queue.add("immediate", { data: "now" }, {
          delay: 0,
        });
        const delayedJob1 = await queue.add("delayed-1", { data: "later1" }, {
          delay: 2000, // 2秒后
        });
        const delayedJob2 = await queue.add("delayed-2", { data: "later2" }, {
          delay: 1000, // 1秒后
        });

        // 等待任务添加
        await new Promise((resolve) => setTimeout(resolve, 500));

        // 立即获取任务（应该只返回无延迟的任务）
        const nextJob = await adapter.getNext("test-mongodb-delay-aggregate");

        // 验证返回的是无延迟的任务
        expect(nextJob).toBeTruthy();
        expect(nextJob?.id).toBe(immediateJob.id);
        expect(nextJob?.name).toBe("immediate");

        // 更新第一个任务的状态为 completed，确保它不会影响后续查询
        await adapter.update(immediateJob.id, {
          status: "completed",
          completedAt: Date.now(),
        });

        // 立即尝试获取（应该返回 null，因为所有延迟任务都未到期）
        const nextJobBeforeDelay = await adapter.getNext(
          "test-mongodb-delay-aggregate",
        );
        expect(nextJobBeforeDelay).toBeNull();

        // 等待 1.2 秒后，delayed-2 的延迟应该已到期（1000ms < 1200ms）
        // delayed-1 延迟 2000ms，还未到期，不应该被获取
        await new Promise((resolve) => setTimeout(resolve, 1200));
        const nextJob2 = await adapter.getNext("test-mongodb-delay-aggregate");

        // 验证返回了任务（延迟已到期的任务）
        expect(nextJob2).toBeTruthy();
        // 验证返回的任务是 delayed-2（延迟 1000ms，已到期）
        // delayed-1 延迟 2000ms，还未到期，不应该被返回
        expect(nextJob2?.name).toBe("delayed-2");
        expect(nextJob2?.id).toBe(delayedJob2.id);
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
    }, {
      sanitizeOps: false,
      sanitizeResources: false,
    });

    it("应该使用 findOneAndUpdate 原子操作", async () => {
      if (!adapter) {
        console.log("⚠️  MongoDB 不可用，跳过测试");
        return;
      }

      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-mongodb-atomic", {
        concurrency: 1,
      });

      try {
        // 添加 10 个任务
        const jobs: any[] = [];
        for (let i = 0; i < 10; i++) {
          const job = await queue.add(`job-${i}`, { data: `data-${i}` });
          jobs.push(job);
        }

        // 等待任务添加
        await new Promise((resolve) => setTimeout(resolve, 500));

        // 并发调用 getNext()（模拟多进程/多线程场景）
        const promises = [];
        for (let i = 0; i < 5; i++) {
          promises.push(adapter.getNext("test-mongodb-atomic"));
        }

        const results = await Promise.all(promises);

        // 验证：应该返回不同的任务（原子操作保证不会返回同一个任务）
        const returnedJobIds = results
          .filter((job) => job !== null)
          .map((job) => job!.id);

        // 验证返回的任务 ID 都是唯一的
        const uniqueIds = new Set(returnedJobIds);
        expect(uniqueIds.size).toBe(returnedJobIds.length);

        // 验证返回的任务数量（最多 5 个，因为并发调用 5 次）
        expect(returnedJobIds.length).toBeLessThanOrEqual(5);
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

    it("应该在聚合管道返回空结果时返回 null", async () => {
      if (!adapter) {
        console.log("⚠️  MongoDB 不可用，跳过测试");
        return;
      }

      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test-mongodb-empty", {
        concurrency: 1,
      });

      try {
        // 添加任务并立即标记为 processing（模拟所有任务都在处理中）
        const job = await queue.add("processing-job", { data: "test" });

        // 等待任务添加
        await new Promise((resolve) => setTimeout(resolve, 500));

        // 手动更新任务状态为 processing
        await adapter.update(job.id, { status: "processing" });

        // 获取下一个任务（应该返回 null，因为没有 pending 任务）
        const nextJob = await adapter.getNext("test-mongodb-empty");

        // 验证返回 null
        expect(nextJob).toBeNull();
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
});
