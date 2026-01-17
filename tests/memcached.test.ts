/**
 * @fileoverview Memcached 队列适配器测试
 */

import { IS_DENO } from "@dreamer/runtime-adapter";
import { describe, expect, it } from "@dreamer/test";
import { QueueManager } from "../src/mod.ts";
import { checkDockerContainer } from "./helpers.ts";

describe("Queue > MemcachedQueueAdapter", () => {
  it("应该检查 Memcached 容器是否运行", async () => {
    const isRunning = await checkDockerContainer("memcached");
    if (!isRunning) {
      console.log("⚠️  Memcached 容器未运行，跳过 Memcached 测试");
      console.log(
        "   启动 Memcached: docker run -d -p 11211:11211 --name memcached memcached:latest",
      );
      return;
    }
    expect(isRunning).toBeTruthy();
  });

  it("应该创建 Memcached 队列适配器", async () => {
    const isRunning = await checkDockerContainer("memcached");
    if (!isRunning) {
      console.log("⚠️  跳过：Memcached 容器未运行");
      return;
    }

    let adapter: any = null;
    try {
      const { MemcachedQueueAdapter } = await import(
        "../src/adapters/memcached.ts"
      );
      adapter = new MemcachedQueueAdapter({
        connection: {
          host: "127.0.0.1",
          port: 11211,
          timeout: 5000,
        },
      });
      await adapter.connect();
      expect(adapter).toBeTruthy();
    } catch (error) {
      console.log(
        `⚠️  跳过：无法创建 Memcached 适配器 - ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw error;
    } finally {
      if (adapter) {
        await adapter.disconnect();
        await new Promise((resolve) =>
          setTimeout(resolve, IS_DENO ? 1000 : 500)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Memcached 适配器添加和获取任务", async () => {
    const isRunning = await checkDockerContainer("memcached");
    if (!isRunning) {
      console.log("⚠️  跳过：Memcached 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { MemcachedQueueAdapter } = await import(
        "../src/adapters/memcached.ts"
      );
      adapter = new MemcachedQueueAdapter({
        connection: {
          host: "127.0.0.1",
          port: 11211,
          timeout: 5000,
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-memcached-add-get", {
        concurrency: 1,
      });

      const job = await queue.add("test-job", { data: "test" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 验证任务已添加
      const addedJob = await adapter.get(job.id);
      expect(addedJob).toBeTruthy();
      expect(addedJob?.id).toBe(job.id);
      expect(addedJob?.name).toBe("test-job");
      expect(addedJob?.data.data).toBe("test");
    } catch (error) {
      console.log(
        `⚠️  跳过：Memcached 测试失败 - ${
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
          setTimeout(resolve, IS_DENO ? 1000 : 500)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Memcached 适配器处理任务", async () => {
    const isRunning = await checkDockerContainer("memcached");
    if (!isRunning) {
      console.log("⚠️  跳过：Memcached 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { MemcachedQueueAdapter } = await import(
        "../src/adapters/memcached.ts"
      );
      adapter = new MemcachedQueueAdapter({
        connection: {
          host: "127.0.0.1",
          port: 11211,
          timeout: 5000,
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({
        adapter,
        autoRecover: false,
      });
      queue = queueManager.createQueue("test-memcached-process", {
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
        `⚠️  跳过：Memcached 测试失败 - ${
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
          setTimeout(resolve, IS_DENO ? 1000 : 500)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Memcached 适配器更新任务状态", async () => {
    const isRunning = await checkDockerContainer("memcached");
    if (!isRunning) {
      console.log("⚠️  跳过：Memcached 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { MemcachedQueueAdapter } = await import(
        "../src/adapters/memcached.ts"
      );
      adapter = new MemcachedQueueAdapter({
        connection: {
          host: "127.0.0.1",
          port: 11211,
          timeout: 5000,
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-memcached-update", {
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
        `⚠️  跳过：Memcached 测试失败 - ${
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
          setTimeout(resolve, IS_DENO ? 1000 : 500)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Memcached 适配器删除任务", async () => {
    const isRunning = await checkDockerContainer("memcached");
    if (!isRunning) {
      console.log("⚠️  跳过：Memcached 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { MemcachedQueueAdapter } = await import(
        "../src/adapters/memcached.ts"
      );
      adapter = new MemcachedQueueAdapter({
        connection: {
          host: "127.0.0.1",
          port: 11211,
          timeout: 5000,
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-memcached-remove", {
        concurrency: 1,
      });

      const job = await queue.add("test-job", { data: "test" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 验证任务存在
      const beforeDelete = await adapter.get(job.id);
      expect(beforeDelete).toBeTruthy();

      // 删除任务
      await adapter.remove(job.id);

      // 验证任务已删除
      const afterDelete = await adapter.get(job.id);
      expect(afterDelete).toBeNull();
    } catch (error) {
      console.log(
        `⚠️  跳过：Memcached 测试失败 - ${
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
          setTimeout(resolve, IS_DENO ? 1000 : 500)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Memcached 适配器获取所有任务", async () => {
    const isRunning = await checkDockerContainer("memcached");
    if (!isRunning) {
      console.log("⚠️  跳过：Memcached 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { MemcachedQueueAdapter } = await import(
        "../src/adapters/memcached.ts"
      );
      adapter = new MemcachedQueueAdapter({
        connection: {
          host: "127.0.0.1",
          port: 11211,
          timeout: 5000,
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-memcached-getall", {
        concurrency: 1,
      });

      const job1 = await queue.add("job1", { data: "data1" });
      const job2 = await queue.add("job2", { data: "data2" });
      const job3 = await queue.add("job3", { data: "data3" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 获取所有任务
      const allJobs = await adapter.getAll("test-memcached-getall");

      expect(allJobs.length).toBeGreaterThanOrEqual(3);
      const jobIds = allJobs.map((j: any) => j.id);
      expect(jobIds).toContain(job1.id);
      expect(jobIds).toContain(job2.id);
      expect(jobIds).toContain(job3.id);
    } catch (error) {
      console.log(
        `⚠️  跳过：Memcached 测试失败 - ${
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
          setTimeout(resolve, IS_DENO ? 1000 : 500)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Memcached 适配器清空队列", async () => {
    const isRunning = await checkDockerContainer("memcached");
    if (!isRunning) {
      console.log("⚠️  跳过：Memcached 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { MemcachedQueueAdapter } = await import(
        "../src/adapters/memcached.ts"
      );
      adapter = new MemcachedQueueAdapter({
        connection: {
          host: "127.0.0.1",
          port: 11211,
          timeout: 5000,
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-memcached-clear", {
        concurrency: 1,
      });

      // 添加任务
      await queue.add("job1", { data: "data1" });
      await queue.add("job2", { data: "data2" });

      // 等待任务添加
      await new Promise((resolve) => setTimeout(resolve, 500));

      // 验证任务存在
      let allJobs = await adapter.getAll("test-memcached-clear");
      expect(allJobs.length).toBeGreaterThanOrEqual(2);

      // 清空队列
      await adapter.clear("test-memcached-clear");

      // 验证队列已清空
      allJobs = await adapter.getAll("test-memcached-clear");
      expect(allJobs.length).toBe(0);
    } catch (error) {
      console.log(
        `⚠️  跳过：Memcached 测试失败 - ${
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
          setTimeout(resolve, IS_DENO ? 1000 : 500)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });

  it("应该使用 Memcached 适配器获取队列统计信息", async () => {
    const isRunning = await checkDockerContainer("memcached");
    if (!isRunning) {
      console.log("⚠️  跳过：Memcached 容器未运行");
      return;
    }

    let adapter: any = null;
    let queueManager: any = null;
    let queue: any = null;
    try {
      const { MemcachedQueueAdapter } = await import(
        "../src/adapters/memcached.ts"
      );
      adapter = new MemcachedQueueAdapter({
        connection: {
          host: "127.0.0.1",
          port: 11211,
          timeout: 5000,
        },
      });
      await adapter.connect();

      queueManager = new QueueManager({ adapter, autoRecover: false });
      queue = queueManager.createQueue("test-memcached-stats", {
        concurrency: 1,
      });

      // 添加任务
      const job1 = await queue.add("job1", { data: "data1" });
      const job2 = await queue.add("job2", { data: "data2" });
      const job3 = await queue.add("job3", { data: "data3" });
      const job4 = await queue.add("job4", { data: "data4" });

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
      const stats = await adapter.getStats("test-memcached-stats");

      expect(stats).toBeTruthy();
      expect(stats.pending).toBeGreaterThanOrEqual(1);
      expect(stats.processing).toBeGreaterThanOrEqual(1);
      expect(stats.completed).toBeGreaterThanOrEqual(1);
      expect(stats.failed).toBeGreaterThanOrEqual(1);
    } catch (error) {
      console.log(
        `⚠️  跳过：Memcached 测试失败 - ${
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
          setTimeout(resolve, IS_DENO ? 1000 : 500)
        );
      }
    }
  }, {
    sanitizeOps: false,
    sanitizeResources: false,
  });
});
