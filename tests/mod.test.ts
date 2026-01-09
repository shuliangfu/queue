/**
 * @fileoverview Queue 基础测试（MemoryQueueAdapter）
 */

import { IS_DENO } from "@dreamer/runtime-adapter";
import { describe, expect, it } from "@dreamer/test";
import { MemoryQueueAdapter, QueueManager } from "../src/mod.ts";

describe("Queue", () => {
  describe("MemoryQueueAdapter", () => {
    it("应该创建内存队列适配器", () => {
      const adapter = new MemoryQueueAdapter();
      expect(adapter).toBeTruthy();
    });

    it("应该添加和获取任务", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      await queue.add("test-job", { data: "test" });
      const stats = await queue.getStats();
      expect(stats.pending).toBe(1);

      await queueManager.close();
    });

    it("应该处理任务", async () => {
      const adapter = new MemoryQueueAdapter();
      const queueManager = new QueueManager({ adapter, autoRecover: false });
      const queue = queueManager.createQueue("test", { concurrency: 1 });

      let processed = false;
      queue.process(async (job) => {
        expect(job.name).toBe("test-job");
        expect(job.data.data).toBe("test");
        processed = true;
      });

      await queue.add("test-job", { data: "test" });

      // 等待任务处理
      // 在 Deno 环境下，需要等待更长时间以确保所有定时器完成
      await new Promise((resolve) => setTimeout(resolve, IS_DENO ? 1000 : 100));
      expect(processed).toBeTruthy();

      await queueManager.close();
    });
  });
});
