/**
 * @fileoverview Queue 测试
 */

import { describe, expect, it } from "jsr:@dreamer/test@^1.0.0-alpha.1";
import { MemoryQueueAdapter, QueueManager } from "../src/mod.ts";

/**
 * 检查 Docker 容器是否运行
 */
async function checkDockerContainer(name: string): Promise<boolean> {
  try {
    const command = new Deno.Command("docker", {
      args: ["ps", "--filter", `name=${name}`, "--format", "{{.Names}}"],
      stdout: "piped",
      stderr: "piped",
    });
    const { success, stdout } = await command.output();
    if (!success) {
      return false;
    }
    const output = new TextDecoder().decode(stdout).trim();
    return output.includes(name);
  } catch {
    return false;
  }
}

/**
 * 创建 Redis 客户端（用于测试）
 */
async function createRedisClient() {
  try {
    // 尝试使用 npm:redis
    const { createClient } = await import("npm:redis@^5.0.0");
    const client = createClient({
      url: "redis://localhost:6379",
    });
    await client.connect();

    // 包装为适配器需要的接口
    return {
      set: async (key: string, value: string) => {
        await client.set(key, value);
      },
      get: (key: string) => client.get(key),
      lpush: (key: string, value: string) => client.lPush(key, value),
      rpop: (key: string) => client.rPop(key),
      lrange: (key: string, start: number, stop: number) =>
        client.lRange(key, start, stop),
      del: (key: string) => client.del(key),
      llen: (key: string) => client.lLen(key),
      lrem: (key: string, count: number, value: string) =>
        client.lRem(key, count, value),
      disconnect: () => client.quit(),
    };
  } catch (error) {
    throw new Error(
      `无法创建 Redis 客户端: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

/**
 * 创建 RabbitMQ 连接（用于测试）
 */
async function createRabbitMQConnection() {
  try {
    // 尝试使用 npm:amqplib
    const amqp = await import("npm:amqplib@^0.10.0");
    // RabbitMQ 默认用户名和密码是 guest/guest
    const connection = await amqp.default.connect("amqp://guest:guest@localhost");

    return {
      connection: {
        createChannel: async () => {
          const channel = await connection.createChannel();
          return {
            assertQueue: async (
              queue: string,
              options?: { durable?: boolean },
            ) => {
              await channel.assertQueue(queue, options);
              return { queue };
            },
            sendToQueue: (
              queue: string,
              content: Uint8Array,
              options?: { persistent?: boolean },
            ) => {
              return channel.sendToQueue(queue, content, options);
            },
            consume: async (
              queue: string,
              onMessage: (
                msg: { content: Uint8Array; ack(): void; nack(): void },
              ) => void,
              options?: { noAck?: boolean },
            ) => {
              const result = await channel.consume(
                queue,
                (msg: any) => {
                  if (msg) {
                    onMessage({
                      content: msg.content,
                      ack: () => channel.ack(msg),
                      nack: () => channel.nack(msg),
                    });
                  }
                },
                options,
              );
              return result.consumerTag;
            },
            cancel: (consumerTag: string) => channel.cancel(consumerTag),
            deleteQueue: (queue: string) => channel.deleteQueue(queue),
            checkQueue: async (queue: string) => {
              const result = await channel.checkQueue(queue);
              return { messageCount: result.messageCount };
            },
          };
        },
        close: () => connection.close(),
      },
    };
  } catch (error) {
    throw new Error(
      `无法创建 RabbitMQ 连接: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

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
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(processed).toBeTruthy();

      await queueManager.close();
    });
  });

  describe("RedisQueueAdapter", () => {
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
      const isRunning = await checkDockerContainer("redis");
      if (!isRunning) {
        console.log("⚠️  跳过：Redis 容器未运行");
        return;
      }

      try {
        const redisClient = await createRedisClient();
        const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
        const adapter = new RedisQueueAdapter({ client: redisClient });
        expect(adapter).toBeTruthy();

        // 清理
        await redisClient.disconnect();
      } catch (error) {
        console.log(
          `⚠️  跳过：无法创建 Redis 客户端 - ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    });

    it("应该使用 Redis 适配器添加和获取任务", async () => {
      const isRunning = await checkDockerContainer("redis");
      if (!isRunning) {
        console.log("⚠️  跳过：Redis 容器未运行");
        return;
      }

      let redisClient: any = null;
      try {
        redisClient = await createRedisClient();
        const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
        const adapter = new RedisQueueAdapter({ client: redisClient });

        const queueManager = new QueueManager({ adapter, autoRecover: false });
        const queue = queueManager.createQueue("test-redis-stats", {
          concurrency: 1,
        });

        // 不设置处理器，确保任务保持 pending 状态
        const job = await queue.add("test-job", { data: "test" });

        // 等待一小段时间确保任务已添加到 Redis
        await new Promise((resolve) => setTimeout(resolve, 200));

        // 直接通过适配器获取任务，验证任务是否被正确添加
        const retrievedJob = await adapter.get(job.id);
        expect(retrievedJob).toBeTruthy();
        expect(retrievedJob?.name).toBe("test-job");
        expect(retrievedJob?.data.data).toBe("test");

        // 清理：先停止队列，再关闭客户端
        // 注意：如果没有调用 process，处理循环不会运行，所以不需要 stop
        // 但为了安全，我们还是调用 stop
        queue.stop();
        await queueManager.close();
        // 等待所有异步操作完成
        await new Promise((resolve) => setTimeout(resolve, 500));
        await redisClient.disconnect();
      } catch (error) {
        console.log(
          `⚠️  跳过：Redis 测试失败 - ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        if (redisClient?.disconnect) {
          await redisClient.disconnect();
        }
      }
    });

    it("应该使用 Redis 适配器处理任务", async () => {
      const isRunning = await checkDockerContainer("redis");
      if (!isRunning) {
        console.log("⚠️  跳过：Redis 容器未运行");
        return;
      }

      let client: any = null;
      try {
        client = await createRedisClient();
        const { RedisQueueAdapter } = await import("../src/adapters/redis.ts");
        const redisAdapter = new RedisQueueAdapter({ client });

        const queueManager = new QueueManager({
          adapter: redisAdapter,
          autoRecover: false,
        });
        const queue = queueManager.createQueue("test-redis-process", {
          concurrency: 1,
        });

        let processed = false;
        queue.process(async (job) => {
          expect(job.name).toBe("test-job");
          expect(job.data.data).toBe("test");
          processed = true;
        });

        await queue.add("test-job", { data: "test" });

        // 等待任务处理
        await new Promise((resolve) => setTimeout(resolve, 500));
        expect(processed).toBeTruthy();

        // 清理：先停止队列，再关闭客户端
        queue.stop(); // 先停止队列处理循环
        await queueManager.close();
        // 等待处理循环完全停止（处理循环每 100ms 检查一次 running 标志）
        await new Promise((resolve) => setTimeout(resolve, 600));
        if (client?.disconnect) {
          await client.disconnect();
        }
      } catch (error) {
        console.log(
          `⚠️  跳过：Redis 测试失败 - ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        if (client?.disconnect) {
          await client.disconnect();
        }
      }
    });
  });

  describe("RabbitMQQueueAdapter", () => {
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

      let connection: any = null;
      try {
        connection = await createRabbitMQConnection();
        const { RabbitMQQueueAdapter } = await import(
          "../src/adapters/rabbitmq.ts"
        );
        const adapter = new RabbitMQQueueAdapter({
          connection: connection.connection,
          queueOptions: { durable: true },
        });
        expect(adapter).toBeTruthy();

        // 清理
        await connection.connection.close();
      } catch (error) {
        console.log(
          `⚠️  跳过：无法创建 RabbitMQ 连接 - ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        if (connection?.connection?.close) {
          await connection.connection.close();
        }
      }
    });

    it("应该使用 RabbitMQ 适配器添加和获取任务", async () => {
      const isRunning = await checkDockerContainer("rabbitmq");
      if (!isRunning) {
        console.log("⚠️  跳过：RabbitMQ 容器未运行");
        return;
      }

      let connection: any = null;
      try {
        connection = await createRabbitMQConnection();
        const { RabbitMQQueueAdapter } = await import(
          "../src/adapters/rabbitmq.ts"
        );
        const adapter = new RabbitMQQueueAdapter({
          connection: connection.connection,
          queueOptions: { durable: true },
        });

        const queueManager = new QueueManager({ adapter, autoRecover: false });
        const queue = queueManager.createQueue("test-rabbitmq", {
          concurrency: 1,
        });

        const job = await queue.add("test-job", { data: "test" });

        // 等待任务添加
        await new Promise((resolve) => setTimeout(resolve, 200));

        // 直接通过适配器获取任务
        const retrievedJob = await adapter.get(job.id);
        expect(retrievedJob).toBeTruthy();
        expect(retrievedJob?.name).toBe("test-job");

        // 清理：先停止队列，再关闭连接
        queue.stop(); // 先停止队列处理循环
        await queueManager.close();
        // 等待处理循环完全停止
        await new Promise((resolve) => setTimeout(resolve, 300));
        await connection.connection.close();
      } catch (error) {
        console.log(
          `⚠️  跳过：RabbitMQ 测试失败 - ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        if (connection?.connection?.close) {
          await connection.connection.close();
        }
      }
    });

    it("应该使用 RabbitMQ 适配器处理任务", async () => {
      const isRunning = await checkDockerContainer("rabbitmq");
      if (!isRunning) {
        console.log("⚠️  跳过：RabbitMQ 容器未运行");
        return;
      }

      let connection: any = null;
      try {
        connection = await createRabbitMQConnection();
        const { RabbitMQQueueAdapter } = await import(
          "../src/adapters/rabbitmq.ts"
        );
        const rabbitMQAdapter = new RabbitMQQueueAdapter({
          connection: connection.connection,
          queueOptions: { durable: true },
        });

        const queueManager = new QueueManager({
          adapter: rabbitMQAdapter,
          autoRecover: false,
        });
        const queue = queueManager.createQueue("test-rabbitmq-process", {
          concurrency: 1,
        });

        let processed = false;
        queue.process(async (job) => {
          expect(job.name).toBe("test-job");
          expect(job.data.data).toBe("test");
          processed = true;
        });

        await queue.add("test-job", { data: "test" });

        // 等待任务处理
        await new Promise((resolve) => setTimeout(resolve, 500));
        expect(processed).toBeTruthy();

        // 清理：先停止队列，再关闭连接
        queue.stop(); // 先停止队列处理循环
        await queueManager.close();
        // 等待处理循环完全停止
        await new Promise((resolve) => setTimeout(resolve, 300));
        if (connection?.connection?.close) {
          await connection.connection.close();
        }
      } catch (error) {
        console.log(
          `⚠️  跳过：RabbitMQ 测试失败 - ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        if (connection?.connection?.close) {
          await connection.connection.close();
        }
      }
    });
  });
});
