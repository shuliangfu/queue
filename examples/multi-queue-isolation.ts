/**
 * 多队列隔离示例
 *
 * 演示多个队列如何独立运行，互不阻塞
 */

import { MemoryQueueAdapter, QueueManager } from "../src/mod.ts";

// 创建内存队列适配器（用于示例，生产环境建议使用 RedisQueueAdapter 或 RabbitMQQueueAdapter）
const adapter = new MemoryQueueAdapter();

// 创建队列管理器
const queueManager = new QueueManager({
  adapter,
  autoRecover: true,
  recoverTimeout: 30000,
});

// 创建三个不同类型的队列，每个队列有不同的并发数
const emailQueue = queueManager.createQueue("email", {
  concurrency: 5, // 邮件队列最多5个并发
  retry: 3,
});

const imageQueue = queueManager.createQueue("image", {
  concurrency: 3, // 图片处理队列最多3个并发
  retry: 2,
});

const reportQueue = queueManager.createQueue("report", {
  concurrency: 2, // 报表生成队列最多2个并发
  retry: 1,
});

// 邮件队列处理器（模拟快速任务）
emailQueue.process(async (job) => {
  console.log(`[邮件队列] 开始处理任务: ${job.name}`, job.data);

  // 模拟邮件发送（快速任务，100ms）
  await new Promise((resolve) => setTimeout(resolve, 100));

  console.log(`[邮件队列] ✅ 完成任务: ${job.name}`);
});

// 图片处理队列处理器（模拟慢速任务）
imageQueue.process(async (job) => {
  console.log(`[图片队列] 开始处理任务: ${job.name}`, job.data);

  // 模拟图片处理（慢速任务，2秒）
  await new Promise((resolve) => setTimeout(resolve, 2000));

  console.log(`[图片队列] ✅ 完成任务: ${job.name}`);
});

// 报表生成队列处理器（模拟非常慢的任务）
reportQueue.process(async (job) => {
  console.log(`[报表队列] 开始处理任务: ${job.name}`, job.data);

  // 模拟报表生成（非常慢的任务，5秒）
  await new Promise((resolve) => setTimeout(resolve, 5000));

  console.log(`[报表队列] ✅ 完成任务: ${job.name}`);
});

// 添加任务到不同队列
console.log("=== 添加任务到不同队列 ===");

// 添加多个邮件任务（应该快速处理，不会阻塞）
for (let i = 1; i <= 3; i++) {
  await emailQueue.add(`send-email-${i}`, { userId: i });
}

// 添加图片处理任务（慢速任务，但不会阻塞邮件队列）
for (let i = 1; i <= 2; i++) {
  await imageQueue.add(`resize-image-${i}`, { imageId: i });
}

// 添加报表生成任务（非常慢的任务，但不会阻塞其他队列）
await reportQueue.add("generate-report", { reportId: 1 });

console.log("\n=== 所有任务已添加，开始处理 ===");
console.log("注意观察：不同队列的任务会并行处理，互不阻塞\n");

// 等待一段时间，观察输出
await new Promise((resolve) => setTimeout(resolve, 10000));

// 获取各队列的统计信息
console.log("\n=== 队列统计信息 ===");
const emailStats = await emailQueue.getStats();
const imageStats = await imageQueue.getStats();
const reportStats = await reportQueue.getStats();

console.log("邮件队列:", emailStats);
console.log("图片队列:", imageStats);
console.log("报表队列:", reportStats);

// 关闭管理器
await queueManager.close();
