/**
 * @fileoverview 提现防重复和防恶意并发攻击的完整方案
 *
 * 针对恶意并发攻击提现不同金额的场景，提供多层防护
 */

import { MongoClient } from "mongodb";
import { Redis } from "npm:ioredis";
// 注意：需要先安装 @dreamer/utils 依赖
// deno add jsr:@dreamer/utils
// 这里使用相对路径作为示例，实际使用时请使用 jsr:@dreamer/utils/lock
import { lockKey, withLock } from "../../utils/src/lock.ts";

// MongoDB 连接
const mongoClient = new MongoClient("mongodb://localhost:27017");
await mongoClient.connect();
const db = mongoClient.db("finance");
const withdrawalsCollection = db.collection("withdrawals");
const accountsCollection = db.collection("accounts");

// Redis 连接（用于分布式锁和限流）
const redis = new Redis("redis://localhost:6379");

/**
 * 方案1：MongoDB 唯一索引 + 分布式锁 + 账户余额检查
 * 这是最可靠的方案
 */
async function withdrawWithFullProtection(
  userId: string,
  amount: number,
  orderNo: string,
  idempotencyKey?: string,
) {
  // ========== 第一层：幂等性令牌（如果提供）==========
  if (idempotencyKey) {
    const key = `idempotency:withdraw:${idempotencyKey}`;
    const cached = await redis.get(key);
    if (cached) {
      return JSON.parse(cached);
    }
  }

  // ========== 第二层：分布式锁（防止并发）==========
  // 使用封装的分布式锁工具
  return await withLock(
    redis,
    lockKey("withdraw", userId),
    async () => {
      // ========== 第三层：MongoDB 唯一索引检查 ==========
      // 检查是否已存在该订单（数据库唯一索引保证）
      const existing = await withdrawalsCollection.findOne({
        userId,
        orderNo,
      });

      if (existing) {
        const result = { success: true, message: "已处理", data: existing };
        if (idempotencyKey) {
          await redis.setex(
            `idempotency:withdraw:${idempotencyKey}`,
            86400,
            JSON.stringify(result),
          );
        }
        return result;
      }

      // ========== 第四层：账户余额检查（在事务中）==========
      const session = mongoClient.startSession();

      try {
        await session.withTransaction(async () => {
          // 使用 findOneAndUpdate 原子性更新账户余额
          const account = await accountsCollection.findOneAndUpdate(
            {
              userId,
              balance: { $gte: amount }, // 余额必须足够
            },
            {
              $inc: { balance: -amount }, // 扣减余额
              $set: { updatedAt: new Date() },
            },
            {
              session,
              returnDocument: "after", // 返回更新后的文档
            },
          );

          if (!account || !account.value) {
            throw new Error("余额不足或账户不存在");
          }

          // 插入提现记录（唯一索引会防止重复）
          await withdrawalsCollection.insertOne(
            {
              userId,
              orderNo,
              amount,
              status: "completed",
              createdAt: new Date(),
              updatedAt: new Date(),
            },
            { session },
          );

          return account.value;
        });
      } finally {
        await session.endSession();
      }

      const result = {
        success: true,
        message: "提现成功",
        data: { userId, orderNo, amount },
      };

      // 缓存结果（幂等性）
      if (idempotencyKey) {
        await redis.setex(
          `idempotency:withdraw:${idempotencyKey}`,
          86400,
          JSON.stringify(result),
        );
      }

      return result;
    },
    {
      ttl: 10, // 10秒过期
      errorMessage: "操作正在进行中，请稍后重试",
    },
  );
}

/**
 * 方案2：时间窗口限流 + 金额限制
 * 防止短时间内多次提现不同金额
 */
async function withdrawWithRateLimit(
  userId: string,
  amount: number,
  orderNo: string,
) {
  const now = Date.now();
  const windowMs = 60000; // 1分钟时间窗口
  const maxAmount = 10000; // 1分钟内最大提现金额
  const maxCount = 5; // 1分钟内最大提现次数

  // 限流键
  const rateLimitKey = `ratelimit:withdraw:${userId}`;
  const amountKey = `amount:withdraw:${userId}`;
  const countKey = `count:withdraw:${userId}`;

  // 检查时间窗口内的提现金额
  const windowAmount = await redis.get(amountKey);
  const windowCount = await redis.get(countKey);

  if (windowAmount && parseInt(windowAmount) + amount > maxAmount) {
    throw new Error(
      `1分钟内累计提现金额不能超过 ${maxAmount}，当前已提现 ${windowAmount}`,
    );
  }

  if (windowCount && parseInt(windowCount) >= maxCount) {
    throw new Error(`1分钟内最多提现 ${maxCount} 次`);
  }

  // 执行提现（使用方案1的完整防护）
  const result = await withdrawWithFullProtection(userId, amount, orderNo);

  // 更新限流计数
  const pipeline = redis.pipeline();
  pipeline.incrby(amountKey, amount);
  pipeline.expire(amountKey, Math.ceil(windowMs / 1000));
  pipeline.incr(countKey);
  pipeline.expire(countKey, Math.ceil(windowMs / 1000));
  await pipeline.exec();

  return result;
}

/**
 * 方案3：使用 MongoDB 唯一索引 + 状态机
 * 适合需要更细粒度控制的场景
 */
async function withdrawWithStateMachine(
  userId: string,
  amount: number,
  orderNo: string,
) {
  const session = mongoClient.startSession();

  try {
    let result;

    await session.withTransaction(async () => {
      // 1. 尝试创建提现记录（状态为 pending）
      // 唯一索引：userId + orderNo 防止重复
      try {
        await withdrawalsCollection.insertOne(
          {
            userId,
            orderNo,
            amount,
            status: "pending",
            createdAt: new Date(),
          },
          { session },
        );
      } catch (error: any) {
        // 如果是重复键错误，说明已存在
        if (error.code === 11000) {
          const existing = await withdrawalsCollection.findOne(
            { userId, orderNo },
            { session },
          );
          if (existing?.status === "completed") {
            result = { success: true, message: "已处理", data: existing };
            return;
          }
          throw new Error("订单已存在且正在处理中");
        }
        throw error;
      }

      // 2. 使用 CAS 更新状态为 processing（原子性）
      const updateResult = await withdrawalsCollection.updateOne(
        {
          userId,
          orderNo,
          status: "pending", // 只有 pending 状态才能更新
        },
        {
          $set: {
            status: "processing",
            startedAt: new Date(),
          },
        },
        { session },
      );

      if (updateResult.modifiedCount === 0) {
        // 状态不是 pending，说明已被其他请求处理
        const existing = await withdrawalsCollection.findOne(
          { userId, orderNo },
          { session },
        );
        if (existing?.status === "completed") {
          result = { success: true, message: "已处理", data: existing };
          return;
        }
        throw new Error("订单状态异常");
      }

      // 3. 检查并扣减账户余额
      const account = await accountsCollection.findOneAndUpdate(
        {
          userId,
          balance: { $gte: amount },
        },
        {
          $inc: { balance: -amount },
          $set: { updatedAt: new Date() },
        },
        {
          session,
          returnDocument: "after",
        },
      );

      if (!account || !account.value) {
        // 余额不足，更新状态为失败
        await withdrawalsCollection.updateOne(
          { userId, orderNo },
          {
            $set: {
              status: "failed",
              error: "余额不足",
              failedAt: new Date(),
            },
          },
          { session },
        );
        throw new Error("余额不足");
      }

      // 4. 更新状态为完成
      await withdrawalsCollection.updateOne(
        { userId, orderNo },
        {
          $set: {
            status: "completed",
            completedAt: new Date(),
          },
        },
        { session },
      );

      result = {
        success: true,
        message: "提现成功",
        data: { userId, orderNo, amount },
      };
    });

    return result!;
  } finally {
    await session.endSession();
  }
}

/**
 * 初始化数据库索引（应用启动时执行一次）
 */
async function initializeIndexes() {
  // 1. 提现记录唯一索引：用户ID + 订单号
  await withdrawalsCollection.createIndex(
    { userId: 1, orderNo: 1 },
    { unique: true, name: "idx_user_order_unique" },
  );

  // 2. 用户ID索引（用于查询用户的所有提现记录）
  await withdrawalsCollection.createIndex(
    { userId: 1, createdAt: -1 },
    { name: "idx_user_created" },
  );

  // 3. 状态索引（用于查询不同状态的提现）
  await withdrawalsCollection.createIndex(
    { status: 1, createdAt: -1 },
    { name: "idx_status_created" },
  );

  // 4. 账户表用户ID唯一索引
  await accountsCollection.createIndex(
    { userId: 1 },
    { unique: true, name: "idx_user_unique" },
  );

  console.log("数据库索引初始化完成");
}

// 使用示例
async function example() {
  // 初始化索引
  await initializeIndexes();

  try {
    // 方案1：完整防护（推荐）
    const result1 = await withdrawWithFullProtection(
      "user123",
      1000,
      "order001",
      "idempotency-key-123",
    );
    console.log("提现结果1:", result1);

    // 方案2：带限流
    const result2 = await withdrawWithRateLimit("user123", 500, "order002");
    console.log("提现结果2:", result2);

    // 方案3：状态机
    const result3 = await withdrawWithStateMachine("user123", 2000, "order003");
    console.log("提现结果3:", result3);
  } catch (error) {
    console.error("提现失败:", error);
  }
}

export {
  initializeIndexes,
  withdrawWithFullProtection,
  withdrawWithRateLimit,
  withdrawWithStateMachine,
};
