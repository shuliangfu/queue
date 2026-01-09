/**
 * @fileoverview 测试辅助函数
 */

import { createCommand, IS_DENO } from "@dreamer/runtime-adapter";

/**
 * 检查 Docker 容器是否运行
 * @param name 容器名称
 * @returns 如果容器正在运行返回 true，否则返回 false
 */
export async function checkDockerContainer(name: string): Promise<boolean> {
  let command: any = null;
  try {
    command = createCommand("docker", {
      args: ["ps", "--filter", `name=${name}`, "--format", "{{.Names}}"],
      stdout: "piped",
      stderr: "piped",
    });
    const outputResult = await command.output();

    // 在 Deno 环境下，需要显式关闭子进程的 stdout 和 stderr 以避免资源泄漏
    if (IS_DENO && command) {
      try {
        // 在 Deno 环境下，command.stdout 和 command.stderr 是 ReadableStream
        // 需要调用 cancel() 来关闭流
        if (command.stdout) {
          await command.stdout.cancel();
        }
      } catch {
        // 忽略取消错误（流可能已经关闭）
      }
      try {
        if (command.stderr) {
          await command.stderr.cancel();
        }
      } catch {
        // 忽略取消错误（流可能已经关闭）
      }
      // 确保子进程被关闭
      try {
        command.kill();
      } catch {
        // 忽略 kill 错误（进程可能已经完成）
      }
    }

    if (!outputResult.success) {
      return false;
    }
    const output = new TextDecoder().decode(outputResult.stdout).trim();
    return output.includes(name);
  } catch {
    // 确保在出错时也关闭子进程
    if (IS_DENO && command) {
      try {
        command.kill();
      } catch {
        // 忽略 kill 错误
      }
    }
    return false;
  }
}
