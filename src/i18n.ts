/**
 * @module @dreamer/queue/i18n
 *
 * i18n for @dreamer/queue: adapter and manager error messages.
 * Uses $tr + module instance, no install(); locale auto-detected from env.
 */

import {
  createI18n,
  type I18n,
  type TranslationData,
  type TranslationParams,
} from "@dreamer/i18n";
import { getEnv } from "@dreamer/runtime-adapter";
import enUS from "./locales/en-US.json" with { type: "json" };
import zhCN from "./locales/zh-CN.json" with { type: "json" };

export type Locale = "en-US" | "zh-CN";

export const DEFAULT_LOCALE: Locale = "en-US";

const QUEUE_LOCALES: Locale[] = ["en-US", "zh-CN"];

const LOCALE_DATA: Record<string, TranslationData> = {
  "en-US": enUS as TranslationData,
  "zh-CN": zhCN as TranslationData,
};

let queueI18n: I18n | null = null;

/**
 * Detect locale from env (LANGUAGE > LC_ALL > LANG).
 */
export function detectLocale(): Locale {
  const langEnv = getEnv("LANGUAGE") || getEnv("LC_ALL") || getEnv("LANG");
  if (!langEnv) return DEFAULT_LOCALE;
  const first = langEnv.split(/[:\s]/)[0]?.trim();
  if (!first) return DEFAULT_LOCALE;
  const match = first.match(/^([a-z]{2})[-_]([A-Z]{2})/i);
  if (match) {
    const normalized = `${match[1].toLowerCase()}-${
      match[2].toUpperCase()
    }` as Locale;
    if (QUEUE_LOCALES.includes(normalized)) return normalized;
  }
  const primary = first.substring(0, 2).toLowerCase();
  if (primary === "zh") return "zh-CN";
  if (primary === "en") return "en-US";
  return DEFAULT_LOCALE;
}

/** 内部初始化，导入 i18n 时自动执行，不导出 */
function initQueueI18n(): void {
  if (queueI18n) return;
  const i18n = createI18n({
    defaultLocale: DEFAULT_LOCALE,
    fallbackBehavior: "default",
    locales: [...QUEUE_LOCALES],
    translations: LOCALE_DATA as Record<string, TranslationData>,
  });
  i18n.setLocale(detectLocale());
  queueI18n = i18n;
}

initQueueI18n();

/**
 * Set locale for queue messages. Initializes i18n if not yet called.
 */
export function setQueueLocale(locale: Locale): void {
  if (!queueI18n) initQueueI18n();
  queueI18n!.setLocale(locale);
}

/**
 * Translate by key. When init not called, returns key.
 */
export function $tr(
  key: string,
  params?: Record<string, string | number>,
  lang?: Locale,
): string {
  if (!queueI18n) initQueueI18n();
  if (!queueI18n) return key;
  if (lang !== undefined) {
    const prev = queueI18n.getLocale();
    queueI18n.setLocale(lang);
    try {
      return queueI18n.t(key, params as TranslationParams);
    } finally {
      queueI18n.setLocale(prev);
    }
  }
  return queueI18n.t(key, params as TranslationParams);
}
