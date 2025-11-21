import { TelegramClient, Api } from "telegram";
import * as fs from "fs";
import * as path from "path";
import { getTelegramClient } from "./client";
import { MatchingMethod } from "../models/videoJob";
import {
  reserveSyntaxMessageId,
  getAllReservedSyntaxMessageIds,
} from "../firebase/syntaxAssignmentsService";

export interface SendPromptOptions {
  customFileName?: string;
  jobId: string;
  jobCreatedAt: number;
  requestMessageId?: number;
}

export interface SyntxResult {
  localPath: string;
  requestMessageId: number;
  videoMessageId: number;
  matchingMethod: MatchingMethod;
  totalMessagesScanned: number;
}

const POLL_INTERVAL_MS = Number(process.env.SYNTX_POLL_INTERVAL_MS || 7000);
const FALLBACK_POLLS_THRESHOLD = Number(process.env.SYNTX_FALLBACK_POLLS || 3);
const FALLBACK_WINDOW_MS = Number(
  process.env.SYNTX_FALLBACK_WINDOW_MS || 2 * 60 * 1000
);
const DEFAULT_TIMEOUT_MS = 30 * 60 * 1000;

type MatchingResult = {
  message: Api.Message;
  matchingMethod: MatchingMethod;
  totalMessagesScanned: number;
};

type PendingJobInfo = {
  jobId: string;
  requestMessageId: number;
  jobCreatedAt: number;
  sentAt: number;
  deadline: number;
  pollCount: number;
  resolve: (result: MatchingResult) => void;
  reject: (reason?: any) => void;
};

type VideoMessageInfo = {
  message: Api.Message;
  text: string;
  jobIdMark: string | null;
  dateMs: number;
};

const pendingJobs = new Map<string, PendingJobInfo>();
const reservedMessageIds = new Set<number>();
let currentClient: TelegramClient | null = null;
let currentChat: Api.TypeEntityLike | null = null;
let pollerRunning = false;
let assignmentsCacheLoaded = false;
let totalMessagesScanned = 0;

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function addJobIdToPrompt(prompt: string, jobId: string): string {
  const marker = `[JOB_ID: ${jobId}]`;
  return `${prompt}\n\n${marker}`;
}

function extractJobIdFromText(text: string): string | null {
  const match = text.match(/\[JOB_ID:\s*([^\]]+)\]/);
  return match ? match[1].trim() : null;
}

function getMessageFullText(message: Api.Message): string {
  const parts: string[] = [];
  if (typeof message.message === "string") {
    parts.push(message.message);
  }
  const mediaCaption = (message.media as any)?.caption;
  if (typeof mediaCaption === "string") {
    parts.push(mediaCaption);
  } else if (mediaCaption && typeof mediaCaption === "object" && "text" in mediaCaption) {
    parts.push(mediaCaption.text || "");
  }
  return parts.join(" ").trim();
}

function isVideoMessage(message: Api.Message): boolean {
  if (!message.media) return false;
  if (message.media instanceof Api.MessageMediaDocument) {
    const document = message.media.document;
    if (document instanceof Api.Document) {
      return document.attributes.some(
        (attr) => attr instanceof Api.DocumentAttributeVideo
      );
    }
  }
  return false;
}

function logWithJob(jobId: string, message: string, extra?: Record<string, unknown>) {
  const context = extra ? ` | ${JSON.stringify(extra)}` : "";
  console.log(`[Syntx][jobId=${jobId}] ${message}${context}`);
}

function logPoll(message: string, extra?: Record<string, unknown>) {
  const context = extra ? ` | ${JSON.stringify(extra)}` : "";
  console.log(`[SyntxPoller] ${message}${context}`);
}

async function ensureAssignmentsCache() {
  if (assignmentsCacheLoaded) {
    return;
  }
  const ids = await getAllReservedSyntaxMessageIds();
  ids.forEach((id) => reservedMessageIds.add(id));
  assignmentsCacheLoaded = true;
}

export async function sendPromptToSyntx(
  prompt: string,
  options: SendPromptOptions
): Promise<SyntxResult> {
  const { customFileName, jobId, jobCreatedAt } = options;
  const client = await getTelegramClient();
  const botUsername = process.env.SYNTX_BOT_USERNAME || "syntxaibot";

  // Проверяем авторизацию
  const isAuthorized = await client.checkAuthorization();
  if (!isAuthorized) {
    throw new Error(
      "Telegram клиент не авторизован. Выполните авторизацию перед использованием."
    );
  }

  console.log(`[Syntx] LOG: подготовка отправки промпта (jobId=${jobId})`);
  const entity = await client.getEntity(botUsername);
  currentClient = client;
  currentChat = entity;

  let actualRequestMessageId: number;
  const promptWithJobId = addJobIdToPrompt(prompt, jobId);
  const sentMessage = await client.sendMessage(entity, { message: promptWithJobId });
  actualRequestMessageId = sentMessage.id;
  logWithJob(jobId, "LOG: промпт отправлен Syntax", {
    promptLength: prompt.length,
    requestMessageId: actualRequestMessageId,
  });

  const sentAt = Date.now();

  const matched = await waitForVideoViaPoller({
    jobId,
    requestMessageId: actualRequestMessageId,
    jobCreatedAt,
    sentAt,
  });

  const videoMessage = matched.message;

  // Подготавливаем директорию для загрузок
  const downloadRoot = process.env.DOWNLOAD_DIR || "./downloads";
  const downloadDir = path.resolve(downloadRoot);
  if (!fs.existsSync(downloadDir)) {
    fs.mkdirSync(downloadDir, { recursive: true });
    logWithJob(jobId, "LOG: создана директория загрузок", { downloadDir });
  }

  const fileName =
    customFileName || `syntx_${jobId}_${Date.now()}.mp4`;
  const filePath = path.join(downloadDir, fileName);

  logWithJob(jobId, "LOG: начало скачивания видео", {
    messageId: videoMessage.id,
    filePath,
  });

  try {
    await client.downloadMedia(videoMessage, { outputFile: filePath });
    await delay(500);
    if (!fs.existsSync(filePath)) {
      throw new Error(`Файл не создан: ${filePath}`);
    }
  } catch (error: any) {
    logWithJob(jobId, "ERROR: failed to download via file mode", {
      messageId: videoMessage.id,
      reason: error?.message || error,
    });
    try {
      const buffer = (await client.downloadMedia(videoMessage, {})) as Buffer;
      if (!buffer?.length) {
        throw new Error("downloadMedia buffer is empty");
      }
      fs.writeFileSync(filePath, buffer);
    } catch (bufferError: any) {
      logWithJob(jobId, "ERROR: failed to download via buffer mode", {
        messageId: videoMessage.id,
        reason: bufferError?.message || bufferError,
      });
      throw new Error(
        `Failed to download fileId=${videoMessage.id} reason=${bufferError?.message || bufferError}`
      );
    }
  }

  const stat = fs.statSync(filePath);
  if (stat.size === 0) {
    throw new Error(`ERROR: downloaded file is empty (${filePath})`);
  }

  logWithJob(jobId, "LOG: видео успешно скачано", {
    filePath,
    size: stat.size,
    matchingMethod: matched.matchingMethod,
    totalMessagesScanned: matched.totalMessagesScanned,
  });

  return {
    localPath: filePath,
    requestMessageId: actualRequestMessageId,
    videoMessageId: videoMessage.id,
    matchingMethod: matched.matchingMethod,
    totalMessagesScanned: matched.totalMessagesScanned,
  };
}

async function waitForVideoViaPoller(job: {
  jobId: string;
  requestMessageId: number;
  jobCreatedAt: number;
  sentAt: number;
}): Promise<MatchingResult> {
  if (!currentClient || !currentChat) {
    throw new Error("Telegram клиент или чат Syntax не инициализированы");
  }

  await ensureAssignmentsCache();

  const pendingJob: PendingJobInfo = {
    jobId: job.jobId,
    requestMessageId: job.requestMessageId,
    jobCreatedAt: job.jobCreatedAt,
    sentAt: job.sentAt,
    deadline: Date.now() + DEFAULT_TIMEOUT_MS,
    pollCount: 0,
    resolve: () => {},
    reject: () => {},
  };

  const promise = new Promise<MatchingResult>((resolve, reject) => {
    pendingJob.resolve = resolve;
    pendingJob.reject = reject;
  });

  pendingJobs.set(job.jobId, pendingJob);
  logWithJob(job.jobId, "LOG: задача зарегистрирована в poller", {
    pendingJobs: pendingJobs.size,
  });

  ensurePolling();

  return promise;
}

function ensurePolling() {
  if (pollerRunning) {
    return;
  }
  pollerRunning = true;
  pollLoop().catch((error) => {
    console.error("[SyntxPoller] Fatal error:", error);
  });
}

async function pollLoop() {
  while (pendingJobs.size > 0) {
    try {
      await pollOnce();
    } catch (error) {
      console.error("[SyntxPoller] Ошибка при pollOnce:", error);
    }
    await delay(POLL_INTERVAL_MS);
  }
  pollerRunning = false;
}

async function pollOnce() {
  if (!currentClient || !currentChat) {
    logPoll("Пропуск pollOnce: нет активного клиента или чата");
    return;
  }

  const messages = await currentClient.getMessages(currentChat, {
    limit: 100,
  });
  totalMessagesScanned += messages.length;
  logPoll("LOG: получен batch сообщений", {
    batchSize: messages.length,
    pendingJobs: pendingJobs.size,
    totalMessagesScanned,
  });

  const videoMessages = filterVideoMessages(messages);
  await matchMessagesByJobId(videoMessages);

  const fallbackReadyJobs = collectFallbackReadyJobs();
  await processFallbackMatches(videoMessages, fallbackReadyJobs);

  checkPendingTimeouts();
}

function filterVideoMessages(messages: Api.Message[]): VideoMessageInfo[] {
  const videoMessages: VideoMessageInfo[] = [];
  for (const message of messages) {
    const text = getMessageFullText(message);
    const jobIdMark = extractJobIdFromText(text);
    const rawDate = message.date as unknown;
    let dateMs: number;
    if (typeof rawDate === "number") {
      dateMs = rawDate * 1000;
    } else if (rawDate instanceof Date) {
      dateMs = rawDate.getTime();
    } else {
      dateMs = Date.now();
    }
    const info: VideoMessageInfo = {
      message,
      text,
      jobIdMark,
      dateMs,
    };
    const preview = text ? text.substring(0, 120).replace(/\n/g, " ") : "";
    logPoll("LOG: scanning message", {
      messageId: message.id,
      hasVideo: isVideoMessage(message),
      jobIdMark: jobIdMark || "none",
      preview,
    });
    if (isVideoMessage(message)) {
      videoMessages.push(info);
    } else {
      logPoll("LOG: message skipped (no video)", { messageId: message.id });
    }
  }
  return videoMessages;
}

async function matchMessagesByJobId(videoMessages: VideoMessageInfo[]) {
  for (const info of videoMessages) {
    if (!info.jobIdMark) {
      logPoll("LOG: NO_MATCH (нет маркера jobId)", { messageId: info.message.id });
      continue;
    }

    const pending = pendingJobs.get(info.jobIdMark);
    if (!pending) {
      logPoll("LOG: маркер найден, но нет ожидающей задачи", {
        messageId: info.message.id,
        jobId: info.jobIdMark,
      });
      continue;
    }

    await attemptAssignment(pending, info, "jobId");
  }
}

function collectFallbackReadyJobs(): PendingJobInfo[] {
  const ready: PendingJobInfo[] = [];
  pendingJobs.forEach((job) => {
    job.pollCount += 1;
    if (job.pollCount >= FALLBACK_POLLS_THRESHOLD) {
      ready.push(job);
    }
  });
  return ready;
}

async function processFallbackMatches(
  videoMessages: VideoMessageInfo[],
  fallbackJobs: PendingJobInfo[]
) {
  if (!fallbackJobs.length) {
    return;
  }

  const candidates = videoMessages.filter((info) => !info.jobIdMark);
  if (!candidates.length) {
    fallbackJobs.forEach((job) =>
      logWithJob(job.jobId, "LOG: fallback не найден (нет кандидатов)")
    );
    return;
  }

  for (const job of fallbackJobs) {
    const targetTimestamp = job.sentAt || job.jobCreatedAt;
    let selected: VideoMessageInfo | null = null;
    let bestDiff = Infinity;

    for (const candidate of candidates) {
      if (reservedMessageIds.has(candidate.message.id)) {
        continue;
      }
      const diff = Math.abs(candidate.dateMs - targetTimestamp);
      if (diff <= FALLBACK_WINDOW_MS && diff < bestDiff) {
        selected = candidate;
        bestDiff = diff;
      }
    }

    if (!selected) {
      logWithJob(job.jobId, "LOG: fallback не найден (нет подходящих по времени)");
      continue;
    }

    logWithJob(job.jobId, "LOG: fallback-попытка", {
      messageId: selected.message.id,
      diffMs: bestDiff,
    });

    await attemptAssignment(job, selected, "timestamp");
    candidates.splice(candidates.indexOf(selected), 1);
  }
}

async function attemptAssignment(
  job: PendingJobInfo,
  messageInfo: VideoMessageInfo,
  method: MatchingMethod
) {
  const messageId = messageInfo.message.id;

  if (reservedMessageIds.has(messageId)) {
    logWithJob(job.jobId, "LOG: сообщение уже забронировано", { messageId });
    return;
  }

  const reserved = await reserveSyntaxMessageId(messageId, job.jobId, method);
  if (!reserved) {
    reservedMessageIds.add(messageId);
    logWithJob(job.jobId, "LOG: messageId уже закреплён за другой задачей", {
      messageId,
    });
    return;
  }

  reservedMessageIds.add(messageId);
  pendingJobs.delete(job.jobId);

  logWithJob(job.jobId, "LOG: FOUND_MATCH", {
    messageId,
    method,
  });

  job.resolve({
    message: messageInfo.message,
    matchingMethod: method,
    totalMessagesScanned,
  });
}

function checkPendingTimeouts() {
  const now = Date.now();
  pendingJobs.forEach((job) => {
    if (now > job.deadline) {
      pendingJobs.delete(job.jobId);
      const error = new Error(
        `Таймаут ожидания видео для jobId=${job.jobId}`
      );
      logWithJob(job.jobId, "ERROR: таймаут ожидания видео");
      job.reject(error);
    }
  });
}
