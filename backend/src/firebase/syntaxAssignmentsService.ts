import { getFirestore } from "./admin";
import { MatchingMethod } from "../models/videoJob";

const COLLECTION_NAME = "syntaxMessageAssignments";

export interface SyntaxAssignment {
  messageId: number;
  jobId: string;
  matchingMethod: MatchingMethod;
  createdAt: number;
}

/**
 * Пытается зарезервировать messageId за конкретной задачей
 * Возвращает true, если удалось создать запись, false если messageId уже занят
 */
export async function reserveSyntaxMessageId(
  messageId: number,
  jobId: string,
  matchingMethod: MatchingMethod
): Promise<boolean> {
  const db = getFirestore();
  const docRef = db.collection(COLLECTION_NAME).doc(String(messageId));

  try {
    await docRef.create({
      messageId,
      jobId,
      matchingMethod,
      createdAt: Date.now(),
    } satisfies SyntaxAssignment);
    return true;
  } catch (error: any) {
    const errorMessage = error?.message || "";
    if (
      error?.code === 6 || // ALREADY_EXISTS
      errorMessage.includes("Already exists") ||
      errorMessage.includes("already exists")
    ) {
      const existing = await docRef.get();
      const existingJob = existing.exists ? existing.data()?.jobId : "unknown";
      console.warn(
        `[SyntxAssignments] messageId=${messageId} уже зарезервирован (jobId=${existingJob})`
      );
      return false;
    }
    console.error("[SyntxAssignments] Ошибка при резервировании messageId:", error);
    throw error;
  }
}

/**
 * Загружает все ранее зарезервированные messageId (для восстановления состояния при рестарте)
 */
export async function getAllReservedSyntaxMessageIds(): Promise<Set<number>> {
  const db = getFirestore();
  const snapshot = await db.collection(COLLECTION_NAME).get();
  const ids = new Set<number>();
  snapshot.forEach((doc) => {
    const numericId = Number(doc.id);
    if (!Number.isNaN(numericId)) {
      ids.add(numericId);
    }
  });
  console.log(`[SyntxAssignments] Загружено ${ids.size} резервов messageId`);
  return ids;
}

