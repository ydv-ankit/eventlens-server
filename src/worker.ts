import db from "./lib/config/db";
import {
  AppRedisClient,
  ensureRedisConnection,
  getRedisClient,
  redisClient,
} from "./lib/config/redis";
import { EventRequestData } from "./types/api";
import { RedisEvent, RetryQueue } from "./types/types";
import {
  BATCH_INSERTION_LIMIT,
  EVENT_QUEUE_KEY,
  FAILED_INSERTION_COUNT,
  REDIS_RETRY_DELAY_MS,
  RETRY_COUNT_LIMIT,
  RETRY_EVENT_QUEUE_LIMIT,
  RETRY_QUEUE_FAILED_RETRY_DELAY,
  TOTAL_EVENTS_PROCESSED,
} from "./utils/constants";
import { SQL_QUERIES } from "./utils/db/queries";
import logger from "./utils/logger";
import {
  batchInsertionsGauge,
  failedInsertionsCounter,
  totalEventsProcessedCounter,
} from "./utils/monitoring/prom";
import opentelemetry, {
  Link,
  SpanStatusCode,
  context,
  propagation,
} from "@opentelemetry/api";

const tracer = opentelemetry.trace.getTracer("eventlens-worker");

const retryQueue = new Array<RetryQueue>();
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

setInterval(() => {
  batchInsertionsGauge.set(0);
  failedInsertionsCounter.set(0);
}, 1000);

const getProjectIdByApiKey = async (apiKey: string) => {
  if (redisClient.isReady) {
    const projectId = await redisClient.get(apiKey);
    if (projectId) {
      return projectId;
    }
  }

  const q = await db.query(SQL_QUERIES.GET_PROJECT_ID_BY_API_KEY, [apiKey]);
  if (q.rowCount === 1) {
    const projectId = q.rows[0].id;
    if (redisClient.isReady) {
      await redisClient.set(apiKey, projectId, {
        expiration: {
          type: "EX",
          value: 5 * 60,
        },
      });
    }
    return projectId;
  }
  return null;
};

// main queue worker
async function mainQueueWorker(
  workerRedisClient: AppRedisClient,
  workerName: string,
) {
  logger.info(`Starting ${workerName}`);
  while (true) {
    const batchInsertionValues = new Array();
    try {
      await ensureRedisConnection(workerRedisClient, workerName);
      if (!workerRedisClient.isReady) {
        await sleep(REDIS_RETRY_DELAY_MS);
        continue;
      }

      const result = await workerRedisClient.BLPOP(EVENT_QUEUE_KEY, 0);
      if (!result) {
        await sleep(REDIS_RETRY_DELAY_MS);
        continue;
      }

      const parsedResult = JSON.parse(result.element) as RedisEvent;
      // trace context
      const links: Link[] = [];
      const inputContext = parsedResult.trace;
      const spanContext = opentelemetry.trace.getSpanContext(
        propagation.extract(context.active(), inputContext),
      );
      if (spanContext) {
        links.push({ context: spanContext });
      }

      const projectId = await getProjectIdByApiKey(parsedResult.event.apiKey);
      const values = [
        parsedResult.event.event_name,
        parsedResult.event.metadata,
        projectId,
        parsedResult.event.user_id,
        parsedResult.event.timestamp,
      ];
      batchInsertionValues.push(...values);
      for (let i = 0; i < BATCH_INSERTION_LIMIT - 1; i++) {
        const result = await workerRedisClient.LPOP(EVENT_QUEUE_KEY);
        if (!result) break;
        const parsedResult = JSON.parse(result) as RedisEvent;
        // trace context
        const inputContext = parsedResult.trace;
        const spanContext = opentelemetry.trace.getSpanContext(
          propagation.extract(context.active(), inputContext),
        );
        if (spanContext) {
          links.push({ context: spanContext });
        }
        // using cache to avoid db lookup
        const projectId = await getProjectIdByApiKey(parsedResult.event.apiKey);
        if (!projectId) continue;
        const values = [
          parsedResult.event.event_name,
          parsedResult.event.metadata,
          projectId,
          parsedResult.event.user_id,
          parsedResult.event.timestamp,
        ];
        batchInsertionValues.push(...values);
      }

      if (batchInsertionValues.length === 0) {
        logger.info("no data to insert");
        continue;
      }

      let query = SQL_QUERIES.BATCH_EVENT_INSERT;
      const valuesLimit = Math.floor(batchInsertionValues.length / 5);
      for (let i = 0; i < valuesLimit; i++) {
        query += `($${5 * i + 1}, $${5 * i + 2}, $${5 * i + 3}, $${5 * i + 4}, $${5 * i + 5})`;
        if (i === valuesLimit - 1) {
          query += `;`;
        } else {
          query += `,`;
        }
      }
      const workerSpan = tracer.startSpan("worker.batch", { links });
      workerSpan.setAttribute(
        "queue.wait.ms",
        Date.now() - Number(parsedResult.queueAt),
      );
      const workerContext = opentelemetry.trace.setSpan(
        context.active(),
        workerSpan,
      );
      await tracer.startActiveSpan(
        "db.insert.batch",
        {},
        workerContext,
        async (span) => {
          try {
            await db.query(query, batchInsertionValues);
            span.setAttribute("db.insert.batch.size", valuesLimit);
            span.setStatus({ code: SpanStatusCode.OK });
          } catch (error) {
            span.recordException(error as Error);
            span.setStatus({
              code: SpanStatusCode.ERROR,
              message: "failed to insert into db",
            });
          } finally {
            span.end();
            workerSpan.end();
          }
        },
      );
      totalEventsProcessedCounter.inc(valuesLimit);
      batchInsertionsGauge.set(valuesLimit);
      redisClient.isReady &&
        (await redisClient.incrBy(TOTAL_EVENTS_PROCESSED, valuesLimit));
    } catch (error) {
      logger.error(
        `${workerName} failed to process queue items: ${String(error)}`,
      );
      // push this data into retry queue for processing again
      const valuesLimit = Math.floor(batchInsertionValues.length / 5);
      for (let idx = 0; idx < valuesLimit; idx++) {
        setTimeout(() => {
          if (retryQueue.length < RETRY_EVENT_QUEUE_LIMIT) {
            retryQueue.push({
              values: batchInsertionValues.slice(idx * 5, idx * 5 + 5),
              retry_count: RETRY_COUNT_LIMIT,
            } as RetryQueue);
          } else {
            logger.error("Retry queue overflow, dropping events");
          }
        }, 100);
      }
      await sleep(REDIS_RETRY_DELAY_MS);
    }
  }
}

// retry queue worker
async function retryQueueWorker() {
  while (true) {
    if (retryQueue.length === 0) {
      await new Promise((r) => setTimeout(r, 100));
      continue;
    }
    const retryItem = retryQueue.shift();
    if (retryItem?.retry_count === 0) {
      logger.info(
        "retry count limit reached, skipping this event: " +
          JSON.stringify(retryItem.values),
      );
      continue;
    }
    try {
      let query = SQL_QUERIES.BATCH_EVENT_INSERT;
      const valuesLimit = Math.floor(Number(retryItem?.values?.length) / 5);
      for (let i = 0; i < valuesLimit; i++) {
        query += `($${5 * i + 1}, $${5 * i + 2}, $${5 * i + 3}, $${5 * i + 4}, $${5 * i + 5})`;
        if (i === valuesLimit - 1) {
          query += `;`;
        } else {
          query += `,`;
        }
      }

      await db.query(SQL_QUERIES.BATCH_EVENT_INSERT, retryItem?.values);
    } catch (error: any) {
      logger.error("{retry queue error} " + error);
      let retry_count = RETRY_COUNT_LIMIT;
      // db down
      if ((error as Error).message.includes("getaddrinfo ENOTFOUND db")) {
        retry_count = retryItem!.retry_count;
      } else {
        retry_count = retryItem!.retry_count - 1;
      }
      retryQueue.push({
        values: retryItem?.values,
        retry_count,
      } as RetryQueue);
      await redisClient.incr(FAILED_INSERTION_COUNT);
      failedInsertionsCounter.set(retryQueue.length);
    }
  }
}

export const startWorkers = async () => {
  // initialize workers
  const client1 = getRedisClient("Worker-1");
  // const client2 = getRedisClient("Worker-2");
  // const client3 = getRedisClient("Worker-3");

  void ensureRedisConnection(client1, "Worker-1");
  // void ensureRedisConnection(client2, "Worker-2");
  // void ensureRedisConnection(client3, "Worker-3");

  void mainQueueWorker(client1, "Worker-1");
  // void mainQueueWorker(client2, "Worker-2");
  // void mainQueueWorker(client3, "Worker-3");
  void retryQueueWorker();
};
