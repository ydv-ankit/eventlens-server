import db from "@/shared/lib/config/db";
import { opentelemetrySDK } from "@/shared/lib/instrumentation";
import { getRedisClient } from "@/shared/lib/config/redis";
import { KafkaEvent, RetryKafkaEvent } from "@/shared/types/types";
import {
  KAFKA_CONSUMER_GROUP,
  KAFKA_RETRY_CONSUMER_GROUP,
  KAFKA_RETRY_TOPIC,
  KAFKA_TOPIC,
  RETRY_COUNT_LIMIT,
} from "@/shared/utils/constants";
import { SQL_QUERIES } from "@/shared/utils/db/queries";
import { ENV } from "@/shared/utils/env";
import logger from "@/shared/utils/logger";
import {
  batchInsertionsGauge,
  failedInsertionsCounter,
  totalEventsProcessedCounter,
} from "@/shared/utils/monitoring/prom";
import { REDIS_CHANNEL_EVENTS, REDIS_KEYS } from "@/shared/utils/constants";
import {
  connectKafkaProducer,
  ensureKafkaTopics,
  getKafkaConsumer,
  kafkaProducer,
  subscribeKafkaConsumer,
} from "@/shared/lib/config/kafka";
import { hashApiKey } from "@/shared/utils/crypto";
import opentelemetry, {
  Link,
  SpanStatusCode,
  context,
  propagation,
} from "@opentelemetry/api";

const tracer = opentelemetry.trace.getTracer("eventlens-worker");

// ── Redis publisher — atomic INCR counters, live event publish ────────────────
const redisPublisher = getRedisClient("WorkerPublisher");

const rIncr = (key: string) =>
  redisPublisher.isReady ? redisPublisher.incr(key).catch(() => {}) : Promise.resolve();

const rIncrBy = (key: string, n: number) =>
  redisPublisher.isReady ? redisPublisher.incrBy(key, n).catch(() => {}) : Promise.resolve();

const rDecrBy = (key: string, n: number) =>
  redisPublisher.isReady ? redisPublisher.decrBy(key, n).catch(() => {}) : Promise.resolve();

const getProjectIdByApiKey = async (apiKey: string) => {
  const q = await db.query(SQL_QUERIES.GET_PROJECT_ID_BY_API_KEY, [hashApiKey(apiKey)]);
  if (q.rowCount === 1) {
    return q.rows[0].project_id;
  }
  return null;
};

const insertRawEvent = async (values: unknown[]) => {
  await db.query(
    `${SQL_QUERIES.BATCH_EVENT_INSERT}($1, $2, $3, $4, $5, $6);`,
    values,
  );
};

const publishRetryEvent = async (
  retryEvent: RetryKafkaEvent,
  topic: string,
  partition: number,
  offset: string,
) => {
  try {
    await kafkaProducer.send({
      topic: ENV.KAFKA_RETRY_TOPIC || KAFKA_RETRY_TOPIC,
      messages: [
        {
          key: retryEvent.event.apiKey,
          value: JSON.stringify(retryEvent),
        },
      ],
    });
    failedInsertionsCounter.inc();
    logger.error(
      `Kafka message moved to retry topic sourceTopic=${topic} partition=${partition} offset=${offset} retryCount=${retryEvent.retryCount}`,
    );
  } catch (error) {
    logger.error(
      `Failed to publish retry message sourceTopic=${topic} partition=${partition} offset=${offset}: ${String(error)}`,
    );
  }
};

const processKafkaMessage = async (
  parsedMessage: KafkaEvent | RetryKafkaEvent,
  topic: string,
  partition: number,
  offset: string,
  retryCount = 0,
) => {
  const extractedContext = propagation.extract(
    context.active(),
    parsedMessage.trace || {},
  );
  const links: Link[] = [];
  const spanContext = opentelemetry.trace.getSpanContext(extractedContext);
  if (spanContext) {
    links.push({ context: spanContext });
  }

  const projectId = await getProjectIdByApiKey(parsedMessage.event.apiKey);
  if (!projectId) {
    logger.error(
      `Kafka message skipped topic=${topic} partition=${partition} offset=${offset}: project not found for api key`,
    );
    return;
  }

  const values = [
    parsedMessage.event.event_name,
    parsedMessage.event.metadata,
    projectId,
    parsedMessage.event.user_id,
    parsedMessage.event.timestamp,
    parsedMessage.event.session_id ?? null,
  ];

  const workerSpan = tracer.startSpan("kafka.message", {
    links,
    attributes: {
      "messaging.system": "kafka",
      "messaging.destination.name": topic,
      "messaging.kafka.partition": partition,
      "messaging.kafka.offset": offset,
      "queue.wait.ms": Date.now() - Number(parsedMessage.queueAt),
      "kafka.retry_count": retryCount,
    },
  });
  const workerContext = opentelemetry.trace.setSpan(
    context.active(),
    workerSpan,
  );

  await tracer.startActiveSpan(
    "db.insert.message",
    {},
    workerContext,
    async (span) => {
      try {
        await insertRawEvent(values);
        span.setAttribute("db.insert.batch.size", 1);
        span.setStatus({ code: SpanStatusCode.OK });
        workerSpan.setStatus({ code: SpanStatusCode.OK });
        totalEventsProcessedCounter.inc(1);
        rIncr(REDIS_KEYS.EVENTS_TOTAL);
        rIncr(REDIS_KEYS.EVENTS_WINDOW);

        // Publish live event to Redis for WebSocket fan-out
        if (redisPublisher.isReady) {
          redisPublisher.publish(REDIS_CHANNEL_EVENTS, JSON.stringify({
            event_name: parsedMessage.event.event_name,
            user_id:    parsedMessage.event.user_id ?? null,
            project_id: projectId,
            timestamp:  parsedMessage.event.timestamp ?? new Date().toISOString(),
          })).catch(() => { /* non-critical */ });
        }

        logger.debug(
          `Kafka message inserted event=${parsedMessage.event.event_name} partition=${partition} offset=${offset} retryCount=${retryCount}`,
        );
      } catch (error) {
        span.recordException(error as Error);
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: "failed to insert kafka message into db",
        });
        workerSpan.recordException(error as Error);
        workerSpan.setStatus({
          code: SpanStatusCode.ERROR,
          message: "failed to process kafka message",
        });

        rIncr(REDIS_KEYS.FAILED_WINDOW);

        if (retryCount > 0) {
          const nextRetryCount =
            (error as Error).message.includes("getaddrinfo ENOTFOUND db")
              ? retryCount
              : retryCount - 1;

          if (nextRetryCount > 0) {
            await publishRetryEvent(
              {
                ...parsedMessage,
                retryCount: nextRetryCount,
              },
              topic,
              partition,
              offset,
            );
          } else {
            logger.error(
              `Kafka retry exhausted topic=${topic} partition=${partition} offset=${offset}: ${String(error)}`,
            );
          }
        } else {
          await publishRetryEvent(
            {
              ...parsedMessage,
              retryCount: RETRY_COUNT_LIMIT,
            },
            topic,
            partition,
            offset,
          );
        }

        logger.error(
          `Kafka message failed topic=${topic} partition=${partition} offset=${offset}: ${String(error)}`,
        );
      } finally {
        span.end();
        workerSpan.end();
      }
    },
  );
};

async function kafkaObservationConsumer() {
  const consumer = getKafkaConsumer(
    ENV.KAFKA_CONSUMER_GROUP || KAFKA_CONSUMER_GROUP,
  );

  try {
    await consumer.connect();
    await subscribeKafkaConsumer(consumer, ENV.KAFKA_TOPIC || KAFKA_TOPIC);
    logger.debug("Kafka batch consumer connected");

    await consumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        const messages = batch.messages.filter((m) => m.value);
        if (messages.length === 0) return;

        const batchSize = messages.length;
        rIncrBy(REDIS_KEYS.INFLIGHT, batchSize);

        try {
          // Deduplicate API key → project_id DB lookups
          const apiKeyCache = new Map<string, number | null>();
          const parsed = messages.map((msg) => ({
            msg,
            data: JSON.parse(msg.value!.toString()) as KafkaEvent,
          }));

          const uniqueKeys = [...new Set(parsed.map((p) => p.data.event.apiKey))];
          await Promise.all(
            uniqueKeys.map(async (key) => {
              apiKeyCache.set(key, await getProjectIdByApiKey(key));
            }),
          );

          const valid = parsed.filter((p) => apiKeyCache.get(p.data.event.apiKey) !== null);

          if (valid.length < batchSize) {
            logger.error(`Batch: ${batchSize - valid.length} messages skipped — unknown API key`);
          }

          if (valid.length > 0) {
            const rows = valid.map((p) => ({
              event_name: p.data.event.event_name,
              metadata:   p.data.event.metadata,
              project_id: apiKeyCache.get(p.data.event.apiKey)!,
              user_id:    p.data.event.user_id,
              timestamp:  p.data.event.timestamp,
              session_id: p.data.event.session_id ?? null,
              raw:        p,
            }));

            // Single multi-row INSERT for the whole batch
            const values = rows.flatMap((r) => [r.event_name, r.metadata, r.project_id, r.user_id, r.timestamp, r.session_id]);
            const placeholders = rows
              .map((_, i) => `($${i * 6 + 1}, $${i * 6 + 2}, $${i * 6 + 3}, $${i * 6 + 4}, $${i * 6 + 5}, $${i * 6 + 6})`)
              .join(", ");

            try {
              await db.query(`${SQL_QUERIES.BATCH_EVENT_INSERT}${placeholders}`, values);

              totalEventsProcessedCounter.inc(rows.length);
              rIncrBy(REDIS_KEYS.EVENTS_TOTAL, rows.length);
              rIncrBy(REDIS_KEYS.EVENTS_WINDOW, rows.length);

              if (redisPublisher.isReady) {
                for (const row of rows) {
                  redisPublisher.publish(
                    REDIS_CHANNEL_EVENTS,
                    JSON.stringify({
                      event_name: row.event_name,
                      user_id:    row.user_id ?? null,
                      project_id: row.project_id,
                      timestamp:  row.timestamp ?? new Date().toISOString(),
                    }),
                  ).catch(() => {});
                }
              }

              logger.debug(`Batch inserted ${rows.length} events partition=${batch.partition}`);
            } catch (error) {
              // Whole batch failed — send each message to retry topic
              logger.error(`Batch INSERT failed (${rows.length} events): ${String(error)}`);
              rIncrBy(REDIS_KEYS.FAILED_WINDOW, rows.length);
              failedInsertionsCounter.inc(rows.length);

              await Promise.allSettled(
                rows.map((row) =>
                  publishRetryEvent(
                    { ...row.raw.data, retryCount: RETRY_COUNT_LIMIT },
                    batch.topic,
                    batch.partition,
                    row.raw.msg.offset,
                  ),
                ),
              );
            }
          }

          // Commit all offsets in this batch
          for (const msg of messages) resolveOffset(msg.offset);
          await heartbeat();
        } finally {
          rDecrBy(REDIS_KEYS.INFLIGHT, batchSize);
        }
      },
    });
  } catch (error) {
    logger.error("Kafka consumer failed: " + String(error));
  }
}

async function kafkaRetryConsumer() {
  const consumer = getKafkaConsumer(
    ENV.KAFKA_RETRY_CONSUMER_GROUP || KAFKA_RETRY_CONSUMER_GROUP,
  );

  try {
    await consumer.connect();
    await subscribeKafkaConsumer(
      consumer,
      ENV.KAFKA_RETRY_TOPIC || KAFKA_RETRY_TOPIC,
    );
    logger.debug("Kafka retry consumer connected");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;

        rIncr(REDIS_KEYS.INFLIGHT);
        try {
          const parsedMessage = JSON.parse(message.value.toString()) as RetryKafkaEvent;
          await processKafkaMessage(parsedMessage, topic, partition, message.offset, parsedMessage.retryCount);
        } catch (error) {
          logger.error(
            `Kafka retry message skipped topic=${topic} partition=${partition} offset=${message.offset}: ${String(error)}`,
          );
        } finally {
          if (redisPublisher.isReady) redisPublisher.decr(REDIS_KEYS.INFLIGHT).catch(() => {});
        }
      },
    });
  } catch (error) {
    logger.error("Kafka retry consumer failed: " + String(error));
  }
}

;(async () => {
  opentelemetrySDK.start();
  await redisPublisher.connect().catch((e) =>
    logger.error("Worker Redis connect failed: " + String(e))
  );
  await ensureKafkaTopics();
  await connectKafkaProducer();
  void kafkaObservationConsumer();
  void kafkaRetryConsumer();
})();
