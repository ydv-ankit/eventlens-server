import db from "@/shared/lib/config/db";
import { opentelemetrySDK } from "@/shared/lib/instrumentation";
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
import {
  connectKafkaProducer,
  ensureKafkaTopics,
  getKafkaConsumer,
  kafkaProducer,
  subscribeKafkaConsumer,
} from "@/shared/lib/config/kafka";
import opentelemetry, {
  Link,
  SpanStatusCode,
  context,
  propagation,
} from "@opentelemetry/api";

const tracer = opentelemetry.trace.getTracer("eventlens-worker");

setInterval(() => {
  batchInsertionsGauge.set(0);
  failedInsertionsCounter.set(0);
}, 1000);

const getProjectIdByApiKey = async (apiKey: string) => {
  const q = await db.query(SQL_QUERIES.GET_PROJECT_ID_BY_API_KEY, [apiKey]);
  if (q.rowCount === 1) {
    return q.rows[0].id;
  }
  return null;
};

const insertRawEvent = async (values: unknown[]) => {
  await db.query(
    `${SQL_QUERIES.BATCH_EVENT_INSERT}($1, $2, $3, $4, $5);`,
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
        batchInsertionsGauge.set(1);
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
    logger.debug("Kafka consumer connected");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) {
          return;
        }

        try {
          const parsedMessage = JSON.parse(
            message.value.toString(),
          ) as KafkaEvent;
          await processKafkaMessage(
            parsedMessage,
            topic,
            partition,
            message.offset,
            0,
          );
        } catch (error) {
          logger.error(
            `Kafka message skipped topic=${topic} partition=${partition} offset=${message.offset}: ${String(error)}`,
          );
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
        if (!message.value) {
          return;
        }

        try {
          const parsedMessage = JSON.parse(
            message.value.toString(),
          ) as RetryKafkaEvent;
          await processKafkaMessage(
            parsedMessage,
            topic,
            partition,
            message.offset,
            parsedMessage.retryCount,
          );
        } catch (error) {
          logger.error(
            `Kafka retry message skipped topic=${topic} partition=${partition} offset=${message.offset}: ${String(error)}`,
          );
        }
      },
    });
  } catch (error) {
    logger.error("Kafka retry consumer failed: " + String(error));
  }
}

;(async () => {
  opentelemetrySDK.start();
  await ensureKafkaTopics();
  await connectKafkaProducer();
  void kafkaObservationConsumer();
  void kafkaRetryConsumer();
})();
