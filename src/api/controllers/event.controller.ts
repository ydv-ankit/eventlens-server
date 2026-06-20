import { EventRequestData } from "@/shared/types/api";
import {
  HTTP_CODE,
  KAFKA_TOPIC,
} from "@/shared/utils/constants";
import logger from "@/shared/utils/logger";
import { NextFunction, Request, Response } from "express";
import opentelemetry, {
  SpanStatusCode,
  context,
  propagation,
} from "@opentelemetry/api";
import { isKafkaProducerConnected, kafkaProducer } from "@/shared/lib/config/kafka";
import { QueueEventEnvelope } from "@/shared/types/types";
import { ENV } from "@/shared/utils/env";

const tracer = opentelemetry.trace.getTracer("eventlens-event-controller");

const newEvent = async (req: Request, res: Response, _next: NextFunction) => {
  return tracer.startActiveSpan("event.enqueue", async (activeSpan) => {
    try {
      const data = req.body as EventRequestData;
      const apiKey = req.headers.authorization?.split(" ")[1];
      activeSpan.setAttributes({
        "messaging.system": "kafka",
        "messaging.destination.name": ENV.KAFKA_TOPIC || KAFKA_TOPIC,
        "event.name": data.event_name,
        "event.user_id_present": Boolean(data.user_id),
      });

      if (!apiKey) throw new Error("API_KEY not found");
      activeSpan.setAttribute("event.api_key_present", true);
      if (!isKafkaProducerConnected()) {
        activeSpan.setStatus({
          code: SpanStatusCode.ERROR,
          message: "Kafka producer unavailable",
        });
        res.error(
          HTTP_CODE.SERVICE_UNAVAILABLE,
          "event ingestion queue unavailable",
        );
        return;
      }

      const traceContext = {};
      propagation.inject(context.active(), traceContext);
      const queueEventEnvelope: QueueEventEnvelope = {
        event: {
          event_name: data.event_name,
          metadata: data.metadata,
          apiKey,
          user_id: data.user_id,
          timestamp: data.timestamp,
        },
        trace: traceContext,
        queueAt: Date.now(),
      };

      await tracer.startActiveSpan("kafka.produce", async (kafkaSpan) => {
        try {
          kafkaSpan.setAttributes({
            "messaging.system": "kafka",
            "messaging.destination.name": ENV.KAFKA_TOPIC || KAFKA_TOPIC,
            "messaging.operation.name": "send",
          });

          await kafkaProducer.send({
            topic: ENV.KAFKA_TOPIC || KAFKA_TOPIC,
            messages: [
              {
                key: data.user_id,
                value: JSON.stringify(queueEventEnvelope),
              },
            ],
          });

          kafkaSpan.setStatus({ code: SpanStatusCode.OK });
        } catch (error) {
          kafkaSpan.recordException(error as Error);
          kafkaSpan.setStatus({
            code: SpanStatusCode.ERROR,
            message: "Failed to publish event to Kafka",
          });
          logger.error("failed to publish event to kafka: " + String(error));
        } finally {
          kafkaSpan.end();
        }
      });

      activeSpan.setAttribute("event.enqueue.rejected", false);
      activeSpan.setStatus({ code: SpanStatusCode.OK });
      res.success(HTTP_CODE.OK, "event accepted");
    } catch (error) {
      logger.error("failed to enqueue event: " + String(error));
      const statusCode = isKafkaProducerConnected()
        ? HTTP_CODE.BAD_REQUEST
        : HTTP_CODE.SERVICE_UNAVAILABLE;
      const message = isKafkaProducerConnected()
        ? "error occured while enqueueing event"
        : "event ingestion queue unavailable";
      activeSpan.recordException(error as Error);
      activeSpan.setStatus({
        code: SpanStatusCode.ERROR,
        message,
      });
      res.error(statusCode, message);
    } finally {
      activeSpan.end();
    }
  });
};

export { newEvent };
