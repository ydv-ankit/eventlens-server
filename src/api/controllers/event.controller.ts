import { redisClient } from "@/shared/lib/config/redis";
import { EventRequestData } from "@/shared/types/api";
import {
  EVENT_QUEUE_KEY,
  EVENT_QUEUE_MAX_SIZE,
  HTTP_CODE,
} from "@/shared/utils/constants";
import logger from "@/shared/utils/logger";
import { NextFunction, Request, Response } from "express";
import opentelemetry, {
  SpanStatusCode,
  context,
  propagation,
} from "@opentelemetry/api";

const tracer = opentelemetry.trace.getTracer("eventlens-event-controller");

const newEvent = async (req: Request, res: Response, _next: NextFunction) => {
  return tracer.startActiveSpan("event.enqueue", async (activeSpan) => {
    try {
      const data = req.body as EventRequestData;
      const apiKey = req.headers.authorization?.split(" ")[1];
      activeSpan.setAttributes({
        "event.queue.name": EVENT_QUEUE_KEY,
        "event.name": data.event_name,
        "event.user_id_present": Boolean(data.user_id),
      });

      if (!apiKey) throw new Error("API_KEY not found");
      activeSpan.setAttribute("event.api_key_present", true);
      if (!redisClient.isReady) {
        activeSpan.setAttribute("event.redis_ready", false);
        activeSpan.setStatus({
          code: SpanStatusCode.ERROR,
          message: "Redis client unavailable",
        });
        res.error(
          HTTP_CODE.SERVICE_UNAVAILABLE,
          "event ingestion queue unavailable",
        );
        return;
      }
      activeSpan.setAttribute("event.redis_ready", true);

      const queueDepth = await redisClient.lLen(EVENT_QUEUE_KEY);
      activeSpan.setAttribute("event.queue.depth_before_enqueue", queueDepth);
      if (queueDepth >= EVENT_QUEUE_MAX_SIZE) {
        logger.error(
          "Queue limit reached to max limit of " + EVENT_QUEUE_MAX_SIZE,
        );
        activeSpan.setAttribute("event.enqueue.rejected", true);
        activeSpan.setStatus({
          code: SpanStatusCode.ERROR,
          message: "Queue limit reached",
        });
        res.error(HTTP_CODE.TOO_MANY_REQUESTS, "retry after some time");
        return;
      }

      await tracer.startActiveSpan("redis.enqueue", async (redisSpan) => {
        try {
          redisSpan.setAttributes({
            "db.system": "redis",
            "db.operation": "LPUSH",
            "db.redis.key": EVENT_QUEUE_KEY,
          });

          const traceContext = {};
          propagation.inject(context.active(), traceContext);
          await redisClient.lPush(
            EVENT_QUEUE_KEY,
            JSON.stringify({
              event: {
                event_name: data.event_name,
                metadata: data.metadata,
                apiKey,
                user_id: data.user_id,
                timestamp: data.timestamp,
              },
              trace: traceContext,
              queueAt: Date.now(),
            }),
          );

          redisSpan.setStatus({ code: SpanStatusCode.OK });
        } catch (error) {
          redisSpan.recordException(error as Error);
          redisSpan.setStatus({
            code: SpanStatusCode.ERROR,
            message: "Failed to enqueue event in Redis",
          });
          throw error;
        } finally {
          redisSpan.end();
        }
      });

      activeSpan.setAttribute("event.enqueue.rejected", false);
      activeSpan.setStatus({ code: SpanStatusCode.OK });
      res.success(HTTP_CODE.OK, "event accepted");
    } catch (error) {
      logger.error("failed to enqueue event: " + String(error));
      const statusCode = redisClient.isReady
        ? HTTP_CODE.BAD_REQUEST
        : HTTP_CODE.SERVICE_UNAVAILABLE;
      const message = redisClient.isReady
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
