import db from "@/shared/lib/config/db";
import { HTTP_CODE } from "@/shared/utils/constants";
import logger from "@/shared/utils/logger";
import { NextFunction, Request, Response } from "express";
import client from "prom-client";

const getSystemHealth = async (_req: Request, res: Response, _next: NextFunction) => {
  try {
    const [perSecResult, totalResult, metricsJson] = await Promise.all([
      db.query(`
        SELECT ROUND(COUNT(*)::numeric / 60.0, 2) AS value
        FROM analytics.raw_event
        WHERE COALESCE(timestamp, created_at) >= NOW() - INTERVAL '60 seconds'
      `),
      db.query(`SELECT COUNT(*)::int AS value FROM analytics.raw_event`),
      client.register.getMetricsAsJSON(),
    ]);

    const eventsPerSec = Number(perSecResult.rows[0]?.value ?? 0);
    const totalEventsProcessed = Number(totalResult.rows[0]?.value ?? 0);

    const metricMap = new Map(
      metricsJson.map((m) => [m.name, (m as any).values?.[0]?.value ?? 0]),
    );

    res.success(HTTP_CODE.OK, "system health", {
      eventsPerSec,
      totalEventsProcessed,
      failedInsertionCount: metricMap.get("failed_insertions_count") ?? 0,
      mainQueueDepth:       metricMap.get("main_queue_depth") ?? 0,
    });
  } catch (error) {
    logger.error("system health failed: " + String(error));
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error fetching system health");
  }
};

export { getSystemHealth };
