import { HTTP_CODE } from "@/shared/utils/constants";
import logger from "@/shared/utils/logger";
import { NextFunction, Request, Response } from "express";
import client from "prom-client";

const getPromMetrics = async (
  req: Request,
  res: Response,
  _next: NextFunction,
) => {
  try {
    const metrics = await client.register.metrics();
    res.contentType(client.register.contentType).send(metrics);
  } catch (error) {
    logger.error("failed to get metrics: " + String(error));
    res.error(HTTP_CODE.SERVICE_UNAVAILABLE, "metrics store unavailable");
  }
};

const getMetrics = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const metricsJson = await client.register.getMetricsAsJSON();
    const metricMap = new Map(
      metricsJson.map((metric) => [metric.name, metric.values?.[0]?.value ?? 0]),
    );

    const metrics = {
      totalRequestsCount: metricMap.get("total_requests_count") ?? 0,
      totalEventsProcessed: metricMap.get("total_events_processed_count") ?? 0,
      mainQueueDepth: metricMap.get("main_queue_depth") ?? 0,
      failedInsertionCount: metricMap.get("failed_insertions_count") ?? 0,
    };

    res.success(HTTP_CODE.OK, "application metrics", metrics);
  } catch (error) {
    logger.error("failed to get metrics: " + String(error));
    res.error(HTTP_CODE.SERVICE_UNAVAILABLE, "metrics store unavailable");
  }
};

export { getPromMetrics, getMetrics };
