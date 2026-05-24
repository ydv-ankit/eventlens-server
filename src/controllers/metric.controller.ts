import { connectRedis, redisClient } from "@/lib/config/redis";
import { EVENT_QUEUE_KEY, FAILED_INSERTION_COUNT, HTTP_CODE, TOTAL_EVENTS_PROCESSED, TOTAL_REQUESTS } from "@/utils/constants";
import logger from "@/utils/logger";
import { NextFunction, Request, Response } from "express";

const getMetrics = async (req: Request, res: Response, _next: NextFunction) => {
    try {
        await connectRedis();
        if (!redisClient.isReady) {
            res.error(HTTP_CODE.SERVICE_UNAVAILABLE, "metrics store unavailable");
            return;
        }

        const totalRequestsCount = await redisClient.get(TOTAL_REQUESTS);
        const totalEventsProcessed = await redisClient.get(TOTAL_EVENTS_PROCESSED);
        const mainQueueDepth = await redisClient.lLen(EVENT_QUEUE_KEY);
        const failedInsertionCount = await redisClient.get(FAILED_INSERTION_COUNT);

        const metrics = {
            totalRequestsCount: totalRequestsCount ?? 0,
            totalEventsProcessed: totalEventsProcessed ?? 0,
            mainQueueDepth: mainQueueDepth ?? 0,
            failedInsertionCount: failedInsertionCount ?? 0
        }

        res.success(HTTP_CODE.OK, "application metrics", metrics);
    } catch (error) {
        logger.error("failed to get metrics: " + String(error));
        const statusCode = redisClient.isReady ? HTTP_CODE.BAD_REQUEST : HTTP_CODE.SERVICE_UNAVAILABLE;
        const message = redisClient.isReady
            ? "error occured while fetching metrics"
            : "metrics store unavailable";
        res.error(statusCode, message);
    }
}

export { getMetrics };
