import { redisClient } from "@/lib/config/redis";
import { EventRequestData } from "@/types/api";
import { EVENT_QUEUE_KEY, EVENT_QUEUE_MAX_SIZE, HTTP_CODE } from "@/utils/constants";
import logger from "@/utils/logger";
import { NextFunction, Request, Response } from "express";

const newEvent = async (req: Request, res: Response, _next: NextFunction) => {
    try {
        const data = req.body as EventRequestData;
        const apiKey = req.headers.authorization?.split(" ")[1];
        if (!apiKey) throw new Error("API_KEY not found");
        if (!redisClient.isReady) {
            res.error(HTTP_CODE.SERVICE_UNAVAILABLE, "event ingestion queue unavailable")
            return
        }
        if (await redisClient.lLen(EVENT_QUEUE_KEY) >= EVENT_QUEUE_MAX_SIZE) {
            logger.error("Queue limit reached to max limit of " + EVENT_QUEUE_MAX_SIZE)
            res.error(HTTP_CODE.TOO_MANY_REQUESTS, "retry after some time")
            return
        }
        await redisClient.lPush(EVENT_QUEUE_KEY, JSON.stringify({
            event_name: data.event_name, 
            metadata: data.metadata, 
            apiKey, 
            user_id: data.user_id, 
            timestamp: data.timestamp
        }))
        res.success(HTTP_CODE.OK, "event accepted")
    } catch (error) {
        logger.error("failed to enqueue event: " + String(error));
        const statusCode = redisClient.isReady ? HTTP_CODE.BAD_REQUEST : HTTP_CODE.SERVICE_UNAVAILABLE;
        const message = redisClient.isReady
            ? "error occured while enqueueing event"
            : "event ingestion queue unavailable";
        res.error(statusCode, message);
    }
}

export {newEvent};
