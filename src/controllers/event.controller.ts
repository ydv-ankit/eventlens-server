import { EventRequestData } from "@/types/api";
import { asyncQueueMaxLimit, HTTP_CODE } from "@/utils/constants";
import logger from "@/utils/logger";
import { asyncQueue } from "@/worker";
import { NextFunction, Request, Response } from "express";

const newEvent = async (req: Request, res: Response, _next: NextFunction) => {
    try {
        const data = req.body as EventRequestData;
        const apiKey = String(req.headers.authorization?.split(" ")[1]);
        if (asyncQueue.length >= asyncQueueMaxLimit) {
            logger.error("Queue limit reached to max limit of " + asyncQueueMaxLimit)
            res.error(HTTP_CODE.TOO_MANY_REQUESTS, "retry after some time")
            return
        }
        asyncQueue.push({
            event_name: data.event_name, 
            metadata: data.metadata, 
            apiKey, 
            user_id: data.user_id, 
            timestamp: data.timestamp
        })
        res.success(HTTP_CODE.OK, "event accepted")
    } catch (error) {
        res.error(HTTP_CODE.BAD_REQUEST, "error occured while creating project");
    }
}

export {newEvent};