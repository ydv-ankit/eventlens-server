import db from "@/shared/lib/config/db";
import { HTTP_CODE } from "@/shared/utils/constants";
import { SQL_QUERIES } from "@/shared/utils/db/queries";
import logger from "@/shared/utils/logger";
import { NextFunction, Request, Response } from "express";

const getOverview = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { project_id } = req.query as Record<string, string>;
    if (!project_id) {
      res.error(HTTP_CODE.BAD_REQUEST, "project_id is required");
      return;
    }
    const result = await db.query(SQL_QUERIES.GET_ANALYTICS_OVERVIEW, [project_id]);
    res.success(HTTP_CODE.OK, "overview fetched", result.rows[0]);
  } catch (error) {
    logger.error("analytics overview failed: " + String(error));
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error fetching overview");
  }
};

const VOLUME_QUERIES: Record<string, string> = {
  "1h":  SQL_QUERIES.GET_EVENT_VOLUME_1H,
  "24h": SQL_QUERIES.GET_EVENT_VOLUME_24H,
  "7d":  SQL_QUERIES.GET_EVENT_VOLUME_7D,
  "30d": SQL_QUERIES.GET_EVENT_VOLUME_30D,
};

const getEventVolume = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { project_id, interval = "24h" } = req.query as Record<string, string>;
    if (!project_id) {
      res.error(HTTP_CODE.BAD_REQUEST, "project_id is required");
      return;
    }
    const query = VOLUME_QUERIES[interval];
    if (!query) {
      res.error(HTTP_CODE.BAD_REQUEST, "interval must be one of: 1h, 24h, 7d, 30d");
      return;
    }
    const result = await db.query(query, [project_id]);
    res.success(HTTP_CODE.OK, "event volume fetched", result.rows);
  } catch (error) {
    logger.error("event volume failed: " + String(error));
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error fetching event volume");
  }
};

const getTopEvents = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { project_id, limit = "10" } = req.query as Record<string, string>;
    if (!project_id) {
      res.error(HTTP_CODE.BAD_REQUEST, "project_id is required");
      return;
    }
    const parsedLimit = Math.min(parseInt(limit, 10) || 10, 50);
    const result = await db.query(SQL_QUERIES.GET_TOP_EVENTS, [project_id, parsedLimit]);
    res.success(HTTP_CODE.OK, "top events fetched", result.rows);
  } catch (error) {
    logger.error("top events failed: " + String(error));
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error fetching top events");
  }
};

const getRecentEvents = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { project_id, limit = "20" } = req.query as Record<string, string>;
    if (!project_id) {
      res.error(HTTP_CODE.BAD_REQUEST, "project_id is required");
      return;
    }
    const parsedLimit = Math.min(parseInt(limit, 10) || 20, 100);
    const result = await db.query(SQL_QUERIES.GET_RECENT_EVENTS, [project_id, parsedLimit]);
    res.success(HTTP_CODE.OK, "recent events fetched", result.rows);
  } catch (error) {
    logger.error("recent events failed: " + String(error));
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error fetching recent events");
  }
};

export { getOverview, getEventVolume, getTopEvents, getRecentEvents };
