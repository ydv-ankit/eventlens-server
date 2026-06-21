import db from "@/shared/lib/config/db";
import { HTTP_CODE } from "@/shared/utils/constants";
import { SQL_QUERIES } from "@/shared/utils/db/queries";
import logger from "@/shared/utils/logger";
import { NextFunction, Request, Response } from "express";

const getUserTimeline = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { id: userId } = req.params;
    const result = await db.query(SQL_QUERIES.GET_USER_TIMELINE, [userId]);
    res.success(HTTP_CODE.OK, "user timeline fetched", result.rows);
  } catch (error) {
    logger.error("user timeline failed: " + String(error));
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error fetching user timeline");
  }
};

export { getUserTimeline };
