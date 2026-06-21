import { NextFunction, Request, Response } from "express";
import db from "@/shared/lib/config/db";
import { SQL_QUERIES } from "@/shared/utils/db/queries";
import { HTTP_CODE } from "@/shared/utils/constants";

// Reads project_id from req.params.id or req.query.project_id
export const requireProjectOwner = async (req: Request, res: Response, next: NextFunction) => {
  const projectId = req.params.id ?? req.query.project_id;
  if (!projectId) {
    res.error(HTTP_CODE.BAD_REQUEST, "project_id is required");
    return;
  }

  const result = await db.query(SQL_QUERIES.GET_PROJECT_BY_ID, [
    projectId,
    req.dbUser.clerk_user_id,
  ]);

  if (!result.rowCount || result.rowCount === 0) {
    res.error(HTTP_CODE.NOT_FOUND, "project not found");
    return;
  }

  next();
};
