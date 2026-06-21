import db from "@/shared/lib/config/db";
import { HTTP_CODE } from "@/shared/utils/constants";
import { SQL_QUERIES } from "@/shared/utils/db/queries";
import { generateApiKey, hashApiKey } from "@/shared/utils/crypto";
import { NextFunction, Request, Response } from "express";

const createApiKey = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { id: projectId } = req.params;

    const rawKey = generateApiKey();
    const keyHash = hashApiKey(rawKey);

    const result = await db.query(SQL_QUERIES.CREATE_API_KEY, [projectId, keyHash]);

    // raw key returned only once — never stored, cannot be retrieved again
    res.success(HTTP_CODE.CREATED, "api key created", { ...result.rows[0], key: rawKey });
  } catch (error) {
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error creating api key");
  }
};

const getApiKeys = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { id: projectId } = req.params;
    const result = await db.query(SQL_QUERIES.GET_API_KEYS_BY_PROJECT, [projectId]);
    res.success(HTTP_CODE.OK, "api keys fetched", result.rows);
  } catch (error) {
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error fetching api keys");
  }
};

const deleteApiKey = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { keyId: id } = req.params;
    const result = await db.query(SQL_QUERIES.DELETE_API_KEY, [id]);
    if (result.rowCount === 0) {
      res.error(HTTP_CODE.NOT_FOUND, "api key not found");
      return;
    }
    res.success(HTTP_CODE.OK, "api key deleted");
  } catch (error) {
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error deleting api key");
  }
};

export { createApiKey, getApiKeys, deleteApiKey };
