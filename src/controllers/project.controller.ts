import db from "@/lib/config/db";
import { HTTP_CODE } from "@/utils/constants";
import logger from "@/utils/logger";
import { NextFunction, Request, Response } from "express";

const createProject = async (_req: Request, res: Response, _next: NextFunction) => {
    try {
        const queryResponse = await db.query("select * from users");
        logger.info("queryResponse" + queryResponse);
        res.success(HTTP_CODE.CREATED, "got data for users", {
            users: queryResponse
        })
    } catch (error) {
        console.log(error);
        res.error(HTTP_CODE.BAD_REQUEST, "error occured while creating project");
    }
}

export {createProject};