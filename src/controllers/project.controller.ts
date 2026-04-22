import db from "@/lib/config/db";
import { HTTP_CODE } from "@/utils/constants";
import logger from "@/utils/logger";
import { randomUUID } from "crypto";
import { NextFunction, Request, Response } from "express";

const generateProjectAPIKey = () => {
    return `api_${randomUUID()}`
}

const createProject = async (req: Request, res: Response, _next: NextFunction) => {
    try {
        const data = req.body;
        const query = `
            INSERT INTO analytics.project(name, apikey, userid) VALUES('${data.name}', '${generateProjectAPIKey()}', 1);
            `;
            console.log(query);
            
        const queryResponse = await db.query(query);
        // logger.info("queryResponse" + queryResponse);
        res.success(HTTP_CODE.CREATED, "got data for users")
    } catch (error) {
        console.log(error);
        res.error(HTTP_CODE.BAD_REQUEST, "error occured while creating project");
    }
}

export {createProject};