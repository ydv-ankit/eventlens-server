import db from "@/lib/config/db";
import { HTTP_CODE } from "@/utils/constants";
import { randomUUID } from "crypto";
import { NextFunction, Request, Response } from "express";

const generateProjectAPIKey = () => {
    return `api_${randomUUID()}`
}

const createProject = async (req: Request, res: Response, _next: NextFunction) => {
    try {
        const data = req.body;
        if (!data.name || data.name === ""){
            throw Error("project name is required");
        }
        const query = `
            INSERT INTO analytics.project(name, api_key, user_id) 
            VALUES('${data.name}', '${generateProjectAPIKey()}', 1);
        `;

        await db.query(query);
        res.success(HTTP_CODE.CREATED, "project created")
    } catch (error) {
        res.error(HTTP_CODE.BAD_REQUEST, "error occured while creating project");
    }
}

export {createProject};