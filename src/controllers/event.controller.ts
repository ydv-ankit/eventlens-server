import db from "@/lib/config/db";
import { EventRequestData } from "@/types/api";
import { HTTP_CODE } from "@/utils/constants";
import { NextFunction, Request, Response } from "express";

const newEvent = async (req: Request, res: Response, _next: NextFunction) => {
    try {
        const data = req.body as EventRequestData;
        const apiKey = req.headers.authorization?.split(" ")[1];
        const query = `
            SELECT id FROM analytics.project
            WHERE api_key = '${apiKey}'
        `;
            
        const q = await db.query(query);
        console.log(q.rowCount);
        console.log(data.metadata);
        
        if (q.rowCount === 1) {
            const projectId = q.rows[0].id;

            const insertEventQuery = `
                INSERT INTO analytics.raw_event (event_name, metadata, project_id, user_id, timestamp)
                VALUES ($1, $2, $3, $4, $5)
            `;
            await db.query(insertEventQuery, [data.event_name, data.metadata, projectId, data.user_id, data.timestamp]);
        }
        res.success(HTTP_CODE.CREATED, "event stored")
    } catch (error) {
        console.log(error);
        res.error(HTTP_CODE.BAD_REQUEST, "error occured while creating project");
    }
}

export {newEvent};