import db from "./lib/config/db";
import { EventRequestData } from "./types/api";
import logger from "./utils/logger";

export const asyncQueue = new Array<EventRequestData & {apiKey: string}>();

;(async function () {
    while (true){
        if (asyncQueue.length === 0) {
            await new Promise((r)=> setTimeout(r, 50));
            continue;
        }
        const item = asyncQueue.shift();
        try {
            logger.info("processing item: " + JSON.stringify(item));
            if (item?.apiKey) {
                const query = `
                    SELECT id FROM analytics.project
                    WHERE api_key = $1
                `;
                    
                const q = await db.query(query, [item.apiKey]);
                
                if (q.rowCount === 1) {
                    const projectId = q.rows[0].id;
                    const insertEventQuery = `
                        INSERT INTO analytics.raw_event (event_name, metadata, project_id, user_id, timestamp)
                        VALUES ($1, $2, $3, $4, $5)
                    `;
                    await db.query(insertEventQuery, [item?.event_name, item?.metadata, projectId, item?.user_id, item?.timestamp]);
                    logger.info("processing completed for item: " + JSON.stringify(item));
                    continue;
                }
            }
            logger.error("processing failed due to invalid apikey for item: " + JSON.stringify(item));
        } catch (error) {
            logger.error("failed to process queue item: " + JSON.stringify(item));
        }
    }
})();
