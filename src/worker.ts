import db from "./lib/config/db";
import { EventRequestData } from "./types/api";
import logger from "./utils/logger";

export const asyncQueue = new Array<EventRequestData & {apiKey: string}>();
const batchInsertionLimit = 100;

;(async function () {
    while (true){
        if (asyncQueue.length === 0) {
            await new Promise((r)=> setTimeout(r, 50));
            continue;
        }
        try {
            const insertionBatchValues = new Array();
            for (let i = 0; i < batchInsertionLimit; i++) {
                const item = asyncQueue.shift();
                if (!item) break;   // no items in queue
                logger.info("processing item: " + JSON.stringify(item));

                if (item?.apiKey) {
                    const query = `
                        SELECT id FROM analytics.project
                        WHERE api_key = $1
                    `;
                    
                    const q = await db.query(query, [item.apiKey]);
                    
                    if (q.rowCount === 1) {
                        const projectId = q.rows[0].id;
                        const values = [item?.event_name, item?.metadata, projectId, item?.user_id, item?.timestamp];
                        insertionBatchValues.push(...values);
                    }
                } else {
                    logger.error("processing failed due to invalid apikey for item: " + JSON.stringify(item));
                }
            }
            
            let insertEventQuery = `
                INSERT INTO analytics.raw_event (event_name, metadata, project_id, user_id, timestamp)
                VALUES 
            `;
            const valuesLimit = Math.floor(insertionBatchValues.length / 5);
            for (let i = 0; i < valuesLimit; i++){
                insertEventQuery += `($${5 * i + 1}, $${5 * i + 2}, $${5 * i + 3}, $${5 * i + 4}, $${5 * i + 5})`;
                if (i === valuesLimit - 1) {
                    insertEventQuery += `;`;
                }else {
                    insertEventQuery += `,`;
                }
            }
            logger.info("query: " + insertEventQuery);
            logger.info("values: " + insertionBatchValues);

            await db.query(insertEventQuery, insertionBatchValues);
        } catch (error) {
            console.log(error);
            
            logger.error("failed to process queue items");
        }
    }
})();
