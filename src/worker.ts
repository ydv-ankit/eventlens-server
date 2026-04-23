import db from "./lib/config/db";
import { EventRequestData } from "./types/api";
import { RetryQueue } from "./types/types";
import { BATCH_INSERTION_LIMIT, RETRY_COUNT_LIMIT, RETRY_QUEUE_LIMIT } from "./utils/constants";
import logger from "./utils/logger";

export const asyncQueue = new Array<EventRequestData & {apiKey: string}>();
const retryQueue = new Array<RetryQueue>();

// main queue worker
;(async function () {
    while (true){
        const batchInsertionValues = new Array();
        if (asyncQueue.length === 0) {
            await new Promise((r)=> setTimeout(r, 50));
            continue;
        }
        try {
            for (let i = 0; i < BATCH_INSERTION_LIMIT; i++) {
                const item = asyncQueue.shift();
                if (!item) break;   // no items in queue
                if (item?.apiKey) {
                    const query = `
                        SELECT id FROM analytics.project
                        WHERE api_key = $1
                    `;
                    
                    const q = await db.query(query, [item.apiKey]);
                    
                    if (q.rowCount === 1) {
                        const projectId = q.rows[0].id;
                        const values = [item?.event_name, item?.metadata, projectId, item?.user_id, item?.timestamp];
                        batchInsertionValues.push(...values);
                    }
                } else {
                    logger.error("processing failed due to invalid apikey for item: " + JSON.stringify(item));
                }
            }

            if (batchInsertionValues.length === 0) {
                logger.info("no data to insert");
                continue;
            }
            
            let batchEventInsertQuery = `
                INSERT INTO analytics.raw_event (event_name, metadata, project_id, user_id, timestamp)
                VALUES 
            `;

            const valuesLimit = Math.floor(batchInsertionValues.length / 5);
            for (let i = 0; i < valuesLimit; i++){
                batchEventInsertQuery += `($${5 * i + 1}, $${5 * i + 2}, $${5 * i + 3}, $${5 * i + 4}, $${5 * i + 5})`;
                if (i === valuesLimit - 1) {
                    batchEventInsertQuery += `;`;
                }else {
                    batchEventInsertQuery += `,`;
                }
            }

            await db.query(batchEventInsertQuery, batchInsertionValues);
        } catch (error) {
            console.log(error);
            logger.error("failed to process queue items");
            // push this data into retry queue for processing again
            batchInsertionValues.forEach((_, idx)=>{
                setTimeout(() => {
                    if(retryQueue.length < RETRY_QUEUE_LIMIT) {
                        retryQueue.push({
                            values: batchInsertionValues.slice(idx * 5, idx * 5 + 5),
                            retry_count: RETRY_COUNT_LIMIT
                        } as RetryQueue);
                    } else {
                        logger.error("Retry queue overflow, dropping events");
                    }
                }, 100);
            });
        }
    }
})();


// retry queue worker
;(async function() {
    while(true) {
        if (retryQueue.length === 0) {
            await new Promise((r) => setTimeout(r, 100));
            continue;
        }
        const retryItem = retryQueue.shift();
        if (retryItem?.retry_count === 0) {
            logger.info("retry count limit reached, skipping this event: " + JSON.stringify(retryItem.values));
            continue;
        }
        try {
            let batchEventInsertQuery = `
                INSERT INTO analytics.raw_event (event_name, metadata, project_id, user_id, timestamp)
                VALUES 
            `;

            const valuesLimit = Math.floor(Number(retryItem?.values?.length) / 5);
            for (let i = 0; i < valuesLimit; i++){
                batchEventInsertQuery += `($${5 * i + 1}, $${5 * i + 2}, $${5 * i + 3}, $${5 * i + 4}, $${5 * i + 5})`;
                if (i === valuesLimit - 1) {
                    batchEventInsertQuery += `;`;
                }else {
                    batchEventInsertQuery += `,`;
                }
            }

            await db.query(batchEventInsertQuery, retryItem?.values);
        } catch (error) {
            retryQueue.push({
                values: retryItem?.values,
                retry_count: retryItem!.retry_count - 1
            } as RetryQueue);
        }
    }
})();