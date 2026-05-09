import db from "./lib/config/db";
import { redisClient, workerRedisClient } from "./lib/config/redis";
import { EventRequestData } from "./types/api";
import { RetryQueue } from "./types/types";
import { BATCH_INSERTION_LIMIT, EVENT_QUEUE_KEY, RETRY_COUNT_LIMIT, RETRY_EVENT_QUEUE_LIMIT } from "./utils/constants";
import { SQL_QUERIES } from "./utils/db/queries";
import logger from "./utils/logger";

const retryQueue = new Array<RetryQueue>();

const getProjectIdByApiKey = async (apiKey: string) => {
    const projectId = await redisClient.get(apiKey);
    if (projectId){
        return projectId;
    }
    
    const q = await db.query(SQL_QUERIES.GET_PROJECT_ID_BY_API_KEY, [apiKey]);
    if (q.rowCount === 1) {
        const projectId = q.rows[0].id;
        await redisClient.set(apiKey, projectId, {
            expiration: {
                type: "EX",
                value: 5 * 60
            }
        });
        return projectId;
    }
    return null;
}

// main queue worker
async function mainQueueWorker () {
    logger.info("Starting main worker")
    while (true){
        const batchInsertionValues = new Array();
        const result = (await workerRedisClient.BLPOP(EVENT_QUEUE_KEY, 0));
        if (!result) break;
        const parsedResult = JSON.parse(result.element) as EventRequestData & {apiKey: string};
        const projectId = await getProjectIdByApiKey(parsedResult.apiKey);
        const values = [parsedResult.event_name, parsedResult.metadata, projectId, parsedResult.user_id, parsedResult.timestamp];
        batchInsertionValues.push(...values);
        try {
            for (let i = 0; i < BATCH_INSERTION_LIMIT - 1; i++) {
                const result = await workerRedisClient.LPOP(EVENT_QUEUE_KEY);
                if (!result) break;
                const parsedResult = JSON.parse(result) as EventRequestData & {apiKey: string};
                // using cache to avoid db lookup
                const projectId = await getProjectIdByApiKey(parsedResult.apiKey);
                if (!projectId) continue;
                const values = [parsedResult.event_name, parsedResult.metadata, projectId, parsedResult.user_id, parsedResult.timestamp];
                batchInsertionValues.push(...values);
            }

            if (batchInsertionValues.length === 0) {
                logger.info("no data to insert");
                continue;
            }
            

            const valuesLimit = Math.floor(batchInsertionValues.length / 5);
            for (let i = 0; i < valuesLimit; i++){
                SQL_QUERIES.BATCH_EVENT_INSERT += `($${5 * i + 1}, $${5 * i + 2}, $${5 * i + 3}, $${5 * i + 4}, $${5 * i + 5})`;
                if (i === valuesLimit - 1) {
                    SQL_QUERIES.BATCH_EVENT_INSERT += `;`;
                }else {
                    SQL_QUERIES.BATCH_EVENT_INSERT += `,`;
                }
            }

            await db.query(SQL_QUERIES.BATCH_EVENT_INSERT, batchInsertionValues);
        } catch (error) {
            logger.error("failed to process queue items");
            // push this data into retry queue for processing again
            const valuesLimit = Math.floor(batchInsertionValues.length / 5);
            for (let idx = 0; idx < valuesLimit; idx++){
                setTimeout(() => {
                    if(retryQueue.length < RETRY_EVENT_QUEUE_LIMIT) {
                        retryQueue.push({
                            values: batchInsertionValues.slice(idx * 5, idx * 5 + 5),
                            retry_count: RETRY_COUNT_LIMIT
                        } as RetryQueue);
                    } else {
                        logger.error("Retry queue overflow, dropping events");
                    }
                }, 100);
            }
        }
    }
}

// retry queue worker
async function retryQueueWorker() {
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
            const valuesLimit = Math.floor(Number(retryItem?.values?.length) / 5);
            for (let i = 0; i < valuesLimit; i++){
                SQL_QUERIES.BATCH_EVENT_INSERT += `($${5 * i + 1}, $${5 * i + 2}, $${5 * i + 3}, $${5 * i + 4}, $${5 * i + 5})`;
                if (i === valuesLimit - 1) {
                    SQL_QUERIES.BATCH_EVENT_INSERT += `;`;
                }else {
                    SQL_QUERIES.BATCH_EVENT_INSERT += `,`;
                }
            }

            await db.query(SQL_QUERIES.BATCH_EVENT_INSERT, retryItem?.values);
        } catch (error) {
            retryQueue.push({
                values: retryItem?.values,
                retry_count: retryItem!.retry_count - 1
            } as RetryQueue);
        }
    }
}

export const startWorkers = () => {
    // initialize workers
    mainQueueWorker();
    retryQueueWorker();
}