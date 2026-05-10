import { createClient } from "redis";
import db from "./lib/config/db";
import { getRedisClient, redisClient } from "./lib/config/redis";
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
async function mainQueueWorker (redisClient: any) {
    logger.info("Starting main worker")
    while (true){
        const batchInsertionValues = new Array();
        try {
            const result = (await redisClient.BLPOP(EVENT_QUEUE_KEY, 0));
            if (!result) break;
            const parsedResult = JSON.parse(result.element) as EventRequestData & {apiKey: string};
            const projectId = await getProjectIdByApiKey(parsedResult.apiKey);
            const values = [parsedResult.event_name, parsedResult.metadata, projectId, parsedResult.user_id, parsedResult.timestamp];
            batchInsertionValues.push(...values);
            for (let i = 0; i < BATCH_INSERTION_LIMIT - 1; i++) {
                const result = await redisClient.LPOP(EVENT_QUEUE_KEY);
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
            
            let query = SQL_QUERIES.BATCH_EVENT_INSERT;
            const valuesLimit = Math.floor(batchInsertionValues.length / 5);
            for (let i = 0; i < valuesLimit; i++){
                query += `($${5 * i + 1}, $${5 * i + 2}, $${5 * i + 3}, $${5 * i + 4}, $${5 * i + 5})`;
                if (i === valuesLimit - 1) {
                    query += `;`;
                }else {
                    query += `,`;
                }
            }
            
            await db.query(query, batchInsertionValues);
        } catch (error) {
            console.log(error);
            logger.error("failed to process queue items" + error);
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
            let query = SQL_QUERIES.BATCH_EVENT_INSERT;
            const valuesLimit = Math.floor(Number(retryItem?.values?.length) / 5);
            for (let i = 0; i < valuesLimit; i++){
                query += `($${5 * i + 1}, $${5 * i + 2}, $${5 * i + 3}, $${5 * i + 4}, $${5 * i + 5})`;
                if (i === valuesLimit - 1) {
                    query += `;`;
                }else {
                    query += `,`;
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

export const startWorkers = async () => {
    // initialize workers
    const client1 = getRedisClient();
    const client2 = getRedisClient();
    const client3 = getRedisClient();
    await client1.connect();
    await client2.connect();
    await client3.connect();
    mainQueueWorker(client1);
    mainQueueWorker(client2);
    mainQueueWorker(client3);
    retryQueueWorker();
}