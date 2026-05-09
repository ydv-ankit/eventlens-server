import logger from '@/utils/logger';
import { createClient } from 'redis';

const redisClient = createClient();
const workerRedisClient = createClient();
redisClient.on('error', (err) => logger.error('Redis Client Error: ' + err.message));
redisClient.on('connect', () => logger.info('Main Redis Client Connected'));
workerRedisClient.on('error', (err) => logger.error('Worker Redis Client Error: ' + err.message));
workerRedisClient.on('connect', () => logger.info('Worker Redis Client Connected'));

export const connectRedis = async () => {
    await redisClient.connect();
    await workerRedisClient.connect();
}

export {redisClient, workerRedisClient};