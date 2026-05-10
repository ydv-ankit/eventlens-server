import { ENV } from '@/utils/env';
import logger from '@/utils/logger';
import { createClient } from 'redis';

const redisUrl = `redis://${ENV.REDIS_HOST}:${ENV.REDIS_PORT}`

const getRedisClient =  () => createClient({
    url: redisUrl
});

const redisClient = getRedisClient();

redisClient.on('error', (err) => console.log(err));
redisClient.on('connect', () => logger.info('Main Redis Client Connected'));

export const connectRedis = async () => {
    await redisClient.connect();
}

export {getRedisClient, redisClient};