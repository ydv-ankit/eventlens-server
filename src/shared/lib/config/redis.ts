import { ENV } from "@/shared/utils/env";
import logger from "@/shared/utils/logger";
import { createClient } from "redis";

const redisUrl = `redis://${ENV.REDIS_HOST}:${ENV.REDIS_PORT}`;

const createRedisClient = () =>
  createClient({
    url: redisUrl,
    socket: {
      reconnectStrategy: (retries) => Math.min(retries * 250, 5_000),
    },
  });

export type AppRedisClient = ReturnType<typeof createRedisClient>;

const attachRedisLogging = (client: AppRedisClient, name: string) => {
  client.on("error", (err) =>
    logger.error(`${name} Redis error: ${err.message}`),
  );
  client.on("connect", () => logger.debug(`${name} Redis socket connected`));
  client.on("ready", () => logger.debug(`${name} Redis client ready`));
  client.on("reconnecting", () => logger.debug(`${name} Redis reconnecting`));
  client.on("end", () => logger.debug(`${name} Redis connection closed`));
};

const getRedisClient = (name = "Worker") => {
  const client = createRedisClient();
  attachRedisLogging(client, name);
  return client;
};

const redisClient = getRedisClient("Main");

const ensureRedisConnection = async (client: AppRedisClient, name: string) => {
  if (client.isReady || client.isOpen) {
    return;
  }

  try {
    await client.connect();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logger.error(`${name} Redis connect failed: ${message}`);
  }
};

export const connectRedis = async () => {
  await ensureRedisConnection(redisClient, "Main");
};

export { ensureRedisConnection, getRedisClient, redisClient };
