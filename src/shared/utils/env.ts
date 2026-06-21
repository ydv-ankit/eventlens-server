import dotenv from "dotenv";

dotenv.config();

export const ENV = {
  APP_PORT: process.env.APP_PORT,
  DB_HOST: process.env.DB_HOST,
  DB_PORT: Number(process.env.DB_PORT || 5432),
  DB_USER: process.env.DB_USER,
  DB_PASSWORD: process.env.DB_PASSWORD,
  DB_NAME: process.env.DB_NAME,
  DB_MAX_CONNECTIONS: Number(process.env.DB_MAX_CONNECTIONS || 10),
  REDIS_HOST: process.env.REDIS_HOST,
  REDIS_PORT: process.env.REDIS_PORT,
  KAFKA_BROKERS: process.env.KAFKA_BROKERS,
  KAFKA_CLIENT_ID: process.env.KAFKA_CLIENT_ID,
  KAFKA_TOPIC: process.env.KAFKA_TOPIC,
  KAFKA_CONSUMER_GROUP: process.env.KAFKA_CONSUMER_GROUP,
  KAFKA_RETRY_TOPIC: process.env.KAFKA_RETRY_TOPIC,
  KAFKA_RETRY_CONSUMER_GROUP: process.env.KAFKA_RETRY_CONSUMER_GROUP,
  KAFKA_TOPIC_PARTITIONS: Number(process.env.KAFKA_TOPIC_PARTITIONS || 4),
  OTLP_TRACE_EXPORTER: process.env.OTLP_TRACE_EXPORTER,
  CLERK_SECRET_KEY: process.env.CLERK_SECRET_KEY,
  CORS_ORIGINS: (process.env.CORS_ORIGINS ?? "http://localhost:5173")
    .split(",")
    .map((o) => o.trim())
    .filter(Boolean),
};
