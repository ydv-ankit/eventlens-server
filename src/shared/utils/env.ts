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
  OTLP_TRACE_EXPORTER: process.env.OTLP_TRACE_EXPORTER,
};
