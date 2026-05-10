import { ENV } from "@/utils/env";
import logger from "@/utils/logger";
import { Pool } from "pg";

const db = new Pool({
    host: ENV.DB_HOST,
    port: ENV.DB_PORT,
    user: ENV.DB_USER,
    password: ENV.DB_PASSWORD,
    database: ENV.DB_NAME,
    max: ENV.DB_MAX_CONNECTIONS,
    keepAlive: true
});

db.on("connect", () => logger.info("Database connection success"));
db.on("error", (err) => logger.error("Database encountered an error: " + err.message))

export default db;
