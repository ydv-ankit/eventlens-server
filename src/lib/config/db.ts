import logger from "@/utils/logger";
import { Pool } from "pg";

const db = new Pool({
    host: "localhost",
    port: 5432,
    user: "postgres",
    password: "postgres",
    database: "eventlens",
    max: 10,
    keepAlive: true
});

db.on("connect", () => logger.info("Database connection success"));
db.on("error", (err) => logger.error("Database encountered an error: " + err.message))

export default db;
