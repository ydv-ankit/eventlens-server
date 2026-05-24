import { app } from "@/app";
import logger from "@/utils/logger";
import { seedDatabase } from "@/utils/db/seed-db";
import { connectRedis } from "./lib/config/redis";
import { startWorkers } from "./worker";
import { ENV } from "./utils/env";

const PORT = Number(ENV.APP_PORT || 8080);
const HOST = "0.0.0.0"

seedDatabase()
    .then(() => app.listen(PORT, HOST, 4096, () => {
        logger.info(`⚙️ Server listening on port: ${PORT}`);
    }))
    .then(() => connectRedis())
    .then(()=> startWorkers())
    .catch((error) => {
        const message = error instanceof Error ? error.message : String(error);
        logger.error("Server bootstrap failed: " + message);
        process.exit(1);
    })

process.on("unhandledRejection", (reason) => {
    logger.error("Unhandled rejection: " + String(reason));
});
