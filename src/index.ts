import { app } from "@/app";
import logger from "@/utils/logger";
import { seedDatabase } from "@/utils/db/seed-db";
import { connectRedis } from "./lib/config/redis";
import { startWorkers } from "./worker";

const PORT = Number(process.env.PORT || 8080);

seedDatabase()
    .then(() => connectRedis())
    .then(()=> startWorkers())
    .then(() => app.listen(PORT, () => {
        logger.info(`⚙️ Server listening on port: ${PORT}`);
    }))

process.on("unhandledRejection", (reason) => {
    logger.error("Unhandled rejection: " + reason);
});